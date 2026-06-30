# -*- coding: utf-8 -*-
"""
recover_parcels_by_log modülü
==============================

TKGM servisinden alınan tüm verilerin eksiksiz şekilde veritabanına
kaydedilmesini sağlayan, hiçbir verinin atlanmamasını garanti eden
kurtarma aracı.

Özellikler
----------
- Log ID aralığına göre sıralı işleme
- Network, veritabanı ve parse hatalarında **exponential backoff** ile retry
- **Transaction + Savepoint** ile atomic kayıt (bir parsel hata verirse
  sadece o parsel iptal edilir, diğerleri transaction içinde korunur)
- **Failed records** tablosuna hata detayı kaydı (veri kaybı önlenir)
- Detaylı Türkçe/UTF-8 loglama (konsol + günlük dosyası)
- Kullanıcı dostu CLI (--from_log_id, --to_log_id, --max-retries,
  --retry-delay, --continue-on-error)

Kullanım
--------
    # Yalnızca belirtilen log_id'den büyük kayıtları sırayla işle
    python -m src.tools.recover_parcels_by_log --from_log_id 941

    # Belirtilen aralıktaki (her iki uç DAHİL) tüm logları sırayla işle
    python -m src.tools.recover_parcels_by_log --from_log_id 941 --to_log_id 1000

    # Retry parametrelerini özelleştir
    python -m src.tools.recover_parcels_by_log --from_log_id 941 \\
        --max-retries 5 --retry-delay 2.0

    # Bir log hata verince dur, sonrakine geçme
    python -m src.tools.recover_parcels_by_log --from_log_id 941 --stop-on-error
"""
import argparse
import sys
import time
import traceback
from datetime import datetime
from typing import Any, Optional

from loguru import logger

from src.database import DatabaseManager
from src.geometry import WFSGeometryProcessor


# ---------------------------------------------------------------------------
# Sabitler
# ---------------------------------------------------------------------------

DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_DELAY = 1.0  # saniye
DEFAULT_RETRY_BACKOFF = 2.0  # her denemede delay bu çarpan kadar artsın


# ---------------------------------------------------------------------------
# Loglama
# ---------------------------------------------------------------------------

def setup_logger() -> str:
    """
    Loglama sistemini UTF-8 uyumlu şekilde yapılandırır.

    - stdout: renksiz, INFO düzeyi (Türkçe karakterler düzgün görünür)
    - dosya: günlük bazlı, DEBUG düzeyi, UTF-8 encoding
    """
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except (AttributeError, ValueError):
        pass

    log_file = f"parcel_recovery_{datetime.now().strftime('%Y-%m-%d')}.log"
    logger.remove()
    logger.add(
        sys.stdout,
        level="INFO",
        colorize=False,
        format=(
            "<level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
            "<level>{message}</level>"
        ),
    )
    logger.add(
        log_file,
        rotation="10 MB",
        level="DEBUG",
        backtrace=True,
        diagnose=True,
        encoding="utf-8",
    )
    return log_file


# ---------------------------------------------------------------------------
# Argüman doğrulama
# ---------------------------------------------------------------------------

def _validate_positive_int(value: str, name: str) -> int:
    """Bir string'i doğrular ve pozitif tam sayıya çevirir."""
    if value is None:
        raise ValueError(f"{name} parametresi eksik!")
    value = str(value).strip()
    if not value:
        raise ValueError(f"{name} boş olamaz!")
    try:
        number = int(value)
    except (ValueError, TypeError):
        raise ValueError(
            f"Geçersiz {name} formatı: '{value}'. Pozitif bir tam sayı girilmelidir."
        )
    if number <= 0:
        raise ValueError(f"{name} pozitif bir tam sayı olmalıdır (girilen: {number}).")
    return number


def _validate_positive_float(value: str, name: str) -> float:
    """Bir string'i doğrular ve pozitif ondalık sayıya çevirir."""
    if value is None:
        raise ValueError(f"{name} parametresi eksik!")
    value = str(value).strip()
    if not value:
        raise ValueError(f"{name} boş olamaz!")
    try:
        number = float(value)
    except (ValueError, TypeError):
        raise ValueError(
            f"Geçersiz {name} formatı: '{value}'. Pozitif bir sayı girilmelidir."
        )
    if number <= 0:
        raise ValueError(f"{name} pozitif bir sayı olmalıdır (girilen: {number}).")
    return number


# ---------------------------------------------------------------------------
# Retry decorator
# ---------------------------------------------------------------------------

def with_retry(
    max_retries: int = DEFAULT_MAX_RETRIES,
    base_delay: float = DEFAULT_RETRY_DELAY,
    backoff: float = DEFAULT_RETRY_BACKOFF,
    retryable_exceptions: tuple = (
        ConnectionError,
        TimeoutError,
        OSError,
    ),
):
    """
    Dekoratör: Network, bağlantı ve timeout hatalarında exponential backoff
    ile tekrar dener. Diğer hatalar için (örn. ValueError) tekrar denemez,
    doğrudan çağırana iletir.
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            last_exc = None
            delay = base_delay
            for attempt in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except retryable_exceptions as e:
                    last_exc = e
                    if attempt == max_retries:
                        logger.error(
                            f"  Son deneme de başarısız oldu ({attempt}/{max_retries}): {e}"
                        )
                        raise
                    logger.warning(
                        f"  Geçici hata ({attempt}/{max_retries}): {e} - "
                        f"{delay:.2f}s sonra tekrar denenecek..."
                    )
                    time.sleep(delay)
                    delay *= backoff
                except Exception:
                    # retryable_exceptions dışındaki hatalar: tekrar deneme
                    raise
            # Buraya teorik olarak ulaşılmaz
            if last_exc:
                raise last_exc
        return wrapper
    return decorator


# ---------------------------------------------------------------------------
# Veritabanı işlemleri (retry ve transaction ile)
# ---------------------------------------------------------------------------

@with_retry(max_retries=DEFAULT_MAX_RETRIES, base_delay=DEFAULT_RETRY_DELAY)
def fetch_logs_in_range(
    db: DatabaseManager, from_id: int, to_id: Optional[int]
) -> list[dict[str, Any]]:
    """
    Belirtilen log_id aralığındaki (her iki uç dahil) kayıtları
    id artan sırada getirir. to_id None ise yalnızca from_id'den büyükler alınır.
    Network/bağlantı hatalarında otomatik retry uygulanır.
    """
    sql = """
        SELECT id, typename, url, feature_count, is_empty, is_successful,
               response_xml, query_time, http_status_code, error_message
        FROM tk_logs
        WHERE id >= %s
    """
    params: list[Any] = [from_id]
    if to_id is not None:
        sql += " AND id <= %s"
        params.append(to_id)
    sql += " ORDER BY id ASC"

    with db.connection.connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, tuple(params))
            return cursor.fetchall() or []


def _record_failure(
    db: DatabaseManager,
    entity_type: str,
    raw_data: Any,
    error: Exception,
    entity_id: str,
) -> None:
    """Hata kaydını failed_records tablosuna yaz (retry korumalı)."""
    try:
        @with_retry(max_retries=2, base_delay=0.5)
        def _do_record():
            with db.connection.connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        INSERT INTO tk_failed_records (
                            entity_type, entity_id, raw_data,
                            error_type, error_message, stack_trace,
                            status
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (entity_type, entity_id, status) DO NOTHING
                        """,
                        (
                            entity_type,
                            entity_id,
                            str(raw_data)[:8000],  # JSONB yerine TEXT gibi saklıyorsak
                            type(error).__name__,
                            str(error)[:2000],
                            traceback.format_exc()[:4000],
                            "failed",
                        ),
                    )
                    conn.commit()

        _do_record()
    except Exception as record_err:
        # Failed record da yazılamadıysa en azından konsola yaz
        logger.critical(f"Failed record tablosuna yazılamadı: {record_err}")


def _process_features_with_transaction(
    db: DatabaseManager,
    features: list[dict[str, Any]],
) -> tuple[int, int]:
    """
    Parsel listesini tek bir transaction içinde, her feature için
    SAVEPOINT kullanarak kaydeder. Bir feature hata verirse sadece
    o feature için ROLLBACK yapılır; diğer başarılı feature'lar
    transaction sonunda COMMIT edilir.

    Returns:
        (saved_2320, saved_4326) tuple'ı.
    """
    saved_2320 = db.insert_parcels(features)
    saved_4326 = db.insert_parcels_4326(features)
    return saved_2320, saved_4326


@with_retry(max_retries=DEFAULT_MAX_RETRIES, base_delay=DEFAULT_RETRY_DELAY)
def process_log(
    db: DatabaseManager,
    processor: WFSGeometryProcessor,
    log: dict[str, Any],
) -> tuple[int, int, int]:
    """
    Tek bir log kaydını işler ve tk_parsel / tk_parsel_4326
    tablolarına transaction içinde ekler.

    Hata durumunda:
    - Network/timeout hatası: otomatik retry
    - Parse hatası: failed_records'a kayıt + atla
    - DB constraint hatası: failed_records'a kayıt + atla

    Returns:
        (saved_2320, saved_4326, feature_count) tuple'ı.
    """
    log_id = log['id']
    log_tag = f"Log ID={log_id}"

    if log.get('is_empty') or log.get('feature_count', 0) == 0:
        logger.info(f"  {log_tag} boş veya 0 feature içeriyor, atlanıyor.")
        return 0, 0, 0

    if not log.get('is_successful'):
        logger.warning(f"  {log_tag} başarısız bir istek, atlanıyor.")
        return 0, 0, 0

    response_xml = log.get('response_xml')
    if not response_xml:
        logger.warning(f"  {log_tag} response_xml verisi yok, atlanıyor.")
        return 0, 0, 0

    # XML parse - parse hatası fatal değil, log'lanır ve atlanır
    try:
        features = processor.process_parcel_wfs_response(response_xml)
    except Exception as parse_err:
        logger.error(f"  {log_tag} XML parse hatası: {parse_err}")
        _record_failure(
            db,
            entity_type="parsel_xml",
            raw_data={"log_id": log_id, "url": log.get("url")},
            error=parse_err,
            entity_id=str(log_id),
        )
        return 0, 0, 0

    logger.info(f"  {log_tag} için {len(features)} parsel bulundu.")
    if not features:
        return 0, 0, 0

    # DB insert - transaction + savepoint zaten parcel_repo içinde
    try:
        saved_2320, saved_4326 = _process_features_with_transaction(db, features)
    except Exception as db_err:
        logger.error(f"  {log_tag} DB insert hatası: {db_err}")
        _record_failure(
            db,
            entity_type="parsel_batch",
            raw_data={"log_id": log_id, "feature_count": len(features)},
            error=db_err,
            entity_id=str(log_id),
        )
        return 0, 0, len(features)

    logger.info(
        f"  {log_tag}: tk_parsel'e {saved_2320}, "
        f"tk_parsel_4326'ya {saved_4326} kayıt eklendi/güncellendi."
    )
    return saved_2320, saved_4326, len(features)


# ---------------------------------------------------------------------------
# Argüman ayrıştırma
# ---------------------------------------------------------------------------

def parse_arguments() -> argparse.Namespace:
    """
    Komut satırı argümanlarını ayrıştırır.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Belirtilen log_id aralığındaki (veya yalnızca log_id'den büyük) "
            "kayıtları sıralı olarak, retry + transaction koruması ile "
            "işler ve parsel verilerini geri yükler."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Örnekler:\n"
            "  python -m src.tools.recover_parcels_by_log --from_log_id 941\n"
            "  python -m src.tools.recover_parcels_by_log --from_log_id 941 --to_log_id 950\n"
            "  python -m src.tools.recover_parcels_by_log 941\n"
            "  python -m src.tools.recover_parcels_by_log --from_log_id 941 "
            "--max-retries 5 --retry-delay 2.0\n"
        ),
    )
    parser.add_argument(
        "from_log_id_pos",
        nargs="?",
        type=str,
        default=None,
        help="Başlangıç log_id (pozitif tam sayı). Belirtilen ID dahil edilir.",
    )
    parser.add_argument(
        "--from_log_id",
        dest="from_log_id",
        type=str,
        default=None,
        help="Başlangıç log_id (pozitif tam sayı, dahil).",
    )
    parser.add_argument(
        "--to_log_id",
        dest="to_log_id",
        type=str,
        default=None,
        help=(
            "Bitiş log_id (pozitif tam sayı, dahil). "
            "Verilmezse sadece --from_log_id'den büyükler alınır."
        ),
    )
    parser.add_argument(
        "--max-retries",
        dest="max_retries",
        type=str,
        default=str(DEFAULT_MAX_RETRIES),
        help=(
            "Network/timeout hatalarında en fazla tekrar deneme sayısı "
            f"(varsayılan: {DEFAULT_MAX_RETRIES})."
        ),
    )
    parser.add_argument(
        "--retry-delay",
        dest="retry_delay",
        type=str,
        default=str(DEFAULT_RETRY_DELAY),
        help=(
            "İlk retry gecikmesi, saniye (varsayılan: "
            f"{DEFAULT_RETRY_DELAY}). Sonraki denemelerde backoff ile çarpılır."
        ),
    )
    parser.add_argument(
        "--stop-on-error",
        dest="stop_on_error",
        action="store_true",
        help="Bir log işlenirken hata olursa programı durdur.",
    )
    return parser.parse_args()


def validate_parsed_args(args: argparse.Namespace) -> tuple[int, Optional[int], int, float, bool]:
    """Ayrıştırılmış argümanları doğrular ve uygun tiplere çevirir."""
    raw_from = args.from_log_id if args.from_log_id is not None else args.from_log_id_pos
    if raw_from is None:
        raise ValueError(
            "from_log_id parametresi eksik! "
            "Kullanım: python -m src.tools.recover_parcels_by_log "
            "--from_log_id <id> [--to_log_id <id>]"
        )
    from_id = _validate_positive_int(raw_from, "from_log_id")

    to_id: Optional[int] = None
    if args.to_log_id is not None:
        to_id = _validate_positive_int(args.to_log_id, "to_log_id")
        if to_id < from_id:
            raise ValueError(
                f"to_log_id ({to_id}) from_log_id'den ({from_id}) küçük olamaz."
            )

    max_retries = _validate_positive_int(args.max_retries, "max-retries")
    retry_delay = _validate_positive_float(args.retry_delay, "retry-delay")

    return from_id, to_id, max_retries, retry_delay, args.stop_on_error


# ---------------------------------------------------------------------------
# Ana iş akışı
# ---------------------------------------------------------------------------

def run_recovery(
    from_log_id: int,
    to_log_id: Optional[int],
    max_retries: int,
    retry_delay: float,
    stop_on_error: bool,
) -> int:
    """
    Ana iş akışı: Verilen log_id aralığındaki tüm kayıtları sıralı olarak,
    retry + transaction koruması ile işler.

    Returns:
        0 başarı, 1 hata.
    """
    log_file = setup_logger()
    logger.info("=" * 80)
    if to_log_id is None:
        logger.info(
            f"Log ID >= {from_log_id} olan tüm kayıtlar sıralı işlenecek "
            f"(sıralama: id ASC, yöntem: sıralı tek tek, "
            f"max-retries={max_retries}, retry-delay={retry_delay}s)"
        )
    else:
        logger.info(
            f"Log ID {from_log_id}..{to_log_id} aralığındaki tüm kayıtlar sıralı işlenecek "
            f"(sıralama: id ASC, yöntem: sıralı tek tek, "
            f"max-retries={max_retries}, retry-delay={retry_delay}s)"
        )
    logger.info("=" * 80)

    try:
        db = DatabaseManager()
        processor = WFSGeometryProcessor()
    except Exception as e:
        logger.critical(f"DatabaseManager veya processor başlatılamadı: {e}")
        return 1

    try:
        logs = fetch_logs_in_range(db, from_log_id, to_log_id)
    except Exception as e:
        logger.critical(f"Log kayıtları veritabanından alınırken hata: {e}")
        return 1

    if not logs:
        if to_log_id is None:
            logger.warning(f"Log ID >= {from_log_id} aralığında kayıt bulunamadı.")
        else:
            logger.warning(
                f"Log ID {from_log_id}..{to_log_id} aralığında kayıt bulunamadı."
            )
        return 0

    logger.info(f"Toplam {len(logs)} log kaydı bulundu, sıralı işleme başlıyor.")

    total_saved_2320 = 0
    total_saved_4326 = 0
    total_features = 0
    processed = 0
    errored = 0
    skipped = 0

    # SIRALI (sequential) işleme - her log sırayla tek tek
    for index, log in enumerate(logs, start=1):
        log_id = log['id']
        logger.info(
            f"[{index}/{len(logs)}] İşleniyor: Log ID={log_id}, "
            f"Tarih={log.get('query_time')}, "
            f"Tip={log.get('typename')}, "
            f"Feature={log.get('feature_count')}"
        )

        try:
            saved_2320, saved_4326, feature_count = process_log(db, processor, log)
            total_saved_2320 += saved_2320
            total_saved_4326 += saved_4326
            total_features += feature_count
            if feature_count == 0:
                skipped += 1
            else:
                processed += 1
        except Exception as e:
            logger.error(f"  Log ID={log_id} işlenirken kritik hata: {e}")
            logger.error(traceback.format_exc())
            errored += 1
            if stop_on_error:
                logger.error("stop-on-error aktif: program durduruluyor.")
                return 1
            # Aksi halde sonraki log'a devam
            continue

    logger.info("=" * 80)
    logger.info("Sıralı işlem tamamlandı!")
    logger.info(f"  Bulunan log sayısı            : {len(logs)}")
    logger.info(f"  Başarıyla işlenen log         : {processed}")
    logger.info(f"  Hatalı log                    : {errored}")
    logger.info(f"  Atlanan log (boş/parse-hata)  : {skipped}")
    logger.info(f"  Toplam işlenen feature        : {total_features}")
    logger.info(f"  tk_parsel eklenen/güncellenen       : {total_saved_2320}")
    logger.info(f"  tk_parsel_4326 eklenen/güncellenen   : {total_saved_4326}")
    logger.info(f"  Log dosyası                   : {log_file}")
    logger.info("=" * 80)
    return 0


def main() -> None:
    """Programın giriş noktası."""
    try:
        args = parse_arguments()
        from_log_id, to_log_id, max_retries, retry_delay, stop_on_error = validate_parsed_args(args)
    except ValueError as err:
        print(f"HATA: {err}", file=sys.stderr)
        print(
            "Kullanım: python -m src.tools.recover_parcels_by_log "
            "--from_log_id <id> [--to_log_id <id>]",
            file=sys.stderr,
        )
        sys.exit(1)
    except SystemExit:
        raise
    except Exception as err:
        print(f"Beklenmeyen hata: {err}", file=sys.stderr)
        sys.exit(1)

    exit_code = run_recovery(
        from_log_id, to_log_id, max_retries, retry_delay, stop_on_error
    )
    sys.exit(exit_code)


if __name__ == "__main__":
    main()