"""
TKGM WFS Veri Tarayıcısı - Ana Uygulama
Türkiye Tapu ve Kadastro Genel Müdürlü parsel verilerini otomatik olarak toplar

Production-grade:
- Merkezi exception handling
- Anlamlı exit kodları (cron, supervisor, systemd için)
- Graceful shutdown
"""

import argparse
import sys
import traceback
from datetime import datetime
from loguru import logger

# Modülleri import et
from src.database import DatabaseManager, SettingsRepository
from src.scraper import TKGMScraper


# Uygulama çıkış kodları (production araçları tarafından izlenir)
EXIT_OK = 0
EXIT_INVALID_ARGS = 2
EXIT_RUNTIME_ERROR = 3
EXIT_DB_ERROR = 4
EXIT_INTERRUPTED = 130  # 128 + SIGINT(2)


def _date_type(value: str) -> datetime:
    """argparse tipi: YYYY-MM-DD formatında tarih bekler."""
    try:
        return datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Geçersiz tarih formatı: '{value}'. Beklenen format: YYYY-MM-DD"
        ) from exc


def main() -> int:
    """Ana fonksiyon - her durumda anlamlı exit kodu döndürür."""
    parser = argparse.ArgumentParser(
        description="TKGM WFS Veri Çekme Uygulaması",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # Senkronizasyon komutları
    parser.add_argument('--fully', action='store_true', help='Tüm parsel verilerini senkronize et')
    parser.add_argument('--daily', action='store_true', help='Günlük parsel verilerini senkronize et')
    parser.add_argument('--daily-inactive', action='store_true', help='Günlük pasif parsel verilerini senkronize et')
    parser.add_argument('--neighbourhoods', action='store_true', help='Mahalle verilerini senkronize et')
    parser.add_argument('--districts', action='store_true', help='İlçe verilerini senkronize et')
    parser.add_argument('--stats', action='store_true', help='İstatistik verilerini göster')
    parser.add_argument('--stats-telegram', action='store_true', help="İstatistikleri Telegram'a gönder")

    # Log sorgu komutları
    parser.add_argument('--query-logs', action='store_true', help='Log kayıtlarında parsel verisi ara')
    parser.add_argument('--adano', type=str, help='Ada numarası ile filtrele')
    parser.add_argument('--parselno', type=str, help='Parsel numarası ile filtrele')
    parser.add_argument('--tapukimlikno', type=int, help='Tapu kimlik no ile filtrele')
    parser.add_argument('--durum', type=str, help='Durum değeri ile filtrele')
    parser.add_argument('--date-from', type=_date_type, help='Başlangıç tarihi (YYYY-MM-DD)')
    parser.add_argument('--date-to', type=_date_type, help='Bitiş tarihi (YYYY-MM-DD)')
    parser.add_argument('--log-id', type=int, help='Belirli bir log kaydını ID ile görüntüle')
    parser.add_argument('--limit', type=int, default=10, help='Maksimum sonuç sayısı (varsayılan: 10)')

    try:
        args = parser.parse_args()
    except SystemExit as exc:
        # argparse -h veya hata durumunda kendi koduyla çıkar
        return exc.code if isinstance(exc.code, int) else EXIT_INVALID_ARGS

    try:
        if args.query_logs or args.log_id:
            from src.log_explorer import LogExplorer
            db = DatabaseManager()
            explorer = LogExplorer(db)

            if args.log_id:
                explorer.show_log_detail(args.log_id)
            else:
                if not any([args.adano, args.parselno, args.tapukimlikno, args.durum]):
                    print(
                        "Hata: En az bir arama kriteri belirtmelisiniz "
                        "(--adano, --parselno, --tapukimlikno, --durum)"
                    )
                    parser.print_help()
                    return EXIT_INVALID_ARGS

                # date_from/date_to zaten datetime objesi (argparse tipi sayesinde)
                date_from = args.date_from.strftime("%Y-%m-%d") if args.date_from else None
                date_to = args.date_to.strftime("%Y-%m-%d") if args.date_to else None

                explorer.search_and_display(
                    adano=args.adano,
                    parselno=args.parselno,
                    tapukimlikno=args.tapukimlikno,
                    durum=args.durum,
                    date_from=date_from,
                    date_to=date_to,
                    limit=args.limit
                )
        elif args.daily:
            scraper = TKGMScraper()
            db = DatabaseManager()
            last_setting = db.get_last_setting(SettingsRepository.TYPE_DAILY_SYNC)
            start_index = last_setting.get('start_index', 0)
            start_date = last_setting.get('query_date', datetime.strptime('2025-10-08', '%Y-%m-%d'))
            scraper.sync_daily_parcels(start_date=start_date, start_index=start_index)
        elif args.daily_inactive:
            scraper = TKGMScraper()
            db = DatabaseManager()
            last_setting = db.get_last_setting(SettingsRepository.TYPE_DAILY_INACTIVE_SYNC)
            start_index = last_setting.get('start_index', 0)
            start_date = last_setting.get('query_date', datetime.strptime('2021-01-01', '%Y-%m-%d'))
            scraper.sync_daily_inactive_parcels(start_date=start_date, start_index=start_index)
        elif args.fully:
            scraper = TKGMScraper()
            db = DatabaseManager()
            last_setting = db.get_last_setting(SettingsRepository.TYPE_FULLY_SYNC)
            start_index = last_setting.get('start_index', 0)
            scraper.sync_fully_parcels(start_index=start_index)
        elif args.neighbourhoods:
            scraper = TKGMScraper()
            scraper.sync_neighbourhoods()
        elif args.districts:
            scraper = TKGMScraper()
            scraper.sync_districts()
        elif args.stats:
            scraper = TKGMScraper()
            scraper.show_stats()
        elif args.stats_telegram:
            scraper = TKGMScraper()
            db = DatabaseManager()
            stats = db.get_statistics()
            if not stats:
                logger.error("İstatistik verileri alınamadı; Telegram gönderimi atlandı")
                return EXIT_RUNTIME_ERROR
            sent = scraper.notifier.send_stats(stats)
            if sent:
                logger.info("İstatistikler Telegram'a gönderildi")
            else:
                logger.error("İstatistikler Telegram'a gönderilemedi")
                return EXIT_RUNTIME_ERROR
        else:
            parser.print_help()
            return EXIT_INVALID_ARGS

        return EXIT_OK

    except KeyboardInterrupt:
        logger.info("Uygulama kullanıcı tarafından durduruldu (Ctrl+C)")
        return EXIT_INTERRUPTED
    except ImportError as e:
        logger.critical(f"Eksik modül/bağımlılık: {e}")
        logger.debug(traceback.format_exc())
        return EXIT_RUNTIME_ERROR
    except ConnectionError as e:
        logger.critical(f"Veritabanı bağlantı hatası: {e}")
        logger.debug(traceback.format_exc())
        return EXIT_DB_ERROR
    except Exception as e:
        logger.critical(f"Beklenmeyen ana uygulama hatası: {e}")
        logger.critical(traceback.format_exc())
        return EXIT_RUNTIME_ERROR


if __name__ == "__main__":
    sys.exit(main())
