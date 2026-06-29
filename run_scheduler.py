"""
TKGM Scheduler - Zamanlama ve Görev Yöneticisi

Mimari:
- Python `schedule` kütüphanesi kullanılır (cron servisi başlatılmaz/kullanılmaz).
- Her uzun süreli görev ayrı bir thread üzerinde çalıştırılır; böylece
  scheduler ana döngüsü bloke olmaz.
- Aynı anda birden fazla görevin çalışmasını önlemek için threading.Lock kullanılır.
"""

import schedule
import time
import subprocess
import signal
import sys
import threading
from datetime import datetime, date
from loguru import logger
from src.database import DatabaseManager
from src.database.repositories import SettingsRepository

# ---------------------------------------------------------------------------
# Loglama ayarları
# ---------------------------------------------------------------------------
logger.remove()
logger.add(
    sys.stdout,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan> - <level>{message}</level>",
)

# ---------------------------------------------------------------------------
# Concurrent çalışmayı önleyen kilit
# ---------------------------------------------------------------------------
_job_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Yardımcı fonksiyonlar
# ---------------------------------------------------------------------------

def run_task(command: str, task_name: str) -> None:
    """
    Verilen shell komutunu çalıştırır, çıktısını loglar.

    Notlar:
    - stdout ve stderr birleştirilerek loglanır; böylece script çıktıları
      kaybolmaz.
    - Komutlar Dockerfile build aşamasında zaten CRLF-temizlenip
      chmod yapıldığından, buraya `sed` eklemeye gerek yoktur.
    """
    logger.info(f"Task Started: {task_name}")
    try:
        result = subprocess.run(
            command,
            shell=True,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

        # Script çıktısını satır satır logla
        if result.stdout:
            for line in result.stdout.splitlines():
                if line.strip():
                    logger.info(f"[{task_name}] {line}")

        if result.returncode == 0:
            logger.info(f"Task Completed Successfully: {task_name}")
        else:
            logger.error(
                f"Task Failed: {task_name} (Return Code: {result.returncode})"
            )

    except Exception as e:
        logger.error(f"Task Execution Error ({task_name}): {e}")


def run_task_in_thread(command: str, task_name: str) -> None:
    """
    run_task'ı ayrı bir thread'de çalıştırır.

    Avantajlar:
    - Scheduler ana döngüsü (while True: schedule.run_pending()) bloke olmaz.
    - Uzun süren OGR/script işlemleri diğer zamanlamaları geciktirmez.
    - _job_lock ile aynı anda yalnızca bir iş çalışır.
    """
    def _worker():
        if not _job_lock.acquire(blocking=False):
            logger.warning(
                f"Task Skipped (another job is still running): {task_name}"
            )
            return
        try:
            run_task(command, task_name)
        finally:
            _job_lock.release()

    thread = threading.Thread(target=_worker, name=f"job-{task_name}", daemon=True)
    thread.start()


# ---------------------------------------------------------------------------
# Görev tanımları
# ---------------------------------------------------------------------------

def daily_active_job() -> None:
    run_task_in_thread(
        "/usr/bin/python3 /app/main.py --daily",
        "Daily Active Sync",
    )


def daily_inactive_job() -> None:
    run_task_in_thread(
        "/usr/bin/python3 /app/main.py --daily-inactive",
        "Daily Inactive Sync",
    )


def oracle_sync_job() -> None:
    run_task_in_thread(
        "bash /app/scripts/sync-oracle.sh",
        "Oracle Sync",
    )


def postgres_sync_job() -> None:
    run_task_in_thread(
        "bash /app/scripts/sync-postgresql.sh",
        "PostgreSQL Sync",
    )


def postgres_kadastro_yeni_sync_job() -> None:
    run_task_in_thread(
        "bash /app/scripts/sync-postgresql-kadastro-yeni.sh",
        "PostgreSQL Kadastro Yeni Sync",
    )


def postgres_sync_4326_job() -> None:
    run_task_in_thread(
        "bash /app/scripts/sync-postgresql-4326.sh",
        "PostgreSQL Sync (EPSG:4326)",
    )


def dispatch_sync_job() -> None:
    """
    DB'deki son senkronizasyon durumuna göre aktif veya pasif sync'i tetikler.

    - query_date >= bugün  → Aktif sync tamamlanmış → Pasif sync çalıştır.
    - query_date <  bugün  → Aktif sync devam ediyor → Aktif sync çalıştır.
    """
    try:
        db = DatabaseManager()
        last_setting = db.get_last_setting(SettingsRepository.TYPE_DAILY_SYNC)

        if not last_setting or "query_date" not in last_setting:
            logger.info("Setting not found, defaulting to Active Sync")
            daily_active_job()
            return

        query_date = last_setting["query_date"]
        if isinstance(query_date, datetime):
            query_date = query_date.date()

        today = date.today()

        if query_date >= today:
            logger.info(
                f"Active Sync Finished (Query Date: {query_date} >= Today: {today})."
                " Dispatching Inactive Sync."
            )
            daily_inactive_job()
        else:
            logger.info(
                f"Active Sync Ongoing (Query Date: {query_date} < Today: {today})."
                " Dispatching Active Sync."
            )
            daily_active_job()

    except Exception as e:
        logger.error(f"Error in dispatch_sync_job: {e}")


# ---------------------------------------------------------------------------
# Sinyal işleyici
# ---------------------------------------------------------------------------

def signal_handler(signum, frame) -> None:
    logger.info(f"Signal received ({signum}). Shutting down scheduler...")
    sys.exit(0)


# ---------------------------------------------------------------------------
# Ana giriş noktası
# ---------------------------------------------------------------------------

def main() -> None:
    logger.info("Scheduler started. Registering jobs...")

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    # ------------------------------------------------------------------
    # Dinamik Dispatcher — DB durumuna göre Active/Inactive sync seçer
    # Her 10 dakikada bir çalışır
    # ------------------------------------------------------------------
    schedule.every(10).minutes.do(dispatch_sync_job)
    logger.info("Registered: Sync Dispatcher (Every 10 minutes)")

    # ------------------------------------------------------------------
    # Sabit Zamanlı OGR Görevleri
    # Her biri farklı saatte çalıştırılır; çakışma riski yoktur.
    # ------------------------------------------------------------------
    schedule.every().day.at("20:00").do(oracle_sync_job)
    logger.info("Registered: Oracle Sync (Every day at 20:00)")

    schedule.every().day.at("21:00").do(postgres_sync_job)
    logger.info("Registered: PostgreSQL Sync (Every day at 21:00)")

    schedule.every().day.at("21:30").do(postgres_sync_4326_job)
    logger.info("Registered: PostgreSQL Sync 4326 (Every day at 21:30)")

    schedule.every().day.at("22:00").do(postgres_kadastro_yeni_sync_job)
    logger.info("Registered: PostgreSQL Kadastro Yeni Sync (Every day at 22:00)")

    logger.info("Scheduler is running. Waiting for jobs...")

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    main()
