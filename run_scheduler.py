import schedule
import time
import subprocess
import signal
import sys
from datetime import datetime, date
from loguru import logger
from src.database import DatabaseManager
from src.database.repositories import SettingsRepository

# Loglama ayarları
logger.remove()
logger.add(sys.stdout, format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan> - <level>{message}</level>")

def run_task(command, task_name):
    """Komutu çalıştır ve çıktısını logla"""
    logger.info(f"Task Started: {task_name}")
    try:
        # Komutu çalıştır
        process = subprocess.run(command, shell=True, check=False)
        
        if process.returncode == 0:
            logger.info(f"Task Completed Successfully: {task_name}")
        else:
            logger.error(f"Task Failed: {task_name} (Return Code: {process.returncode})")
            
    except Exception as e:
        logger.error(f"Task Execution Error ({task_name}): {e}")

def daily_active_job():
    run_task("/usr/bin/python3 /app/main.py --daily", "Daily Active Sync")

def daily_inactive_job():
    run_task("/usr/bin/python3 /app/main.py --daily-inactive", "Daily Inactive Sync")

def oracle_sync_job():
    run_task("sed -i 's/\\r$//' /app/scripts/sync-oracle.sh && bash /app/scripts/sync-oracle.sh", "Oracle Sync")

def postgres_sync_job():
    run_task("sed -i 's/\\r$//' /app/scripts/sync-postgresql.sh && bash /app/scripts/sync-postgresql.sh", "PostgreSQL Sync")

def dispatch_sync_job():
    """DB durumuna göre uygun senkronizasyon görevini tetikler"""
    try:
        db = DatabaseManager()
        # Aktif senkronizasyon durumunu kontrol et
        last_setting = db.get_last_setting(SettingsRepository.TYPE_DAILY_SYNC)
        
        # Eğer aktif sync hiç çalışmamışsa önce onu çalıştır
        if not last_setting or 'query_date' not in last_setting:
            logger.info("Setting not found, defaulting to Active Sync")
            daily_active_job()
            return

        query_date = last_setting['query_date']
        if isinstance(query_date, datetime):
            query_date = query_date.date()
            
        today = date.today()
        
        # User Logic: query_date bugünkü tarihten BÜYÜK ve EŞİT ise aktif kayıtlar bitmiş demektir.
        # Bu durumda Pasif (Inactive) senkronizasyona geçilebilir.
        if query_date >= today:
            logger.info(f"Active Sync Finished (Query Date: {query_date} >= Today: {today}). Dispatching Inactive Sync.")
            daily_inactive_job()
        else:
            logger.info(f"Active Sync Ongoing (Query Date: {query_date} < Today: {today}). Dispatching Active Sync.")
            daily_active_job()
            
    except Exception as e:
        logger.error(f"Error in dispatch_sync_job: {e}")

def signal_handler(signum, frame):
    logger.info(f"Signal received ({signum}). Shutting down scheduler...")
    sys.exit(0)

def main():
    logger.info("Scheduler started (Time-Window Mode). Registering jobs...")
    
    # Sinyalleri yakala
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    # Her 10 dakikada bir dispatcher çalışır ve saate göre karar verir
    schedule.every(10).minutes.do(dispatch_sync_job)
    logger.info("Registered: Sync Dispatcher (Every 10 minutes)")
    
    # Sabit Zamanlı Görevler
    schedule.every().day.at("20:00").do(oracle_sync_job)
    logger.info("Registered: Oracle Sync (Every day at 20:00)")
    
    schedule.every().day.at("21:00").do(postgres_sync_job)
    logger.info("Registered: PostgreSQL Sync (Every day at 21:00)")

    # Başlangıçta hemen bir kontrol yapmak isteyebiliriz
    # dispatch_sync_job()
    
    logger.info("Waiting for jobs...")
    
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    main()
