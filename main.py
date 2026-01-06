"""
TKGM WFS Veri Tarayıcısı - Ana Uygulama
Türkiye Tapu ve Kadastro Genel Müdürlü parsel verilerini otomatik olarak toplar
"""

import argparse
from datetime import datetime
from loguru import logger

# Modülleri import et
from src.database import DatabaseManager, SettingsRepository
from src.scraper import TKGMScraper


def main():
    """Ana fonksiyon"""
    scraper = TKGMScraper()

    parser = argparse.ArgumentParser(description='TKGM WFS Veri Çekme Uygulaması')
    parser.add_argument('--fully', action='store_true', help='Tüm parsel verilerini senkronize et')
    parser.add_argument('--daily', action='store_true', help='Günlük parsel verilerini senkronize et')
    parser.add_argument('--daily-inactive', action='store_true', help='Günlük pasif parsel verilerini senkronize et')
    parser.add_argument('--neighbourhoods', action='store_true', help='Mahalle verilerini senkronize et')
    parser.add_argument('--districts', action='store_true', help='İlçe verilerini senkronize et')
    parser.add_argument('--stats', action='store_true', help='İstatistik verilerini göster')
    parser.add_argument('--stats-telegram', action='store_true', help='İstatistikleri Telegram\'a gönder')

    try:
        args = parser.parse_args()

        if args.daily:
            db = DatabaseManager()
            last_setting = db.get_last_setting(SettingsRepository.TYPE_DAILY_SYNC)
            start_index = last_setting.get('start_index', 0)
            start_date = last_setting.get('query_date', datetime.strptime('2025-10-08', '%Y-%m-%d'))
            scraper.sync_daily_parcels(start_date=start_date, start_index=start_index)
        elif args.daily_inactive:
            db = DatabaseManager()
            last_setting = db.get_last_setting(SettingsRepository.TYPE_DAILY_INACTIVE_SYNC)
            start_index = last_setting.get('start_index', 0)
            start_date = last_setting.get('query_date', datetime.strptime('2021-01-01', '%Y-%m-%d'))
            scraper.sync_daily_inactive_parcels(start_date=start_date, start_index=start_index)
        elif args.fully:
            db = DatabaseManager()
            last_setting = db.get_last_setting(SettingsRepository.TYPE_FULLY_SYNC)
            start_index = last_setting.get('start_index', 0)            
            scraper.sync_fully_parcels(start_index=start_index)
        elif args.neighbourhoods:
            scraper.sync_neighbourhoods()
        elif args.districts:
            scraper.sync_districts()
        elif args.stats:
            scraper.show_stats()
        elif args.stats_telegram:
            db = DatabaseManager()
            stats = db.get_statistics()
            if not stats:
                logger.error("İstatistik verileri alınamadı; Telegram gönderimi atlandı")
            else:
                sent = scraper.notifier.send_stats(stats)
                if sent:
                    logger.info("İstatistikler Telegram'a gönderildi")
                else:
                    logger.error("İstatistikler Telegram'a gönderilemedi")
        else:
            parser.print_help()

    except KeyboardInterrupt:
        logger.info("Uygulama kullanıcı tarafından durduruldu")
    except Exception as e:
        logger.error(f"Ana uygulama hatası: {e}")


if __name__ == "__main__":
    main()
