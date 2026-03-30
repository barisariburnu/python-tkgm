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
    parser = argparse.ArgumentParser(description='TKGM WFS Veri Çekme Uygulaması')

    # Senkronizasyon komutları
    parser.add_argument('--fully', action='store_true', help='Tüm parsel verilerini senkronize et')
    parser.add_argument('--daily', action='store_true', help='Günlük parsel verilerini senkronize et')
    parser.add_argument('--daily-inactive', action='store_true', help='Günlük pasif parsel verilerini senkronize et')
    parser.add_argument('--neighbourhoods', action='store_true', help='Mahalle verilerini senkronize et')
    parser.add_argument('--districts', action='store_true', help='İlçe verilerini senkronize et')
    parser.add_argument('--stats', action='store_true', help='İstatistik verilerini göster')
    parser.add_argument('--stats-telegram', action='store_true', help='İstatistikleri Telegram\'a gönder')

    # Log sorgu komutları
    parser.add_argument('--query-logs', action='store_true', help='Log kayıtlarında parsel verisi ara')
    parser.add_argument('--adano', type=int, help='Ada numarası ile filtrele')
    parser.add_argument('--parselno', type=int, help='Parsel numarası ile filtrele')
    parser.add_argument('--tapukimlikno', type=int, help='Tapu kimlik no ile filtrele')
    parser.add_argument('--durum', type=str, help='Durum değeri ile filtrele')
    parser.add_argument('--date-from', type=str, help='Başlangıç tarihi (YYYY-MM-DD)')
    parser.add_argument('--date-to', type=str, help='Bitiş tarihi (YYYY-MM-DD)')
    parser.add_argument('--log-id', type=int, help='Belirli bir log kaydını ID ile görüntüle')
    parser.add_argument('--limit', type=int, default=10, help='Maksimum sonuç sayısı (varsayılan: 10)')

    try:
        args = parser.parse_args()

        if args.query_logs or args.log_id:
            from src.log_explorer import LogExplorer
            db = DatabaseManager()
            explorer = LogExplorer(db)

            if args.log_id:
                explorer.show_log_detail(args.log_id)
            else:
                if not any([args.adano, args.parselno, args.tapukimlikno, args.durum]):
                    print("Hata: En az bir arama kriteri belirtmelisiniz "
                          "(--adano, --parselno, --tapukimlikno, --durum)")
                    parser.print_help()
                    return

                explorer.search_and_display(
                    adano=args.adano,
                    parselno=args.parselno,
                    tapukimlikno=args.tapukimlikno,
                    durum=args.durum,
                    date_from=args.date_from,
                    date_to=args.date_to,
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
