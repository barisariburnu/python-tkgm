"""
TKGM WFS Veri Tarayıcısı - Ana Uygulama
Türkiye Tapu ve Kadastro Genel Müdürlüğü parsel verilerini otomatik olarak toplar
"""

import os
import sys
import signal
from typing import Optional
from datetime import datetime, timedelta
from loguru import logger

# Modülleri import et
from src.database import DatabaseManager
from src.telegram import TelegramNotifier
from src.client import TKGMClient
from .database import DatabaseManager
from .database.repositories import SettingsRepository
from .geometry import WFSGeometryProcessor
from .security import SensitiveDataDataFilter
from src.config import settings


class TKGMScraper:
    """TKGM veri tarayıcısı ana sınıfı"""
    
    def __init__(self):
        # Loglama ayarları
        self._setup_logging()
        
        # Çalışma durumu kontrolü için flag
        self.running = True
        
        # Bileşenleri başlat
        self._initialize_components()
        
        # Telegram bildirim modülü
        self.notifier = TelegramNotifier()
        
        # Sinyal yakalayıcıları ayarla
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        logger.info("TKGM Veri Tarayıcısı başlatıldı")
    

    def _setup_logging(self):
        """Loglama sistemini ayarla - production-grade (rotation, retention, secret maskeleme)"""
        log_level = settings.LOG_LEVEL
        log_file = settings.LOG_FILE

        # Log dizinini oluştur
        os.makedirs(os.path.dirname(log_file), exist_ok=True)

        # --- Self-Cleanup (FIFO) ---
        # Çok büyümesini engellemek için başlangıç temizliği
        try:
            if os.path.exists(log_file):
                file_size = os.path.getsize(log_file)
                max_size = 100 * 1024 * 1024  # 100 MB
                keep_size = 50 * 1024 * 1024  # 50 MB

                if file_size > max_size:
                    print(
                        f"Log dosyası boyutu sınırı aştı "
                        f"({file_size/1024/1024:.2f} MB). Temizleniyor..."
                    )

                    # Son keep_size kadar veriyi oku
                    with open(log_file, 'rb') as f:
                        f.seek(-keep_size, 2)  # Sondan geriye git
                        data = f.read()

                    # Dosyayı yeniden yaz
                    with open(log_file, 'wb') as f:
                        f.write(data)
                        f.write(
                            f"\n[CLEANUP] Log file truncated. "
                            f"Kept last {keep_size/1024/1024:.2f} MB.\n".encode('utf-8')
                        )

                    print("Log dosyası temizlendi.")
        except Exception as e:
            print(f"Log temizleme hatası: {e}")
        # ---------------------------

        # Mevcut logları temizle
        logger.remove()

        # Hassas veri maskeleme filtresi
        secret_filter = SensitiveDataDataFilter()

        # Konsol loglama (renksiz, sadece seviye + mesaj)
        logger.add(
            sys.stdout,
            level=log_level,
            format=(
                "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
                "<level>{level: <8}</level> | "
                "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
                "<level>{message}</level>"
            ),
            filter=secret_filter,
        )

        # Dosya loglama - production-grade rotation
        logger.add(
            log_file,
            level=log_level,
            format=(
                "{time:YYYY-MM-DD HH:mm:ss} | "
                "{level: <8} | {name}:{function}:{line} - {message}"
            ),
            rotation="50 MB",          # 50 MB'da bir rotation
            retention="30 days",      # 30 günden eski log'ları otomatik sil
            compression="zip",        # Eski log'ları zip'le (disk tasarrufu)
            encoding="utf-8",
            enqueue=True,             # Thread-safe (multiprocessing için)
            filter=secret_filter,
        )

        # Hata logu ayrı dosyada (production monitoring için kritik)
        error_log_file = log_file.replace(".log", ".errors.log")
        logger.add(
            error_log_file,
            level="ERROR",
            format=(
                "{time:YYYY-MM-DD HH:mm:ss} | "
                "{level: <8} | {name}:{function}:{line} - {message}\n"
                "{exception}"
            ),
            rotation="20 MB",
            retention="90 days",
            compression="zip",
            encoding="utf-8",
            backtrace=True,
            diagnose=True,
            enqueue=True,
            filter=secret_filter,
        )

        logger.info(
            f"Loglama sistemi ayarlandı: {log_level} seviyesi, "
            f"dosya: {log_file}, hata logu: {error_log_file}"
        )


    def _initialize_components(self):
        """Sistem bileşenlerini başlat"""
        try:
            # Veritabanı bağlantısını oluştur
            self.db = DatabaseManager()

            # Veritabanı bağlantısını test et
            if not self.db.test_connection():
                raise Exception("Veritabanı bağlantısı kurulamadı")
            
            # PostGIS uzantısını kontrol et
            if not self.db.check_postgis_extension():
                logger.warning("PostGIS uzantısı bulunamadı, geometri işlemleri çalışmayabilir")
            
            # Tabloları oluştur
            self.db.create_tables()
            
            # TKGM istemcisini başlat (DatabaseManager referansı ile)
            self.client = TKGMClient(db_manager=self.db)
            # Test connection is skipped during initialization to avoid consuming daily limit
            # Use client.test_connection() manually when needed
            # if not self.client.test_connection():
            #     raise Exception("TKGM servis bağlantısı kurulamadı")
            
            logger.info("Tüm bileşenler başarıyla başlatıldı")
            
        except Exception as e:
            logger.error(f"Bileşen başlatma hatası: {e}")
            sys.exit(1)
    

    def _signal_handler(self, signum, frame):
        """Sinyal yakalayıcısı (Ctrl+C, SIGTERM)"""
        # Birden fazla sinyalde tekrarlı logları önle
        if not getattr(self, 'running', True):
            return
        logger.info(f"Sinyal alındı: {signum}, uygulama kapatılıyor...")
        self.running = False
        # İstemciyi mümkünse durdur ve oturumu kapat
        try:
            if hasattr(self, 'client') and self.client:
                self.client.running = False
                try:
                    # tuple timeout desteği (connect, read)
                    self.client.timeout = (1, 1)  # sonraki denemelerde hızlı zaman aşımı
                    self.client.session.close()
                except Exception:
                    pass
        except Exception:
            pass
        # Ana akışı derhal sonlandır
        raise KeyboardInterrupt


    def sync_districts(self):
        """İlçe verilerini senkronize et"""
        logger.info("İlçe verilerini senkronize etme işlemi başlatılıyor...")
        
        # Check if daily limit is reached
        if self.db.is_daily_limit_reached():
            logger.error("⚠️  Günlük servis limiti daha önce aşılmış. Bugün için işlem yapılamaz.")
            logger.info("Limit yarın sıfırlanacak. Manuel olarak temizlemek için: db.clear_daily_limit()")
            return
        client = TKGMClient(typename=settings.ILCELER, db_manager=self.db)
        content = client.fetch_features()
        
        if content is None:
            logger.error("TKGM servisinden ilçe verisi alınamadı")
            return
        
        processor = WFSGeometryProcessor()
        
        try:
            all_features = processor.process_district_wfs_response(content)
            
            logger.info(f"Toplam {len(all_features)} geometri başarıyla işlendi")
                
            if not all_features:
                logger.info("İlçe verisi bulunamadı")
                return
            
            # Veritabanına kaydet
            try:
                self.db.insert_districts(all_features)
                logger.info(f"{len(all_features)} ilçe veritabanına kaydedildi")
            except Exception as e:
                logger.error(f"Veritabanına kaydetme hatası: {e}")
            
        except Exception as e:
            logger.error(f"İlçe verilerini işlerken hata: {e}")


    def sync_neighbourhoods(self):
        """Mahalle verilerini senkronize et"""
        logger.info("Mahalle verilerini senkronize etme işlemi başlatılıyor...")
        
        # Check if daily limit is reached
        if self.db.is_daily_limit_reached():
            logger.error("⚠️  Günlük servis limiti daha önce aşılmış. Bugün için işlem yapılamaz.")
            logger.info("Limit yarın sıfırlanacak. Manuel olarak temizlemek için: db.clear_daily_limit()")
            return

        client = TKGMClient(typename=settings.MAHALLELER, db_manager=self.db)
        content = client.fetch_features()
        
        if content is None:
            logger.error("TKGM servisinden mahalle verisi alınamadı")
            return
        
        processor = WFSGeometryProcessor()
        
        try:
            all_features = processor.process_neighbourhood_wfs_response(content)
            
            logger.info(f"Toplam {len(all_features)} geometri başarıyla işlendi")
                
            if not all_features:
                logger.info("Mahalle verisi bulunamadı")
                return
            
            # Veritabanına kaydet
            try:
                self.db.insert_neighbourhoods(all_features)
                logger.info(f"{len(all_features)} mahalle veritabanına kaydedildi")
            except Exception as e:
                logger.error(f"Veritabanına kaydetme hatası: {e}")
                
        except Exception as e:
            logger.error(f"Mahalle verilerini işlerken hata: {e}")
            return


    def sync_daily_parcels(self, start_date: Optional[datetime] = None, start_index: Optional[int] = 0):
        """Günlük parsel verilerini senkronize et - sayfalama ve tarih kontrolü ile"""
        logger.info("Günlük parsel verilerini senkronize etme işlemi başlatılıyor...")
        
        # Check if daily limit is reached
        if self.db.is_daily_limit_reached():
            logger.error("⚠️  Günlük servis limiti daha önce aşılmış. Bugün için işlem yapılamaz.")
            logger.info("Limit yarın sıfırlanacak. Manuel olarak temizlemek için: db.clear_daily_limit()")
            return
        
        max_features = settings.MAX_FEATURES
        current_index = start_index
        current_date = start_date if start_date else (datetime.now() - timedelta(days=1))
        # Sadece tamamlanmı günleri işle (Bugünü dahil etme)
        end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        
        
        # Özet metrikleri
        summary_found = 0
        summary_saved = 0
        summary_pages = 0
        summary_errors = 0
        features_count = 0
        
        # TKGMClient örneğini oluştur
        client = TKGMClient(
            typename=settings.PARSELLER,
            max_features=max_features,
            db_manager=self.db
        )
        
        while current_date < end_date and self.running:
            # Sadece mevcut günü sorgula (current_date ile bir sonraki günün başlangıcı arası)
            next_day = current_date + timedelta(days=1)
            # Eğer next_day end_date'i aşıyorsa, end_date'e kadar kısıtla
            query_end = next_day if next_day < end_date else end_date
            
            logger.info(f"[{current_date.isoformat()}] Index {current_index} - {current_index + max_features} arasında işleniyor")
            summary_pages += 1
            
            # CQL filtre oluştur - Sadece o güne ait guncelleme ve kayıtlar
            cql_filter = (
                f"(onaydurum=1 and durum=3 and "
                f"sistemguncellemetarihi>='{current_date.isoformat()}' and "
                f"sistemguncellemetarihi<'{query_end.isoformat()}')"
            )
            
            logger.info(f"Parsel verilerini çekmek için kullanılan CQL filtre: {cql_filter}")
            content = client.fetch_features(start_index=current_index, cql_filter=cql_filter)
            
            if content is None:
                logger.error("TKGM servisinden parsel verisi alınamadı")
                summary_errors += 1
                # Hata durumunda mevcut ilerlemeyi kaydet (aynı güne takılmayı önle)
                self.db.update_setting(
                    query_date=current_date, 
                    start_index=current_index, 
                    scrape_type=SettingsRepository.TYPE_DAILY_SYNC
                )
                break
            
            processor = WFSGeometryProcessor()
            if not self.running:
                break
            
            try:
                # Geometrileri işle
                all_features = processor.process_parcel_wfs_response(content)
                logger.info(f"Bu sayfada {len(all_features)} parsel bulundu")

                if len(all_features) == 0:
                    logger.info(f"[{current_date.isoformat()}] Bu gün için başka veri kalmadı, bir sonraki güne geçiliyor")
                    current_date = next_day
                    current_index = 0
                    # Yeni güne geçişi kaydet
                    self.db.update_setting(query_date=current_date, start_index=current_index, scrape_type=SettingsRepository.TYPE_DAILY_SYNC)
                    continue

                features_count = len(all_features)
                summary_found += features_count
                
                # Veritabanına kaydet ve raporla
                if all_features:
                    try:
                        saved_count = self.db.insert_parcels(all_features)
                        unsaved_count = max(0, features_count - saved_count)
                        logger.info(
                            f"[{current_date}] Index {current_index} - {current_index + max_features} arasında "
                            f"{saved_count} parsel veritabanına kaydedildi, {unsaved_count} kaydedilemedi"
                        )
                        summary_saved += saved_count

                        # Orijinal EPSG:4326 koordinatlariyla tk_parsel_4326 tablosuna da kaydet
                        try:
                            saved_4326_count = self.db.insert_parcels_4326(all_features)
                            logger.info(
                                f"[{current_date}] tk_parsel_4326 tablosuna {saved_4326_count} kayıt yazıldı"
                            )
                        except Exception as e:
                            logger.error(f"tk_parsel_4326 kayıt hatası: {e}")

                        # Başarılı çekim sonrası raporu Telegram'a gönder
                        if self.notifier.is_configured():
                            try:
                                pull_msg = self.notifier.format_pull_report(
                                    date=current_date,
                                    start_index=current_index,
                                    end_index=current_index + max_features,
                                    found=features_count,
                                    saved=saved_count,
                                    unsaved=unsaved_count,
                                    status="Aktif"
                                )
                                self.notifier.send_message(pull_msg)
                            except Exception as e:
                                logger.error(f"Telegram rapor hatası: {e}")

                        # Sonraki sayfa için start_index'i artır
                        current_index += max_features

                        # Eğer bu sayfada gelen veri max_features'tan azsa, bu gün bitmiş demektir
                        if features_count < max_features:
                            logger.info(f"[{current_date.isoformat()}] Gün tamamlandı. Toplam {summary_found} parsel çekildi.")
                            current_date = next_day
                            current_index = 0

                        # Durumu veritabanına kaydet (Her sayfada veya gün geçişinde)
                        self.db.update_setting(
                            query_date=current_date, 
                            start_index=current_index, 
                            scrape_type=SettingsRepository.TYPE_DAILY_SYNC
                        )
                        logger.info(
                            f"Parsel sorgu ayarları güncellendi: query_date={current_date.isoformat()}, start_index={current_index}"
                        )

                    except Exception as e:
                        logger.error(f"Veritabanı kayıt hatası: {e}")
                        summary_errors += 1
                        # Hata durumunda mevcut ilerlemeyi kaydet
                        self.db.update_setting(
                            query_date=current_date, 
                            start_index=current_index, 
                            scrape_type=SettingsRepository.TYPE_DAILY_SYNC
                        )
                        break
                else:
                    logger.info(f"[{current_date.isoformat()}] Kaydedilecek veri bulunamadı, sonraki gün...")
                    current_date = next_day
                    current_index = 0
                    self.db.update_setting(query_date=current_date, start_index=current_index, scrape_type=SettingsRepository.TYPE_DAILY_SYNC)
                    continue
                
            except Exception as e:
                logger.error(f"Parsel işleme hatası: {e}")
                summary_errors += 1
                # Hata durumunda mevcut ilerlemeyi kaydet
                self.db.update_setting(
                    query_date=current_date, 
                    start_index=current_index, 
                    scrape_type=SettingsRepository.TYPE_DAILY_SYNC
                )
                break
        
        # İşlem tamamlandığında final güncelleme
        self.db.update_setting(query_date=current_date, start_index=current_index, scrape_type=SettingsRepository.TYPE_DAILY_SYNC)
        
        if not self.running:
            logger.info("İşlem kullanıcı tarafından durduruldu")
        else:
            logger.info(f"Index {current_index} - {current_index + max_features} arasında toplam {features_count} parsel çekildi. Tüm veriler çekildi. Son işlenen tarih: {current_date.strftime('%Y-%m-%d')}")

    def sync_daily_inactive_parcels(self, start_date: Optional[datetime] = None, start_index: Optional[int] = 0):
        """Günlük pasif parsel verilerini senkronize et - sayfalama ve tarih kontrolü ile"""
        logger.info("Günlük pasif parsel verilerini senkronize etme işlemi başlatılıyor...")

        # Check if daily limit is reached
        if self.db.is_daily_limit_reached():
            logger.error("⚠️  Günlük servis limiti daha önce aşılmış. Bugün için işlem yapılamaz.")
            logger.info("Limit yarın sıfırlanacak. Manuel olarak temizlemek için: db.clear_daily_limit()")
            return

        max_features = settings.MAX_FEATURES
        current_index = start_index
        current_date = start_date if start_date else (datetime.now() - timedelta(days=1))
        # Sadece tamamlanmı günleri işle (Bugünü dahil etme)
        end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)


        # Özet metrikleri
        summary_found = 0
        summary_saved = 0
        summary_pages = 0
        summary_errors = 0
        features_count = 0

        # TKGMClient örneğini oluştur
        client = TKGMClient(
            typename=settings.PARSELLER,
            max_features=max_features,
            db_manager=self.db
        )

        while current_date < end_date and self.running:
            # Sadece mevcut günü sorgula (current_date ile bir sonraki günün başlangıcı arası)
            next_day = current_date + timedelta(days=1)
            # Eğer next_day end_date'i aşıyorsa, end_date'e kadar kısıtla
            query_end = next_day if next_day < end_date else end_date

            logger.info(f"[{current_date.isoformat()}] Index {current_index} - {current_index + max_features} arasında işleniyor")
            summary_pages += 1

            # CQL filtre oluştur - Pasif kayıtlar (onaydurum=1 and durum=2)
            cql_filter = (
                f"(onaydurum=1 and durum=2 and "
                f"sistemguncellemetarihi>='{current_date.isoformat()}' and "
                f"sistemguncellemetarihi<'{query_end.isoformat()}')"
            )

            logger.info(f"Pasif parsel verilerini çekmek için kullanılan CQL filtre: {cql_filter}")
            content = client.fetch_features(start_index=current_index, cql_filter=cql_filter)

            if content is None:
                logger.error("TKGM servisinden pasif parsel verisi alınamadı")
                summary_errors += 1
                # Hata durumunda mevcut ilerlemeyi kaydet (aynı güne takılmayı önle)
                self.db.update_setting(
                    query_date=current_date, 
                    start_index=current_index, 
                    scrape_type=SettingsRepository.TYPE_DAILY_INACTIVE_SYNC
                )
                break

            processor = WFSGeometryProcessor()
            if not self.running:
                break

            try:
                # Geometrileri işle
                all_features = processor.process_parcel_wfs_response(content)
                logger.info(f"Bu sayfada {len(all_features)} pasif parsel bulundu")

                if len(all_features) == 0:
                    logger.info(f"[{current_date.isoformat()}] Bu gün için başka pasif veri kalmadı, bir sonraki güne geçiliyor")
                    current_date = next_day
                    current_index = 0
                    # Yeni güne geçişi kaydet
                    self.db.update_setting(query_date=current_date, start_index=current_index, scrape_type=SettingsRepository.TYPE_DAILY_INACTIVE_SYNC)
                    continue
                features_count = len(all_features)
                summary_found += features_count

                # Veritabanına kaydet ve raporla
                if all_features:
                    try:
                        saved_count = self.db.insert_parcels(all_features)
                        unsaved_count = max(0, features_count - saved_count)
                        logger.info(
                            f"[{current_date}] Index {current_index} - {current_index + max_features} arasında "
                            f"{saved_count} pasif parsel veritabanına kaydedildi, {unsaved_count} kaydedilemedi"
                        )
                        summary_saved += saved_count

                        # Orijinal EPSG:4326 koordinatlariyla tk_parsel_4326 tablosuna da kaydet
                        try:
                            saved_4326_count = self.db.insert_parcels_4326(all_features)
                            logger.info(
                                f"[{current_date}] tk_parsel_4326 (pasif) tablosuna {saved_4326_count} kayıt yazıldı"
                            )
                        except Exception as e:
                            logger.error(f"tk_parsel_4326 (pasif) kayıt hatası: {e}")

                        # Başarılı çekim sonrası raporu Telegram'a gönder
                        if self.notifier.is_configured():
                            try:
                                pull_msg = self.notifier.format_pull_report(
                                    date=current_date,
                                    start_index=current_index,
                                    end_index=current_index + max_features,
                                    found=features_count,
                                    saved=saved_count,
                                    unsaved=unsaved_count,
                                    status="Pasif"
                                )
                                self.notifier.send_message(pull_msg)
                            except Exception as e:
                                logger.error(f"Telegram rapor hatası: {e}")

                        # Sonraki sayfa için start_index'i artır
                        current_index += max_features

                        # Eğer bu sayfada gelen veri max_features'tan azsa, bu gün bitmiş demektir
                        if features_count < max_features:
                            logger.info(f"[{current_date.isoformat()}] Gün tamamlandı. Toplam {summary_found} pasif parsel çekildi.")
                            current_date = next_day
                            current_index = 0

                        # Durumu veritabanına kaydet (Her sayfada veya gün geçişinde)
                        self.db.update_setting(
                            query_date=current_date,
                            start_index=current_index,
                            scrape_type=SettingsRepository.TYPE_DAILY_INACTIVE_SYNC
                        )
                        logger.info(
                            f"Pasif parsel sorgu ayarları güncellendi: query_date={current_date.isoformat()}, start_index={current_index}"
                        )

                    except Exception as e:
                        logger.error(f"Veritabanı kayıt hatası: {e}")
                        summary_errors += 1
                        # Hata durumunda mevcut ilerlemeyi kaydet
                        self.db.update_setting(
                            query_date=current_date, 
                            start_index=current_index, 
                            scrape_type=SettingsRepository.TYPE_DAILY_INACTIVE_SYNC
                        )
                        break
                else:
                    logger.info(f"[{current_date.isoformat()}] Kaydedilecek pasif veri bulunamadı, sonraki gün...")
                    current_date = next_day
                    current_index = 0
                    self.db.update_setting(query_date=current_date, start_index=current_index, scrape_type=SettingsRepository.TYPE_DAILY_INACTIVE_SYNC)
                    continue

            except Exception as e:
                logger.error(f"Pasif parsel işleme hatası: {e}")
                summary_errors += 1
                # Hata durumunda mevcut ilerlemeyi kaydet
                self.db.update_setting(
                    query_date=current_date, 
                    start_index=current_index, 
                    scrape_type=SettingsRepository.TYPE_DAILY_INACTIVE_SYNC
                )
                break

        # İşlem tamamlandığında final güncelleme
        self.db.update_setting(query_date=current_date, start_index=current_index, scrape_type=SettingsRepository.TYPE_DAILY_INACTIVE_SYNC)

        if not self.running:
            logger.info("İşlem kullanıcı tarafından durduruldu")
        else:
            logger.info(f"Index {current_index} - {current_index + max_features} arasında toplam {features_count} pasif parsel çekildi. Tüm veriler çekildi. Son işlenen tarih: {current_date.strftime('%Y-%m-%d')}")

    def sync_fully_parcels(self, start_index: Optional[int] = 0):
        """Tüm parsel verilerini senkronize et - sayfalama ve tarih kontrolü ile"""
        logger.info("Tüm parsel verilerini senkronize etme işlemi başlatılıyor...")
        
        # Check if daily limit is reached
        if self.db.is_daily_limit_reached():
            logger.error("⚠️  Günlük servis limiti daha önce aşılmış. Bugün için işlem yapılamaz.")
            logger.info("Limit yarın sıfırlanacak. Manuel olarak temizlemek için: db.clear_daily_limit()")
            return
        
        max_features = settings.MAX_FEATURES
        cutoff_date = settings.CUTOFF_DATE
        current_index = start_index
        current_date = datetime.now()
        features_count = 0
        
        # TKGMClient örneğini oluştur
        client = TKGMClient(
            typename=settings.PARSELLER,
            max_features=max_features,
            db_manager=self.db
        )
        
        while self.running:
            logger.info(f"Index {current_index} - {current_index + max_features} arasında işleniyor")
            
            cql_filter = f"(onaydurum=1 and sistemguncellemetarihi<'{cutoff_date}' and sistemkayittarihi<'{cutoff_date}')"
            
            logger.info(f"Parsel verilerini çekmek için kullanılan CQL filtre: {cql_filter}")
            content = client.fetch_features(start_index=current_index, cql_filter=cql_filter)
            
            if content is None:
                logger.error("TKGM servisinden parsel verisi alınamadı")
                # Hata durumunda mevcut ilerlemeyi kaydet
                self.db.update_setting(
                    query_date=current_date, 
                    start_index=current_index, 
                    scrape_type=SettingsRepository.TYPE_FULLY_SYNC
                )
                break
            
            processor = WFSGeometryProcessor()
            if not self.running:
                break
            
            try:
                # Geometrileri işle
                all_features = processor.process_parcel_wfs_response(content)
                logger.info(f"Toplam {len(all_features)} parsel bulundu")

                if len(all_features) == 0:
                    logger.info(f"Index {current_index} - {current_index + max_features} arasında feature member bulunamadı, bir sonraki sayfaya geçiliyor")
                    self.running = False
                    continue

                # Veritabanına kaydet
                try:
                    saved_count = self.db.insert_parcels(all_features)
                    unsaved_count = max(0, len(all_features) - saved_count)
                    logger.info(f"{saved_count} parsel veritabanına kaydedildi, {unsaved_count} kaydedilemedi")

                    # Orijinal EPSG:4326 koordinatlariyla tk_parsel_4326 tablosuna da kaydet
                    try:
                        saved_4326_count = self.db.insert_parcels_4326(all_features)
                        logger.info(f"tk_parsel_4326 tablosuna {saved_4326_count} kayıt yazıldı")
                    except Exception as e:
                        logger.error(f"tk_parsel_4326 (full sync) kayıt hatası: {e}")

                    # Sonraki sayfa için start_index'i artır
                    current_index += max_features

                    # tk_settings tablosuna güncelleme yap - sadece tarih ve index
                    self.db.update_setting(query_date=current_date, start_index=current_index, scrape_type=SettingsRepository.TYPE_FULLY_SYNC)
                    logger.info(f"Parsel sorgu ayarları güncellendi: query_date={current_date.strftime('%Y-%m-%d')}, start_index={current_index}")

                    # Eğer çekilen parsel sayısı 1000'den azsa, tüm veriler çekilmiş demektir
                    if len(all_features) < max_features:
                        logger.info(f"Index {current_index} - {current_index + max_features} arasında toplam {len(all_features)} parsel çekildi. Tüm veriler çekildi.")
                        self.running = False

                except Exception as e:
                    logger.error(f"Veritabanına kaydetme hatası: {e}")
                    # Hata durumunda mevcut ilerlemeyi kaydet
                    self.db.update_setting(
                        query_date=current_date, 
                        start_index=current_index, 
                        scrape_type=SettingsRepository.TYPE_FULLY_SYNC
                    )
                    break

            except Exception as e:
                logger.error(f"Parsel verilerini işlerken hata: {e}")
                # Hata durumunda mevcut ilerlemeyi kaydet
                self.db.update_setting(
                    query_date=current_date, 
                    start_index=current_index, 
                    scrape_type=SettingsRepository.TYPE_FULLY_SYNC
                )
                break
        
        # İşlem tamamlandığında final güncelleme
        self.db.update_setting(query_date=current_date, start_index=current_index, scrape_type=SettingsRepository.TYPE_FULLY_SYNC)
        
        if not self.running:
            logger.info("İşlem kullanıcı tarafından durduruldu")
        else:
            logger.info(f"Index {current_index} - {current_index + max_features} arasında toplam {features_count} parsel çekildi. Tüm veriler çekildi. Son işlenen tarih: {current_date.strftime('%Y-%m-%d')}")


    def show_stats(self):
        """Veritabanı istatistiklerini görüntüle"""
        try:
            stats = self.db.get_statistics()
            
            if not stats:
                logger.error("İstatistik verileri alınamadı")
                return
            
            print("\n" + "="*60)
            print("           TKGM VERİTABANI İSTATİSTİKLERİ")
            print("="*60)
            
            # Parsel İstatistikleri
            print("\n📊 PARSEL İSTATİSTİKLERİ:")
            print(f"   • Toplam Parsel Sayısı      : {stats.get('total_parcels', 0):,}")
            print(f"   • Bugün Eklenen            : {stats.get('parcels_today', 0):,}")
            print(f"   • Son 7 Günde Eklenen      : {stats.get('parcels_last_week', 0):,}")
            print(f"   • Toplam Alan (m²)         : {stats.get('total_area', 0):,.2f}")
            
            # Tarih Aralığı
            date_range = stats.get('date_range', {})
            if date_range.get('min_date') and date_range.get('max_date'):
                print(f"   • Tarih Aralığı            : {date_range['min_date']} - {date_range['max_date']}")
            
            # Diğer Veriler
            print("\n🏘️  DİĞER VERİLER:")
            print(f"   • Toplam İlçe Sayısı       : {stats.get('total_districts', 0):,}")
            print(f"   • Toplam Mahalle Sayısı    : {stats.get('total_neighbourhoods', 0):,}")
            
            # Sorgu İstatistikleri
            print("\n🔍 SORGU İSTATİSTİKLERİ:")
            print(f"   • Toplam Sorgu Sayısı      : {stats.get('total_queries', 0):,}")
            print(f"   • Bugün Yapılan Sorgu      : {stats.get('queries_today', 0):,}")
            print(f"   • Ortalama Sonuç/Sorgu     : {stats.get('avg_features_per_query', 0):.1f}")
            
            # Sistem Bilgileri
            print("\n⚙️  SİSTEM BİLGİLERİ:")
            if stats.get('last_update'):
                print(f"   • Son Güncelleme           : {stats['last_update']}")
            
            # Mevcut Ayarlar
            current_settings = stats.get('current_settings', {})
            if current_settings:
                print("\n📋 MEVCUT AYARLAR:")
                if current_settings.get('query_date'):
                    print(f"   • Sorgu Tarihi             : {current_settings['query_date']}")
                print(f"   • Başlangıç İndeksi        : {current_settings.get('start_index', 0)}")
                if current_settings.get('last_updated'):
                    print(f"   • Ayar Güncelleme          : {current_settings['last_updated']}")
            
            print("\n" + "="*60)
            
        except Exception as e:
            logger.error(f"İstatistikleri görüntülerken hata: {e}")

