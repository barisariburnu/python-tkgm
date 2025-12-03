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
        """Loglama sistemini ayarla"""
        log_level = settings.LOG_LEVEL
        log_file = settings.LOG_FILE
        
        # Log dizinini oluştur
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        
        # --- Self-Cleanup (FIFO) ---
        try:
            if os.path.exists(log_file):
                file_size = os.path.getsize(log_file)
                max_size = 100 * 1024 * 1024  # 100 MB
                keep_size = 50 * 1024 * 1024  # 50 MB
                
                if file_size > max_size:
                    print(f"Log dosyası boyutu sınırı aştı ({file_size/1024/1024:.2f} MB). Temizleniyor...")
                    
                    # Son keep_size kadar veriyi oku
                    with open(log_file, 'rb') as f:
                        f.seek(-keep_size, 2)  # Sondan geriye git
                        data = f.read()
                    
                    # Dosyayı yeniden yaz
                    with open(log_file, 'wb') as f:
                        f.write(data)
                        f.write(f"\n[CLEANUP] Log file truncated. Kept last {keep_size/1024/1024:.2f} MB.\n".encode('utf-8'))
                    
                    print("Log dosyası temizlendi.")
        except Exception as e:
            print(f"Log temizleme hatası: {e}")
        # ---------------------------
        
        # Mevcut logları temizle
        logger.remove()
        
        # Konsol loglama
        logger.add(
            sys.stdout,
            level=log_level,
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
        )
        
        # Dosya loglama
        logger.add(
            log_file,
            level=log_level,
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}"
        )
        
        logger.info(f"Loglama sistemi ayarlandı: {log_level} seviyesi, dosya: {log_file}")


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
                    self.client.timeout = 1  # sonraki denemelerde hızlı zaman aşımı
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
        end_date = datetime.now()
        
        # Özet metrikleri
        summary_found = 0
        summary_saved = 0
        summary_pages = 0
        summary_empty_pages = 0
        summary_errors = 0
        features_count = 0
        
        # TKGMClient örneğini oluştur
        client = TKGMClient(
            typename=settings.PARSELLER,
            max_features=max_features,
            db_manager=self.db
        )
        
        while current_date < end_date and self.running:
            logger.info(f"[{current_date.isoformat()}] Index {current_index} - {current_index + max_features} arasında işleniyor")
            summary_pages += 1
            
            # CQL filtre oluştur
            cql_filter = f"(onaydurum=1 and durum=3 and sistemguncellemetarihi>='{current_date.isoformat()}' and sistemguncellemetarihi<'{end_date.isoformat()}' and sistemkayittarihi<'{end_date.isoformat()}')"
            
            logger.info(f"Parsel verilerini çekmek için kullanılan CQL filtre: {cql_filter}")
            content = client.fetch_features(start_index=current_index, cql_filter=cql_filter)
            
            if content is None:
                logger.error("TKGM servisinden parsel verisi alınamadı")
                summary_errors += 1
                break
            
            processor = WFSGeometryProcessor()
            if not self.running:
                break
            
            try:
                # XML'i parse et
                feature_members = processor.parse_wfs_xml(content)
                logger.info(f"Toplam {len(feature_members)} parsel bulundu")
                
                if len(feature_members) == 0:
                    logger.info(f"[{current_date.isoformat()}] Index {current_index} - {current_index + max_features} arasında feature member bulunamadı, bir sonraki sayfaya geçiliyor")
                    summary_empty_pages += 1
                    current_date = current_date + timedelta(days=1)
                    current_index = 0
                    continue
                
                # Process each feature member
                all_features = processor.process_parcel_wfs_response(content)

                logger.info(f"Toplam {len(all_features)} geometri başarıyla işlendi")
                
                if not all_features:
                    logger.info(f"[{current_date}] Index {current_index} - {current_index + max_features} arasında parsel verisi bulunamadı")
                    current_date = current_date + timedelta(days=1)
                    current_index = 0
                    continue
                
                features_count = len(all_features)
                logger.info(f"[{current_date}] Index {current_index} - {current_index + max_features} arasında toplam {features_count} parsel özelliği çekildi")
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
                                )
                                self.notifier.send_message(pull_msg)
                            except Exception as e:
                                logger.error(f"Servis çekim raporu gönderilemedi: {e}")

                        # Sonraki sayfa için start_index'i artır
                        current_index += max_features

                        # tk_settings tablosuna güncelleme yap - sadece tarih ve index
                        # Ayarları güncelle
                        self.db.update_setting(
                            query_date=current_date, 
                            start_index=current_index, 
                            scrape_type=SettingsRepository.TYPE_DAILY_SYNC
                        )
                        logger.info(
                            f"Parsel sorgu ayarları güncellendi: query_date={current_date.strftime('%Y-%m-%d')}, start_index={current_index}"
                        )

                        # Eğer çekilen parsel sayısı 1000'den azsa, tüm veriler çekilmiş demektir
                        if features_count < max_features:
                            logger.info(
                                f"[{current_date.isoformat()}] Index {current_index - max_features} - {current_index} arasında toplam "
                                f"{features_count} parsel çekildi. Tüm veriler çekildi."
                            )
                            current_date = current_date + timedelta(days=1)
                            current_index = 0

                    except Exception as e:
                        logger.error(f"Veritabanına kaydetme hatası: {e}")
                        summary_errors += 1
                        break
                else:
                    logger.info(f"[{current_date.isoformat()}] Index {current_index} - {current_index + max_features} arasında kaydedilecek parsel verisi bulunamadı")
                    current_date = current_date + timedelta(days=1)
                    current_index = 0
                    # Yeni tarihe geçerken ayarları güncelle
                    self.db.update_setting(query_date=current_date, start_index=current_index, scrape_type=SettingsRepository.TYPE_DAILY_SYNC)
                    continue
                
            except Exception as e:
                logger.error(f"Parsel verilerini işlerken hata: {e}")
                summary_errors += 1
                break
        
        # İşlem tamamlandığında final güncelleme
        self.db.update_setting(query_date=current_date, start_index=current_index, scrape_type=SettingsRepository.TYPE_DAILY_SYNC)
        
        if not self.running:
            logger.info("İşlem kullanıcı tarafından durduruldu")
        else:
            logger.info(f"Index {current_index} - {current_index + max_features} arasında toplam {features_count} parsel çekildi. Tüm veriler çekildi. Son işlenen tarih: {current_date.strftime('%Y-%m-%d')}")

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
                break
            
            processor = WFSGeometryProcessor()
            if not self.running:
                break
            
            try:
                # XML'i parse et
                feature_members = processor.parse_wfs_xml(content)
                logger.info(f"Toplam {len(feature_members)} parsel bulundu")
                
                if len(feature_members) == 0:
                    logger.info(f"Index {current_index} - {current_index + max_features} arasında feature member bulunamadı, bir sonraki sayfaya geçiliyor")
                    self.running = False
                    continue
                
                # Process each feature member
                all_features = processor.process_parcel_wfs_response(content)
                
                logger.info(f"Toplam {len(all_features)} geometri başarıyla işlendi")
                    
                if not all_features:
                    logger.info("Bu sayfada parsel verisi bulunamadı")
                    break
                
                features_count = len(all_features)
                logger.info(f"Toplam {features_count} parsel özelliği çekildi")
                
                # Veritabanına kaydet
                if all_features:
                    try:
                        saved_count = self.db.insert_parcels(all_features)
                        unsaved_count = max(0, features_count - saved_count)
                        logger.info(f"{saved_count} parsel veritabanına kaydedildi, {unsaved_count} kaydedilemedi")
                        
                        # Sonraki sayfa için start_index'i artır
                        current_index += max_features
                        
                        # tk_settings tablosuna güncelleme yap - sadece tarih ve index
                        self.db.update_setting(query_date=current_date, start_index=current_index, scrape_type=SettingsRepository.TYPE_FULLY_SYNC)
                        logger.info(f"Parsel sorgu ayarları güncellendi: query_date={current_date.strftime('%Y-%m-%d')}, start_index={current_index}")

                        # Eğer çekilen parsel sayısı 1000'den azsa, tüm veriler çekilmiş demektir
                        if features_count < max_features:
                            logger.info(f"Index {current_index} - {current_index + max_features} arasında toplam {features_count} parsel çekildi. Tüm veriler çekildi.")
                            self.running = False
                    
                    except Exception as e:
                        logger.error(f"Veritabanına kaydetme hatası: {e}")
                        break
                else:
                    logger.info(f"Index {current_index} - {current_index + max_features} arasında kaydedilecek parsel verisi bulunamadı")
                    self.running = False
                    # Yeni tarihe geçerken ayarları güncelle
                    self.db.update_setting(query_date=current_date, start_index=current_index, scrape_type=SettingsRepository.TYPE_FULLY_SYNC)
                    continue
                
            except Exception as e:
                logger.error(f"Parsel verilerini işlerken hata: {e}")
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

