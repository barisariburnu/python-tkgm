"""
TKGM WFS Veri Tarayıcısı - Ana Uygulama
Türkiye Tapu ve Kadastro Genel Müdürlü parsel verilerini otomatik olarak toplar
"""

import os
import sys
import signal
import argparse
from loguru import logger
from dotenv import load_dotenv
from typing import Optional
from datetime import datetime, date, timedelta

# Modülleri import et
from src.database import DatabaseManager
from src.telegram import TelegramNotifier
from src.client import TKGMClient
from src.geometry import WFSGeometryProcessor


class TKGMScraper:
    """TKGM veri tarayıcısı ana sınıfı"""
    
    def __init__(self):
        # .env dosyasını yükle
        load_dotenv()
        
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
        log_level = os.getenv('LOG_LEVEL', 'INFO')
        log_file = os.getenv('LOG_FILE', 'logs/scraper.log')
        
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
            if not self.client.test_connection():
                raise Exception("TKGM servis bağlantısı kurulamadı")
            
            # GML parser'ı başlat
            #self.parser = GMLParser()
            
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
        client = TKGMClient(typename=os.getenv('İLÇELER', 'TKGM:ilceler'), db_manager=self.db)
        content = client.fetch_features()
        
        if content is None:
            logger.error("TKGM servisinden ilçe verisi alınamadı")
            return
        
        processor = WFSGeometryProcessor()
        
        try:
            # XML'i parse et
            feature_members = processor.parse_wfs_xml(content)
            logger.info(f"{len(feature_members)} ilçe bulundu")
            
            # Tüm features'ları toplamak için ana liste
            all_features = []
            
            # Process each feature member
            for i, feature_member in enumerate(feature_members):
                try:
                    # Find ilceler elements
                    elements = []
                    for child in feature_member:
                        if 'ilceler' in child.tag:
                            elements.append(child)
                    
                    # Her feature_member için features işle
                    for elem in elements:
                        try:
                            # Extract FID from ilceler element
                            fid_full = elem.get('fid', '')
                            fid_value = None
                            if fid_full and '.' in fid_full:
                                fid_value = fid_full.split('.')[-1]
                            
                            # Initialize feature data with all TKGM fields
                            feature = {
                                'fid': fid_value,
                                'tapukimlikno': None,
                                'ilref': None,
                                'ad': None,
                                'durum': None,
                            }
                            
                            # Extract all feature attributes
                            for child in elem:
                                tag_name = child.tag.split('}')[-1]  # Remove namespace
                                if tag_name == 'tapukimlikno':
                                    feature['tapukimlikno'] = child.text
                                elif tag_name == 'ilref':
                                    feature['ilref'] = child.text
                                elif tag_name == 'ad':
                                    feature['ad'] = child.text
                                elif tag_name == 'durum':
                                    feature['durum'] = child.text
                            
                            # Process geometry
                            geom = processor.process_geometry_element(elem=elem)
                            if geom and geom.get('wkt'):
                                feature['wkt'] = geom['wkt']
                                all_features.append(feature)
                            
                        except Exception as e:
                            logger.error(f"Öğe işlenirken hata oluştu: {e}")
                            continue
            
                except Exception as e:
                    logger.error(f"Özellik üyesi {i+1} işlenirken hata oluştu: {e}")
                    continue
            
            logger.info(f"Toplam {len(all_features)} geometri başarıyla işlendi")
                
            if not all_features:
                logger.info("İlçe verisi bulunamadı")
                return True
            
            features_count = len(all_features)
            logger.info(f"{features_count} ilçe özelliği çekildi")
            
            # Veritabanına kaydet
            if all_features:
                db = DatabaseManager()
                try:
                    db.insert_districts(all_features)
                    logger.info(f"{len(all_features)} ilçe veritabanına kaydedildi")
                except Exception as e:
                    logger.error(f"Veritabanına kaydetme hatası: {e}")
            else:
                logger.info("Kaydedilecek ilçe verisi bulunamadı")
                
        except Exception as e:
            logger.error(f"İlçe verilerini işlerken hata: {e}")
            return


    def sync_neighbourhoods(self):
        """Mahalle verilerini senkronize et"""
        logger.info("Mahalle verilerini senkronize etme işlemi başlatılıyor...")
        client = TKGMClient(typename=os.getenv('MAHALLELER', 'TKGM:mahalleler'), db_manager=self.db)
        content = client.fetch_features()
        
        if content is None:
            logger.error("TKGM servisinden mahalle verisi alınamadı")
            return
        
        processor = WFSGeometryProcessor()
        
        try:
            # XML'i parse et
            feature_members = processor.parse_wfs_xml(content)
            logger.info(f"{len(feature_members)} mahalle bulundu")
            
            # Tüm features'ları toplamak için ana liste
            all_features = []
            
            # Process each feature member
            for i, feature_member in enumerate(feature_members):
                try:
                    # Find mahalleler elements
                    elements = []
                    for child in feature_member:
                        if 'mahalleler' in child.tag:
                            elements.append(child)
                    
                    # Her feature_member için features işle
                    for elem in elements:
                        try:
                            # Extract FID from mahalleler element
                            fid_full = elem.get('fid', '')
                            fid_value = None
                            if fid_full and '.' in fid_full:
                                fid_value = fid_full.split('.')[-1]
                            
                            # Initialize feature data with all TKGM fields
                            feature = {
                                'fid': fid_value,
                                'ilceref': None,
                                'tapukimlikno': None,
                                'durum': None,
                                'sistemkayittarihi': None,
                                'tip': None,
                                'tapumahallead': None,
                                'kadastromahallead': None
                            }
                            
                            # Extract all feature attributes
                            for child in elem:
                                tag_name = child.tag.split('}')[-1]  # Remove namespace
                                if tag_name == 'ilceref':
                                    feature['ilceref'] = child.text
                                elif tag_name == 'tapukimlikno':
                                    feature['tapukimlikno'] = child.text
                                elif tag_name == 'durum':
                                    feature['durum'] = child.text
                                elif tag_name == 'sistemkayittarihi':
                                    feature['sistemkayittarihi'] = child.text
                                elif tag_name == 'tip':
                                    feature['tip'] = child.text
                                elif tag_name == 'tapumahallead':
                                    feature['tapumahallead'] = child.text
                                elif tag_name == 'kadastromahallead':
                                    feature['kadastromahallead'] = child.text
                            
                            # Process geometry
                            geom = processor.process_geometry_element(elem=elem)
                            if geom and geom.get('wkt'):
                                feature['wkt'] = geom['wkt']
                                all_features.append(feature)
                            
                        except Exception as e:
                            logger.error(f"Öğe işlenirken hata oluştu: {e}")
                            continue
            
                except Exception as e:
                    logger.error(f"Özellik üyesi {i+1} işlenirken hata oluştu: {e}")
                    continue
            
            logger.info(f"Toplam {len(all_features)} geometri başarıyla işlendi")
                
            if not all_features:
                logger.info("Mahalle verisi bulunamadı")
                return True
            
            features_count = len(all_features)
            logger.info(f"{features_count} mahalle özelliği çekildi")
            
            # Veritabanına kaydet
            if all_features:
                db = DatabaseManager()
                try:
                    db.insert_neighbourhoods(all_features)
                    logger.info(f"{len(all_features)} mahalle veritabanına kaydedildi")
                except Exception as e:
                    logger.error(f"Veritabanına kaydetme hatası: {e}")
            else:
                logger.info("Kaydedilecek mahalle verisi bulunamadı")
                
        except Exception as e:
            logger.error(f"Mahalle verilerini işlerken hata: {e}")
            return


    def sync_daily_parcels(self, start_date: Optional[datetime] = None, start_index: Optional[int] = 0):
        """Günlük parsel verilerini senkronize et - sayfalama ve tarih kontrolü ile"""
        logger.info(f"Günlük parsel verilerini senkronize etme işlemi başlatılıyor...")
        max_features = int(os.getenv('MAX_FEATURES', 1000))
        
        db = DatabaseManager()
        current_index = start_index
        current_date = start_date if start_date else (datetime.now() - timedelta(days=1))
        end_date = datetime.now()
        
        # Özet metrikleri
        summary_found = 0
        summary_saved = 0
        summary_pages = 0
        summary_empty_pages = 0
        summary_errors = 0
        run_start = current_date
                
        # TKGMClient: sınıf örneğini kullan, sinyal ile durdurulabilir olsun
        client = getattr(self, 'client', None)
        if client is None:
            client = TKGMClient(db_manager=db)
            self.client = client
        client.typename = os.getenv('PARSELLER', 'TKGM:parseller')
        client.max_features = max_features
        
        while current_date < end_date and self.running:
            logger.info(f"[{current_date.isoformat()}] Index {current_index} - {current_index + max_features} arasında işleniyor")
            summary_pages += 1
            
            # CQL filtre oluştur
            cql_filter = f"(onaydurum=1 and sistemguncellemetarihi>='{current_date.isoformat()}' and sistemguncellemetarihi<'{end_date.isoformat()}' and sistemkayittarihi<'{end_date.isoformat()}')"
            
            logger.info(f"Parsel verilerini çekmek için kullanılan CQL filtre: {cql_filter}")
            content = client.fetch_features(start_index=current_index, cql_filter=cql_filter)
            
            if content is None:
                logger.error(f"TKGM servisinden parsel verisi alınamadı")
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
                
                # Tüm features'ları toplamak için ana liste
                all_features = []
                
                # Process each feature member
                for i, feature_member in enumerate(feature_members):
                    if not self.running:
                        break
                    try:
                        # Find parsel elements
                        elements = []
                        for child in feature_member:
                            if 'parsel' in child.tag:
                                elements.append(child)
                        
                        # Her feature_member için parsel işle
                        for elem in elements:
                            if not self.running:
                                break
                            try:
                                # Extract FID from parsel element
                                fid_full = elem.get('fid', '')
                                fid_value = None
                                if fid_full and '.' in fid_full:
                                    fid_value = fid_full.split('.')[-1]
                                
                                # Initialize feature data with all TKGM fields
                                feature = {
                                    'fid': fid_value,
                                    'parselno': None,
                                    'adano': None,
                                    'tapukimlikno': None,
                                    'tapucinsaciklama': None,
                                    'tapuzeminref': None,
                                    'tapumahalleref': None,
                                    'tapualan': None,
                                    'tip': None,
                                    'belirtmetip': None,
                                    'durum': None,
                                    'geom': None,
                                    'sistemkayittarihi': None,
                                    'onaydurum': None,
                                    'kadastroalan': None,
                                    'tapucinsid': None,
                                    'sistemguncellemetarihi': None,
                                    'kmdurum': None,
                                    'hazineparseldurum': None,
                                    'terksebep': None,
                                    'detayuretimyontem': None,
                                    'orjinalgeomwkt': None,
                                    'orjinalgeomkoordinatsistem': None,
                                    'orjinalgeomuretimyontem': None,
                                    'dom': None,
                                    'epok': None,
                                    'detayverikalite': None,
                                    'orjinalgeomepok': None,
                                    'parseltescildurum': None,
                                    'olcuyontem': None,
                                    'detayarsivonaylikoordinat': None,
                                    'detaypaftazeminuyumluluk': None,
                                    'tesisislemfenkayitref': None,
                                    'terkinislemfenkayitref': None,
                                    'yanilmasiniri': None,
                                    'hesapverikalite': None,
                                    'created_at': datetime.now(),
                                    'updated_at': datetime.now()
                                }
                                
                                # Extract all feature attributes
                                for child in elem:
                                    tag_name = child.tag.split('}')[-1]  # Remove namespace
                                    if tag_name == 'parselno':
                                        feature['parselno'] = child.text
                                    elif tag_name == 'adano':
                                        feature['adano'] = child.text
                                    elif tag_name == 'tapukimlikno':
                                        feature['tapukimlikno'] = child.text
                                    elif tag_name == 'tapucinsaciklama':
                                        feature['tapucinsaciklama'] = child.text
                                    elif tag_name == 'tapuzeminref':
                                        feature['tapuzeminref'] = child.text
                                    elif tag_name == 'tapumahalleref':
                                        feature['tapumahalleref'] = child.text
                                    elif tag_name == 'tapualan':
                                        feature['tapualan'] = child.text
                                    elif tag_name == 'tip':
                                        feature['tip'] = child.text
                                    elif tag_name == 'belirtmetip':
                                        feature['belirtmetip'] = child.text
                                    elif tag_name == 'durum':
                                        feature['durum'] = child.text
                                    elif tag_name == 'geom':
                                        feature['geom'] = child.text
                                    elif tag_name == 'sistemkayittarihi':
                                        feature['sistemkayittarihi'] = child.text
                                    elif tag_name == 'onaydurum':
                                        feature['onaydurum'] = child.text
                                    elif tag_name == 'kadastroalan':
                                        feature['kadastroalan'] = child.text
                                    elif tag_name == 'tapucinsid':
                                        feature['tapucinsid'] = child.text
                                    elif tag_name == 'sistemguncellemetarihi':
                                        feature['sistemguncellemetarihi'] = child.text
                                    elif tag_name == 'kmdurum':
                                        feature['kmdurum'] = child.text
                                    elif tag_name == 'hazineparseldurum':
                                        feature['hazineparseldurum'] = child.text
                                    elif tag_name == 'terksebep':
                                        feature['terksebep'] = child.text
                                    elif tag_name == 'detayuretimyontem':
                                        feature['detayuretimyontem'] = child.text
                                    elif tag_name == 'orjinalgeomwkt':
                                        feature['orjinalgeomwkt'] = child.text
                                    elif tag_name == 'orjinalgeomkoordinatsistem':
                                        feature['orjinalgeomkoordinatsistem'] = child.text
                                    elif tag_name == 'orjinalgeomuretimyontem':
                                        feature['orjinalgeomuretimyontem'] = child.text
                                    elif tag_name == 'dom':
                                        feature['dom'] = child.text
                                    elif tag_name == 'epok':
                                        feature['epok'] = child.text
                                    elif tag_name == 'detayverikalite':
                                        feature['detayverikalite'] = child.text
                                    elif tag_name == 'orjinalgeomepok':
                                        feature['orjinalgeomepok'] = child.text
                                    elif tag_name == 'parseltescildurum':
                                        feature['parseltescildurum'] = child.text
                                    elif tag_name == 'olcuyontem':
                                        feature['olcuyontem'] = child.text
                                    elif tag_name == 'detayarsivonaylikoordinat':
                                        feature['detayarsivonaylikoordinat'] = child.text
                                    elif tag_name == 'detaypaftazeminuyumluluk':
                                        feature['detaypaftazeminuyumluluk'] = child.text
                                    elif tag_name == 'tesisislemfenkayitref':
                                        feature['tesisislemfenkayitref'] = child.text
                                    elif tag_name == 'terkinislemfenkayitref':
                                        feature['terkinislemfenkayitref'] = child.text
                                    elif tag_name == 'yanilmasiniri':
                                        feature['yanilmasiniri'] = child.text
                                    elif tag_name == 'hesapverikalite':
                                        feature['hesapverikalite'] = child.text
                                
                                # Process geometry
                                geom = processor.process_geometry_element(elem=elem)
                                if geom and geom.get('wkt'):
                                    feature['wkt'] = geom['wkt']
                                    all_features.append(feature)
                                
                            except Exception as e:
                                logger.error(f"Öğe işlenirken hata oluştu: {e}")
                                continue
                
                    except Exception as e:
                        logger.error(f"Özellik üyesi {i+1} işlenirken hata oluştu: {e}")
                        continue
                
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
                        saved_count = db.insert_parcels(all_features)
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
                        db.update_setting(query_date=current_date, start_index=current_index, scrape_type=False)
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
                    db.update_setting(query_date=current_date, start_index=current_index, scrape_type=False)
                    continue
                
            except Exception as e:
                logger.error(f"Parsel verilerini işlerken hata: {e}")
                summary_errors += 1
                break
        
        # İşlem tamamlandığında final güncelleme
        db.update_setting(query_date=current_date, start_index=current_index, scrape_type=False)
        
        if not self.running:
            logger.info(f"İşlem kullanıcı tarafından durduruldu")
        else:
            logger.info(f"Index {current_index} - {current_index + max_features} arasında toplam {features_count} parsel çekildi. Tüm veriler çekildi. Son işlenen tarih: {current_date.strftime('%Y-%m-%d')}")

    def sync_fully_parcels(self, start_index: Optional[int] = 0):
        """Tüm parsel verilerini senkronize et - sayfalama ve tarih kontrolü ile"""
        logger.info(f"Tüm parsel verilerini senkronize etme işlemi başlatılıyor...")
        max_features = int(os.getenv('MAX_FEATURES', 1000))
        
        db = DatabaseManager()
        current_index = start_index
        current_date = datetime.now()
                
        # TKGMClient: sınıf örneğini kullan, sinyal ile durdurulabilir olsun
        client = getattr(self, 'client', None)
        if client is None:
            client = TKGMClient(db_manager=db)
            self.client = client
        client.typename = os.getenv('PARSELLER', 'TKGM:parseller')
        client.max_features = max_features
        
        while self.running:
            logger.info(f"Index {current_index} - {current_index + max_features} arasında işleniyor")
            
            cql_filter = f"(onaydurum=1 and sistemguncellemetarihi<'2025-10-09' and sistemkayittarihi<'2025-10-09')"
            
            logger.info(f"Parsel verilerini çekmek için kullanılan CQL filtre: {cql_filter}")
            content = client.fetch_features(start_index=current_index, cql_filter=cql_filter)
            
            if content is None:
                logger.error(f"TKGM servisinden parsel verisi alınamadı")
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
                
                # Tüm features'ları toplamak için ana liste
                all_features = []
                
                # Process each feature member
                for i, feature_member in enumerate(feature_members):
                    if not self.running:
                        break
                    try:
                        # Find parsel elements
                        elements = []
                        for child in feature_member:
                            if 'parsel' in child.tag:
                                elements.append(child)
                        
                        # Her feature_member için parsel işle
                        for elem in elements:
                            if not self.running:
                                break
                            try:
                                # Extract FID from parsel element
                                fid_full = elem.get('fid', '')
                                fid_value = None
                                if fid_full and '.' in fid_full:
                                    fid_value = fid_full.split('.')[-1]
                                
                                # Initialize feature data with all TKGM fields
                                feature = {
                                    'fid': fid_value,
                                    'parselno': None,
                                    'adano': None,
                                    'tapukimlikno': None,
                                    'tapucinsaciklama': None,
                                    'tapuzeminref': None,
                                    'tapumahalleref': None,
                                    'tapualan': None,
                                    'tip': None,
                                    'belirtmetip': None,
                                    'durum': None,
                                    'geom': None,
                                    'sistemkayittarihi': None,
                                    'onaydurum': None,
                                    'kadastroalan': None,
                                    'tapucinsid': None,
                                    'sistemguncellemetarihi': None,
                                    'kmdurum': None,
                                    'hazineparseldurum': None,
                                    'terksebep': None,
                                    'detayuretimyontem': None,
                                    'orjinalgeomwkt': None,
                                    'orjinalgeomkoordinatsistem': None,
                                    'orjinalgeomuretimyontem': None,
                                    'dom': None,
                                    'epok': None,
                                    'detayverikalite': None,
                                    'orjinalgeomepok': None,
                                    'parseltescildurum': None,
                                    'olcuyontem': None,
                                    'detayarsivonaylikoordinat': None,
                                    'detaypaftazeminuyumluluk': None,
                                    'tesisislemfenkayitref': None,
                                    'terkinislemfenkayitref': None,
                                    'yanilmasiniri': None,
                                    'hesapverikalite': None,
                                    'created_at': datetime.now(),
                                    'updated_at': datetime.now()
                                }
                                
                                # Extract all feature attributes
                                for child in elem:
                                    tag_name = child.tag.split('}')[-1]  # Remove namespace
                                    if tag_name == 'parselno':
                                        feature['parselno'] = child.text
                                    elif tag_name == 'adano':
                                        feature['adano'] = child.text
                                    elif tag_name == 'tapukimlikno':
                                        feature['tapukimlikno'] = child.text
                                    elif tag_name == 'tapucinsaciklama':
                                        feature['tapucinsaciklama'] = child.text
                                    elif tag_name == 'tapuzeminref':
                                        feature['tapuzeminref'] = child.text
                                    elif tag_name == 'tapumahalleref':
                                        feature['tapumahalleref'] = child.text
                                    elif tag_name == 'tapualan':
                                        feature['tapualan'] = child.text
                                    elif tag_name == 'tip':
                                        feature['tip'] = child.text
                                    elif tag_name == 'belirtmetip':
                                        feature['belirtmetip'] = child.text
                                    elif tag_name == 'durum':
                                        feature['durum'] = child.text
                                    elif tag_name == 'geom':
                                        feature['geom'] = child.text
                                    elif tag_name == 'sistemkayittarihi':
                                        feature['sistemkayittarihi'] = child.text
                                    elif tag_name == 'onaydurum':
                                        feature['onaydurum'] = child.text
                                    elif tag_name == 'kadastroalan':
                                        feature['kadastroalan'] = child.text
                                    elif tag_name == 'tapucinsid':
                                        feature['tapucinsid'] = child.text
                                    elif tag_name == 'sistemguncellemetarihi':
                                        feature['sistemguncellemetarihi'] = child.text
                                    elif tag_name == 'kmdurum':
                                        feature['kmdurum'] = child.text
                                    elif tag_name == 'hazineparseldurum':
                                        feature['hazineparseldurum'] = child.text
                                    elif tag_name == 'terksebep':
                                        feature['terksebep'] = child.text
                                    elif tag_name == 'detayuretimyontem':
                                        feature['detayuretimyontem'] = child.text
                                    elif tag_name == 'orjinalgeomwkt':
                                        feature['orjinalgeomwkt'] = child.text
                                    elif tag_name == 'orjinalgeomkoordinatsistem':
                                        feature['orjinalgeomkoordinatsistem'] = child.text
                                    elif tag_name == 'orjinalgeomuretimyontem':
                                        feature['orjinalgeomuretimyontem'] = child.text
                                    elif tag_name == 'dom':
                                        feature['dom'] = child.text
                                    elif tag_name == 'epok':
                                        feature['epok'] = child.text
                                    elif tag_name == 'detayverikalite':
                                        feature['detayverikalite'] = child.text
                                    elif tag_name == 'orjinalgeomepok':
                                        feature['orjinalgeomepok'] = child.text
                                    elif tag_name == 'parseltescildurum':
                                        feature['parseltescildurum'] = child.text
                                    elif tag_name == 'olcuyontem':
                                        feature['olcuyontem'] = child.text
                                    elif tag_name == 'detayarsivonaylikoordinat':
                                        feature['detayarsivonaylikoordinat'] = child.text
                                    elif tag_name == 'detaypaftazeminuyumluluk':
                                        feature['detaypaftazeminuyumluluk'] = child.text
                                    elif tag_name == 'tesisislemfenkayitref':
                                        feature['tesisislemfenkayitref'] = child.text
                                    elif tag_name == 'terkinislemfenkayitref':
                                        feature['terkinislemfenkayitref'] = child.text
                                    elif tag_name == 'yanilmasiniri':
                                        feature['yanilmasiniri'] = child.text
                                    elif tag_name == 'hesapverikalite':
                                        feature['hesapverikalite'] = child.text
                                
                                # Process geometry
                                geom = processor.process_geometry_element(elem=elem)
                                if geom and geom.get('wkt'):
                                    feature['wkt'] = geom['wkt']
                                    all_features.append(feature)
                                
                            except Exception as e:
                                logger.error(f"Öğe işlenirken hata oluştu: {e}")
                                continue
                
                    except Exception as e:
                        logger.error(f"Özellik üyesi {i+1} işlenirken hata oluştu: {e}")
                        continue
                
                logger.info(f"Toplam {len(all_features)} geometri başarıyla işlendi")
                    
                if not all_features:
                    logger.info(f"Bu sayfada parsel verisi bulunamadı")
                    break
                
                features_count = len(all_features)
                logger.info(f"Toplam {features_count} parsel özelliği çekildi")
                
                # Veritabanına kaydet
                if all_features:
                    try:
                        saved_count = db.insert_parcels(all_features)
                        unsaved_count = max(0, features_count - saved_count)
                        logger.info(f"{saved_count} parsel veritabanına kaydedildi, {unsaved_count} kaydedilemedi")
                        
                        # Sonraki sayfa için start_index'i artır
                        current_index += max_features
                        
                        # tk_settings tablosuna güncelleme yap - sadece tarih ve index
                        db.update_setting(query_date=current_date, start_index=current_index, scrape_type=True)
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
                    db.update_setting(query_date=current_date, start_index=current_index, scrape_type=True)
                    continue
                
            except Exception as e:
                logger.error(f"Parsel verilerini işlerken hata: {e}")
                break
        
        # İşlem tamamlandığında final güncelleme
        db.update_setting(query_date=current_date, start_index=current_index, scrape_type=True)
        
        if not self.running:
            logger.info(f"İşlem kullanıcı tarafından durduruldu")
        else:
            logger.info(f"Index {current_index} - {current_index + max_features} arasında toplam {features_count} parsel çekildi. Tüm veriler çekildi. Son işlenen tarih: {current_date.strftime('%Y-%m-%d')}")


    def show_stats(self):
        """Veritabanı istatistiklerini görüntüle"""
        try:
            db = DatabaseManager()
            stats = db.get_statistics()
            
            if not stats:
                logger.error("İstatistik verileri alınamadı")
                return
            
            print("\n" + "="*60)
            print("           TKGM VERİTABANI İSTATİSTİKLERİ")
            print("="*60)
            
            # Parsel İstatistikleri
            print(f"\n📊 PARSEL İSTATİSTİKLERİ:")
            print(f"   • Toplam Parsel Sayısı      : {stats.get('total_parcels', 0):,}")
            print(f"   • Bugün Eklenen            : {stats.get('parcels_today', 0):,}")
            print(f"   • Son 7 Günde Eklenen      : {stats.get('parcels_last_week', 0):,}")
            print(f"   • Toplam Alan (m²)         : {stats.get('total_area', 0):,.2f}")
            
            # Tarih Aralığı
            date_range = stats.get('date_range', {})
            if date_range.get('min_date') and date_range.get('max_date'):
                print(f"   • Tarih Aralığı            : {date_range['min_date']} - {date_range['max_date']}")
            
            # Diğer Veriler
            print(f"\n🏘️  DİĞER VERİLER:")
            print(f"   • Toplam İlçe Sayısı       : {stats.get('total_districts', 0):,}")
            print(f"   • Toplam Mahalle Sayısı    : {stats.get('total_neighbourhoods', 0):,}")
            
            # Sorgu İstatistikleri
            print(f"\n🔍 SORGU İSTATİSTİKLERİ:")
            print(f"   • Toplam Sorgu Sayısı      : {stats.get('total_queries', 0):,}")
            print(f"   • Bugün Yapılan Sorgu      : {stats.get('queries_today', 0):,}")
            print(f"   • Ortalama Sonuç/Sorgu     : {stats.get('avg_features_per_query', 0):.1f}")
            
            # Sistem Bilgileri
            print(f"\n⚙️  SİSTEM BİLGİLERİ:")
            if stats.get('last_update'):
                print(f"   • Son Güncelleme           : {stats['last_update']}")
            
            # Mevcut Ayarlar
            current_settings = stats.get('current_settings', {})
            if current_settings:
                print(f"\n📋 MEVCUT AYARLAR:")
                if current_settings.get('query_date'):
                    print(f"   • Sorgu Tarihi             : {current_settings['query_date']}")
                print(f"   • Başlangıç İndeksi        : {current_settings.get('start_index', 0)}")
                if current_settings.get('last_updated'):
                    print(f"   • Ayar Güncelleme          : {current_settings['last_updated']}")
            
            print("\n" + "="*60)
            
        except Exception as e:
            logger.error(f"İstatistikleri görüntülerken hata: {e}")


def main():
    """Ana fonksiyon"""
    scraper = TKGMScraper()

    parser = argparse.ArgumentParser(description='TKGM WFS Veri Çekme Uygulaması')
    parser.add_argument('--fully', action='store_true', help='Tüm parsel verilerini senkronize et')
    parser.add_argument('--daily', action='store_true', help='Günlük parsel verilerini senkronize et')
    parser.add_argument('--neighbourhoods', action='store_true', help='Mahalle verilerini senkronize et')
    parser.add_argument('--districts', action='store_true', help='İlçe verilerini senkronize et')
    parser.add_argument('--stats', action='store_true', help='İstatistik verilerini göster')
    parser.add_argument('--stats-telegram', action='store_true', help='İstatistikleri Telegram\'a gönder')

    try:
        args = parser.parse_args()

        if args.daily:
            db = DatabaseManager()
            last_setting = db.get_last_setting(False)
            start_index = last_setting.get('start_index', 0)
            start_date = last_setting.get('query_date', datetime.strptime('2025-10-08', '%Y-%m-%d'))
            scraper.sync_daily_parcels(start_date=start_date, start_index=start_index)
        elif args.fully:
            db = DatabaseManager()
            last_setting = db.get_last_setting(True)
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