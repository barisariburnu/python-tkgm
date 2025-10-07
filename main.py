"""
TKGM WFS Veri Tarayıcısı - Ana Uygulama
Türkiye Tapu ve Kadastro Genel Müdürlüğü parsel verilerini otomatik olarak toplar
"""

import os
import sys
import signal
import argparse
from loguru import logger
from dotenv import load_dotenv
from typing import Optional
from datetime import datetime, timedelta

# Modülleri import et
from src.database import DatabaseManager
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
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
            rotation="10 MB",
            retention="30 days",
            compression="zip"
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
        logger.info(f"Sinyal alındı: {signum}, uygulama kapatılıyor...")
        self.running = False


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
                    # Find mahalleler elements
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


    def sync_daily_parcels(self, neighbourhood_id: int, start_date: datetime, start_index: Optional[int] = 0):
        """Parsel verilerini senkronize et"""
        logger.info(f"Parsel verilerini senkronize etme işlemi başlatılıyor... Başlangıç tarihi: {start_date.strftime('%Y-%m-%d')}")
        
        db = DatabaseManager()
        current_index = start_index
        current_date = start_date.date()
        today = datetime.now().date()
        
        # TKGMClient instance'ını döngü dışında bir kez oluştur
        client = TKGMClient(typename=os.getenv('PARSELLER', 'TKGM:parseller'), db_manager=db)
        
        while current_date <= today and self.running:
            logger.info(f"Mahalle: {neighbourhood_id} - Sayfa {current_index + 1} işleniyor (start_index: {current_index})")

            base_filter = f"(tapukimlikno>0 and tapuzeminref>0 and onaydurum=1 and tapumahalleref={neighbourhood_id})"
            date_filter = f"(sistemkayittarihi>='{start_date.strftime('%Y-%m-%d')}' OR sistemguncellemetarihi>='{start_date.strftime('%Y-%m-%d')}')"
            cql_filter = f"({base_filter} and {date_filter})"

            logger.info(f"Parsel verilerini çekmek için kullanılan CQL filtre: {cql_filter}")
            content = client.fetch_features(start_index=current_index, cql_filter=cql_filter)
            
            if content is None:
                logger.error(f"Mahalle: {neighbourhood_id} - TKGM servisinden parsel verisi alınamadı")
                break
            
            processor = WFSGeometryProcessor()
            
            try:
                # XML'i parse et
                feature_members = processor.parse_wfs_xml(content)
                logger.info(f"Mahalle: {neighbourhood_id} - {len(feature_members)} parsel bulundu")
                
                # Eğer feature member yoksa bir sonraki tarihe geç
                if len(feature_members) == 0:
                    logger.info(f"Mahalle: {neighbourhood_id} - Tarih {current_date.strftime('%Y-%m-%d')} için feature member bulunamadı, bir sonraki tarihe geçiliyor")
                    current_date += timedelta(days=1)
                    current_index = 0  # Yeni tarih için start_index'i sıfırla
                    continue
                
                # Tüm features'ları toplamak için ana liste
                all_features = []
                
                # Process each feature member
                for i, feature_member in enumerate(feature_members):
                    try:
                        # Find parsel elements
                        elements = []
                        for child in feature_member:
                            if 'parsel' in child.tag:
                                elements.append(child)
                        
                        # Her feature_member için parsel işle
                        for elem in elements:
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
                                logger.error(f"Mahalle: {neighbourhood_id} - Öğe işlenirken hata oluştu: {e}")
                                continue
                
                    except Exception as e:
                        logger.error(f"Mahalle: {neighbourhood_id} - Özellik üyesi {i+1} işlenirken hata oluştu: {e}")
                        continue
                
                logger.info(f"Mahalle: {neighbourhood_id} - Toplam {len(all_features)} geometri başarıyla işlendi")
                    
                if not all_features:
                    logger.info(f"Mahalle: {neighbourhood_id} - Bu sayfada parsel verisi bulunamadı")
                    break
                
                features_count = len(all_features)
                logger.info(f"Mahalle: {neighbourhood_id} - Toplam {features_count} parsel özelliği çekildi")
                
                # Veritabanına kaydet
                if all_features:
                    try:
                        db.insert_parcels(all_features)
                        logger.info(f"Mahalle: {neighbourhood_id} - {len(all_features)} parsel veritabanına kaydedildi")
                        
                        # Sonraki sayfa için start_index'i artır
                        current_index += 1
                        
                        # tk_settings tablosuna güncelleme yap - sadece tarih ve index
                        db.update_setting(query_date=current_date, start_index=current_index, neighbourhood_id=neighbourhood_id)
                        logger.info(f"Mahalle: {neighbourhood_id} - Parsel sorgu ayarları güncellendi: query_date={current_date}, start_index={current_index}")
                        
                    except Exception as e:
                        logger.error(f"Mahalle: {neighbourhood_id} - Veritabanına kaydetme hatası: {e}")
                        break
                else:
                    logger.info(f"Mahalle: {neighbourhood_id} - Kaydedilecek parsel verisi bulunamadı")
                    # Veri bulunamadığında bir sonraki tarihe geç
                    current_date += timedelta(days=1)
                    current_index = 0
                    # Yeni tarihe geçerken ayarları güncelle
                    db.update_setting(query_date=current_date, start_index=current_index, neighbourhood_id=neighbourhood_id)
                    continue

            except Exception as e:
                logger.error(f"Mahalle: {neighbourhood_id} - Parsel verilerini işlerken hata: {e}")
                break
        
        logger.info(f"Mahalle: {neighbourhood_id} - Günlük parsel verilerinin senkronizasyonu tamamlandı. Son işlenen sayfa: {current_index}")


    def sync_fully_parcels(self, neighbourhood_id: int, start_index: Optional[int] = 0):
        """Tüm parsel verilerini senkronize et - sayfalama ve tarih kontrolü ile"""
        logger.info(f"Mahalle: {neighbourhood_id} - Tüm parsel verilerini senkronize etme işlemi başlatılıyor...")
        
        db = DatabaseManager()
        current_index = start_index
        current_date = datetime.now()
                
        # TKGMClient instance'ını döngü dışında bir kez oluştur
        client = TKGMClient(typename=os.getenv('PARSELLER', 'TKGM:parseller'), db_manager=db)
        
        while self.running:
            logger.info(f"Mahalle: {neighbourhood_id} - Sayfa {current_index + 1} işleniyor (start_index: {current_index})")
            
            cql_filter = f"(sistemguncellemetarihi>'1900-01-01' and onaydurum=1 and tapumahalleref={neighbourhood_id})"
            
            logger.info(f"Mahalle: {neighbourhood_id} - Parsel verilerini çekmek için kullanılan CQL filtre: {cql_filter}")
            content = client.fetch_features(start_index=current_index, cql_filter=cql_filter)
            
            if content is None:
                logger.error(f"Mahalle: {neighbourhood_id} - TKGM servisinden parsel verisi alınamadı")
                break
            
            processor = WFSGeometryProcessor()
            
            try:
                # XML'i parse et
                feature_members = processor.parse_wfs_xml(content)
                logger.info(f"Mahalle: {neighbourhood_id} - Toplam {len(feature_members)} parsel bulundu")
                
                # Eğer feature member yoksa bir sonraki tarihe geç
                if len(feature_members) == 0:
                    logger.info(f"Mahalle: {neighbourhood_id} - Sayfa {current_index + 1} için feature member bulunamadı, bir sonraki sayfaya geçiliyor")
                    self.running = False
                    continue
                
                # Tüm features'ları toplamak için ana liste
                all_features = []
                
                # Process each feature member
                for i, feature_member in enumerate(feature_members):
                    try:
                        # Find parsel elements
                        elements = []
                        for child in feature_member:
                            if 'parsel' in child.tag:
                                elements.append(child)
                        
                        # Her feature_member için parsel işle
                        for elem in elements:
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
                                logger.error(f"Mahalle: {neighbourhood_id} - Öğe işlenirken hata oluştu: {e}")
                                continue
                
                    except Exception as e:
                        logger.error(f"Mahalle: {neighbourhood_id} - Özellik üyesi {i+1} işlenirken hata oluştu: {e}")
                        continue
                
                logger.info(f"Mahalle: {neighbourhood_id} - Toplam {len(all_features)} geometri başarıyla işlendi")
                    
                if not all_features:
                    logger.info(f"Mahalle: {neighbourhood_id} - Bu sayfada parsel verisi bulunamadı")
                    break
                
                features_count = len(all_features)
                logger.info(f"Mahalle: {neighbourhood_id} - Toplam {features_count} parsel özelliği çekildi")
                
                # Veritabanına kaydet
                if all_features:
                    try:
                        db.insert_parcels(all_features)
                        logger.info(f"Mahalle: {neighbourhood_id} - {len(all_features)} parsel veritabanına kaydedildi")
                        
                        # Sonraki sayfa için start_index'i artır
                        current_index += 1
                        
                        # tk_settings tablosuna güncelleme yap - sadece tarih ve index
                        db.update_setting(query_date=current_date, start_index=current_index, neighbourhood_id=neighbourhood_id, scrape_type=True)
                        logger.info(f"Mahalle: {neighbourhood_id} - Parsel sorgu ayarları güncellendi: query_date={current_date}, start_index={current_index}")

                        # Eğer çekilen parsel sayısı 1000'den azsa, tüm veriler çekilmiş demektir
                        if features_count < 1000:
                            logger.info(f"Mahalle: {neighbourhood_id} - Toplam {features_count} parsel çekildi. Tüm veriler çekildi.")
                            self.running = False
                        
                    except Exception as e:
                        logger.error(f"Mahalle: {neighbourhood_id} - Veritabanına kaydetme hatası: {e}")
                        break
                else:
                    logger.info(f"Mahalle: {neighbourhood_id} - Kaydedilecek parsel verisi bulunamadı")
                    # Veri bulunamadığında bir sonraki tarihe geç
                    self.running = False
                    current_index = 0
                    # Yeni tarihe geçerken ayarları güncelle
                    db.update_setting(query_date=current_date, start_index=current_index, neighbourhood_id=neighbourhood_id, scrape_type=True)
                    continue
                
            except Exception as e:
                logger.error(f"Mahalle: {neighbourhood_id} - Parsel verilerini işlerken hata: {e}")
                break
        
        # İşlem tamamlandığında final güncelleme
        db.update_setting(query_date=current_date, start_index=current_index, neighbourhood_id=neighbourhood_id, scrape_type=True)
        
        if not self.running:
            logger.info(f"Mahalle: {neighbourhood_id} - İşlem kullanıcı tarafından durduruldu")
        else:
            logger.info(f"Mahalle: {neighbourhood_id} - Tüm parsel verilerinin senkronizasyonu tamamlandı. Son işlenen tarih: {current_date.strftime('%Y-%m-%d')}, Son sayfa: {current_index}")


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

    try:
        args = parser.parse_args()

        if args.daily:
            db = DatabaseManager()
            neighbourhoods = db.get_neighbourhoods()

            for neighbourhood in neighbourhoods:
                last_setting = db.get_last_setting(neighbourhood.get("tapukimlikno"), False)
                # Eğer son sorgu tarihi varsa, başlangıç tarihini son sorgu tarihinden sonraki gün olarak ayarla
                yesterday = datetime.now() - timedelta(days=1)
                start_date = last_setting.get('query_date', yesterday)
                start_index = 0

                scraper.sync_daily_parcels(neighbourhood.get("tapukimlikno"), start_date=start_date, start_index=start_index)
        elif args.fully:
            db = DatabaseManager()
            neighbourhoods = db.get_neighbourhoods()

            for neighbourhood in neighbourhoods:
                last_setting = db.get_last_setting(neighbourhood.get("tapukimlikno"), True)
                start_index = last_setting.get('start_index', 0)
                
                scraper.sync_fully_parcels(neighbourhood.get("tapukimlikno"), start_index=start_index)
        elif args.neighbourhoods:
            scraper.sync_neighbourhoods()
        elif args.districts:
            scraper.sync_districts()
        elif args.stats:
            scraper.show_stats()
        else:
            parser.print_help()

    except KeyboardInterrupt:
        logger.info("Uygulama kullanıcı tarafından durduruldu")
    except Exception as e:
        logger.error(f"Ana uygulama hatası: {e}")


if __name__ == "__main__":
    main()