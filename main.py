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
        
        # Bileşenleri başlat
        self._initialize_components()
        
        # Sinyal yakalayıcıları ayarla
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        logger.info("TKGM Scraper başlatıldı")
    

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
            db = DatabaseManager()

            # Veritabanı bağlantısını test et
            if not db.test_connection():
                raise Exception("Veritabanı bağlantısı kurulamadı")
            
            # PostGIS uzantısını kontrol et
            if not db.check_postgis_extension():
                logger.warning("PostGIS uzantısı bulunamadı, geometri işlemleri çalışmayabilir")
            
            # Tabloları oluştur
            db.create_tables()
            
            # TKGM istemcisini başlat
            self.client = TKGMClient()
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


    def sync_neighbourhoods(self):
        """Mahalle verilerini senkronize et"""
        logger.info("Mahalle verilerini senkronize etme işlemi başlatılıyor...")
        client = TKGMClient(typename=os.getenv('MAHALLELER'))
        content = client.fetch_features()
            
        if not content:
            logger.warning("Mahalle verileri çekilemedi")
            return False
        
        try:
            # Parse XML
            processor = WFSGeometryProcessor()
            feature_members = processor.parse_wfs_xml(content)
            logger.info(f"{len(feature_members)} mahalle bulundu")
            
            # Process each feature member
            for i, feature_member in enumerate(feature_members):
                try:
                    # Find mahalleler elements
                    elements = []
                    for child in feature_member:
                        if 'mahalleler' in child.tag:
                            elements.append(child)
                    
                    features = []
                    for elem in elements:
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
                        
                    geom = processor.process_geometry_element(elem=elem)
                    feature['wkt'] = geom['wkt']
            
                except Exception as e:
                    logger.error(f"Failed to process feature member {i+1}: {e}")
                    continue
            
            logger.info(f"Successfully processed {len(features)} geometries")
                
            if not features:
                logger.info("Mahalle verisi bulunamadı")
                return True
            
            features_count = len(features)
            logger.info(f"{features_count} mahalle özelliği çekildi")
            
            # Veritabanına kaydet
            db = DatabaseManager()
            saved_count = db.insert_neighbourhoods(features)
            logger.info(f"{saved_count} mahalle veritabanına kaydedildi")
            return True
                
        except Exception as e:
            logger.error(f"Mahalle senkronizasyonu sırasında hata: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False


def main():
    """Ana fonksiyon"""
    scraper = TKGMScraper()

    parser = argparse.ArgumentParser(description='TKGM WFS Veri Çekme Uygulaması')
    parser.add_argument('--full', action='store_true', help='Tüm parsel verilerini senkronize et')
    parser.add_argument('--daily', action='store_true', help='Günlük parsel verilerini senkronize et')
    parser.add_argument('--neighbourhoods', action='store_true', help='Mahalle verilerini senkronize et')
    parser.add_argument('--districts', action='store_true', help='İlçe verilerini senkronize et')
    parser.add_argument('--stats', action='store_true', help='İstatistik verilerini göster')

    try:
        args = parser.parse_args()

        scraper.sync_neighbourhoods()

    except KeyboardInterrupt:
        logger.info("Uygulama kullanıcı tarafından durduruldu")
    except Exception as e:
        logger.error(f"Ana uygulama hatası: {e}")


if __name__ == "__main__":
    main()