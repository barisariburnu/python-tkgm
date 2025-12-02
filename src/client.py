"""
TKGM WFS Servis İstemci Modülü
WFS 1.0.0 uyumlu servis ile etkileşim, kimlik doğrulama ve sayfalandırma
"""

import os
import time
import requests
from requests.auth import HTTPBasicAuth
from typing import Optional, Dict
from urllib.parse import urlencode
from datetime import datetime
from loguru import logger
from dotenv import load_dotenv

load_dotenv()


class TKGMClient:
    """TKGM WFS servis istemci sınıfı"""
    
    def __init__(
        self, 
        typename: Optional[str] = None, 
        max_features: Optional[int] = None, 
        db_manager: Optional[object] = None  # DatabaseManager type
    ) -> None:
        self.base_url = os.getenv('TKGM_BASE_URL')
        self.username = os.getenv('TKGM_USERNAME')
        self.password = os.getenv('TKGM_PASSWORD')
        self.typename = typename or os.getenv('PARSELLER', 'TKGM:parseller')
        self.max_features = max_features or int(os.getenv('MAXFEATURES', 1000))
        
        # DatabaseManager referansı (loglama için)
        self.db = db_manager
        
        # HTTP oturum ayarları
        self.session = requests.Session()
        self.session.auth = HTTPBasicAuth(self.username, self.password)
        self.session.headers.update({
            'User-Agent': 'TKGM-Python-Client/1.0',
            'Accept': 'application/xml, text/xml'
        })
        
        # Timeout ve retry ayarları
        self.timeout: int = 300  # 5 dakika
        self.running: bool = True
        self.retry_delay: int = 5  # saniye
        self.max_retries: int = 10  # maksimum deneme sayısı
        
        logger.info("TKGM İstemci başlatıldı")
    

    def test_connection(self) -> bool:
        """TKGM servis bağlantısını test et"""
        try:
            logger.info("TKGM servis bağlantısı test ediliyor...")
            
            # Minimal bir istek gönder
            test_params = {
                'REQUEST': 'GetFeature',
                'SERVICE': 'WFS',
                'SRSNAME': 'EPSG:4326',
                'VERSION': '1.1.2',
                'TYPENAME':  os.getenv('MAHALLELER', 'TKGM:mahalleler'),
                'MAXFEATURES': '1',
                'STARTINDEX': '0'
            }
            
            test_url = f"{self.base_url}?{urlencode(test_params)}"
            logger.debug(f"Test URL: {test_url}")
            response = self.session.get(test_url, timeout=self.timeout)
            response.raise_for_status()
            
            # Yanıtın XML olup olmadığını kontrol et
            content_type = response.headers.get('content-type', '').lower()
            if 'xml' not in content_type:
                logger.warning(f"Beklenmeyen içerik türü: {content_type}")
            
            logger.info("TKGM servis bağlantısı başarılı")
            return True
            
        except Exception as e:
            logger.error(f"TKGM servis bağlantı testi başarısız: {e}")
            return False
    

    def _build_request_params(self, start_index: int = 0, cql_filter: str = None) -> Dict[str, str]:
        """WFS istek parametrelerini oluştur"""
        params = {
            'SERVICE': 'WFS',
            'VERSION': '1.1.2',
            'REQUEST': 'GetFeature',
            'SRSNAME': 'EPSG:4326',
            'TYPENAME': self.typename,
            'MAXFEATURES': str(self.max_features),
            'STARTINDEX': str(start_index)
        }
        
        # CQL filtre varsa ve None/boş değilse ekle
        if cql_filter and cql_filter.strip():
            params['cql_filter'] = cql_filter.strip()
        
        return params


    def fetch_features(self, start_index: int = 0, cql_filter: str = None) -> Optional[Dict]:
        """WFS servisinden özellikleri çek"""
        params = self._build_request_params(start_index, cql_filter)
        url = f"{self.base_url}?{urlencode(params)}"
        logger.info(f"Request URL: {url}")
    
        metadata = {
            'request_url': url,
            'start_index': start_index,
            'max_features': self.max_features,
            'timestamp': datetime.now(),
            'success': False,
            'error_message': None,
            'response_size': 0,
            'execution_time': 0,
            'feature_count': 0,
            'http_status_code': None,
            'response_content': None
        }
        
        start_time = time.time()
        attempt = 0

        while self.running and attempt < self.max_retries:
            attempt += 1

            try:
                logger.info(f"TKGM servisine istek gönderiliyor (Deneme: {attempt}/{self.max_retries})")
        
                response = self.session.get(url, timeout=self.timeout)
                metadata['http_status_code'] = response.status_code

                # HTTP durum kodunu kontrol et
                response.raise_for_status()
                
                # UTF-8 kodlamasını zorla belirt
                response.encoding = 'utf-8'
                
                # Yanıt içeriğini al
                content = response.text
                metadata['response_content'] = content
                metadata['response_size'] = len(content)
                metadata['execution_time'] = time.time() - start_time
                metadata['success'] = True
                
                # XML içeriğinden feature sayısını çıkarmaya çalış
                try:
                    import xml.etree.ElementTree as ET
                    root = ET.fromstring(content)
                    # WFS yanıtında feature sayısını bul
                    features = root.findall('.//{http://www.opengis.net/gml}featureMember')
                    metadata['feature_count'] = len(features)
                    
                    # Sonuç boş mu kontrol et
                    is_empty = metadata['feature_count'] == 0
                    
                except Exception as xml_error:
                    logger.warning(f"XML parse hatası: {xml_error}")
                    metadata['feature_count'] = 0
                    is_empty = True
        
                logger.info(f"TKGM servisinden yanıt alındı: {metadata['response_size']} bayt, {metadata['execution_time']:.2f} saniye, {metadata['feature_count']} özellik")
                
                # Başarılı sorguyu logla
                self.db.insert_log(
                    typename=self.typename,
                    url=url,
                    feature_count=metadata['feature_count'],
                    is_empty=is_empty,
                    is_successful=True,
                    http_status_code=metadata['http_status_code'],
                    response_xml=content,
                    response_size=metadata['response_size'],
                    execution_duration=metadata['execution_time']
                )
                
                return content
            
            except requests.exceptions.Timeout:
                error_msg = f"İstek zaman aşımına uğradı (Deneme: {attempt}/{self.max_retries})"
                logger.warning(error_msg)
                metadata['error_message'] = error_msg
                
                if attempt < self.max_retries:
                    time.sleep(self.retry_delay)
                    
            except requests.exceptions.HTTPError as e:
                error_msg = f"HTTP hatası: {e.response.status_code} - {e.response.reason}"
                logger.error(error_msg)
                metadata['error_message'] = error_msg
                metadata['http_status_code'] = e.response.status_code
                
                # 4xx hataları için tekrar deneme yapma
                if 400 <= e.response.status_code < 500:
                    break
                    
                if attempt < self.max_retries:
                    time.sleep(self.retry_delay)
                    
            except requests.exceptions.ConnectionError:
                error_msg = f"Bağlantı hatası (Deneme: {attempt}/{self.max_retries})"
                logger.warning(error_msg)
                metadata['error_message'] = error_msg
                
                if attempt < self.max_retries:
                    time.sleep(self.retry_delay)
                    
            except Exception as e:
                error_msg = f"Beklenmeyen hata: {str(e)}"
                logger.error(error_msg)
                metadata['error_message'] = error_msg
                break
        
        metadata['execution_time'] = time.time() - start_time
        
        # Maksimum deneme sayısına ulaşıldı mı kontrol et
        if attempt >= self.max_retries:
            metadata['error_message'] = f"Maksimum deneme sayısına ({self.max_retries}) ulaşıldı"
 
        logger.error(f"TKGM servis isteği başarısız: {metadata['error_message']}")

        return None
