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
    
    def __init__(self, typename=None, max_features=None):
        self.base_url = os.getenv('TKGM_BASE_URL')
        self.username = os.getenv('TKGM_USERNAME')
        self.password = os.getenv('TKGM_PASSWORD')
        self.typename = typename or os.getenv('PARSELLER', 'TKGM:parseller')
        self.max_features = max_features or int(os.getenv('MAXFEATURES', 1000))
        
        # HTTP oturum ayarları
        self.session = requests.Session()
        self.session.auth = HTTPBasicAuth(self.username, self.password)
        self.session.headers.update({
            'User-Agent': 'TKGM-Python-Client/1.0',
            'Accept': 'application/xml, text/xml'
        })
        
        # Timeout ve retry ayarları
        self.timeout = 300  # 5 dakika
        self.max_retries = 3
        self.retry_delay = 5  # saniye
        
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
            logger.info(test_url)
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
            params['CQL_FILTER'] = cql_filter.strip()
        
        return params


    def fetch_features(self, start_index: int = 0, cql_filter: str = None) -> Optional[Dict]:
        """WFS servisinden özellikleri çek"""
        params = self._build_request_params(start_index, cql_filter)
        
        if '?' in self.base_url:
            url = f"{self.base_url}&{urlencode(params)}"
        else:
            url = f"{self.base_url}?{urlencode(params)}"
    
        metadata = {
            'request_url': url,
            'start_index': start_index,
            'max_features': self.max_features,
            'timestamp': datetime.now(),
            'success': False,
            'error_message': None,
            'response_size': 0,
            'execution_time': 0
        }
        
        start_time = time.time()
    
        for attempt in range(self.max_retries):
            try:
                logger.info(f"TKGM servisine istek gönderiliyor (Deneme {attempt + 1}/{self.max_retries})")
        
                response = self.session.get(url, timeout=self.timeout)

                # HTTP durum kodunu kontrol et
                response.raise_for_status()
                
                # Yanıt içeriğini al
                content = response.text
                metadata['response_size'] = len(content)
                metadata['execution_time'] = time.time() - start_time
                metadata['success'] = True
        
                logger.info(f"TKGM servisinden yanıt alındı: {metadata['response_size']} bayt, {metadata['execution_time']:.2f} saniye")
                
                return content
            
            except requests.exceptions.Timeout:
                error_msg = f"İstek zaman aşımına uğradı (Deneme {attempt + 1})"
                logger.warning(error_msg)
                metadata['error_message'] = error_msg
                
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                    continue
                    
            except requests.exceptions.HTTPError as e:
                error_msg = f"HTTP hatası: {e.response.status_code} - {e.response.reason}"
                logger.error(error_msg)
                metadata['error_message'] = error_msg
                
                # 4xx hataları için tekrar deneme yapma
                if 400 <= e.response.status_code < 500:
                    break
                    
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                    continue
                    
            except requests.exceptions.ConnectionError:
                error_msg = f"Bağlantı hatası (Deneme {attempt + 1})"
                logger.warning(error_msg)
                metadata['error_message'] = error_msg
                
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                    continue
                    
            except Exception as e:
                error_msg = f"Beklenmeyen hata: {str(e)}"
                logger.error(error_msg)
                metadata['error_message'] = error_msg
                break
        
        metadata['execution_time'] = time.time() - start_time
        logger.error(f"TKGM servis isteği başarısız: {metadata['error_message']}")