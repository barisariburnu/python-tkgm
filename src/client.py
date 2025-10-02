"""
TKGM WFS Servis İstemci Modülü
WFS 1.0.0 uyumlu servis ile etkileşim, kimlik doğrulama ve sayfalandırma
"""

import os
import requests
from requests.auth import HTTPBasicAuth
from urllib.parse import urlencode
from loguru import logger
from dotenv import load_dotenv

load_dotenv()


class TKGMClient:
    """TKGM WFS servis istemci sınıfı"""
    
    def __init__(self):
        self.base_url = os.getenv('TKGM_BASE_URL')
        self.username = os.getenv('TKGM_USERNAME')
        self.password = os.getenv('TKGM_PASSWORD')
        self.typename = os.getenv('TYPENAME', 'TKGM:parseller')
        self.max_features = int(os.getenv('MAXFEATURES', 1000))
        self.cql_filter_base = os.getenv('CQL_FILTER', '(tapukimlikno>0 and tapuzeminref>0 and onaydurum=1)')
        
        # HTTP oturum ayarları
        self.session = requests.Session()
        self.session.auth = HTTPBasicAuth(self.username, self.password)
        self.session.headers.update({
            'User-Agent': 'TKGM-Python-Client/1.0',
            'Accept': 'application/xml, text/xml',
            'Content-Type': 'application/x-www-form-urlencoded'
        })
        
        # Timeout ve retry ayarları
        self.timeout = 300  # 5 dakika
        self.max_retries = 3
        self.retry_delay = 5  # saniye
        
        logger.info("TKGM Client başlatıldı")
    

    def test_connection(self) -> bool:
        """TKGM servis bağlantısını test et"""
        try:
            logger.info("TKGM servis bağlantısı test ediliyor...")
            
            # Minimal bir istek gönder
            test_params = {
                'TYPENAME': self.typename,
                'MAXFEATURES': '1',
                'STARTINDEX': '0',
                'CQL_FILTER': self.cql_filter_base
            }
            
            if '?' in self.base_url:
                test_url = f"{self.base_url}&{urlencode(test_params)}"
            else:
                test_url = f"{self.base_url}?{urlencode(test_params)}"
            
            response = self.session.get(test_url, timeout=30)
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