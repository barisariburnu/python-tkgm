"""PostgreSQL Connection Management

Bu modül veritabanı bağlantı yönetiminden sorumludur.
"""

import os
import psycopg2
from psycopg2.extras import RealDictCursor
from loguru import logger
from dotenv import load_dotenv


class DatabaseConnection:
    """PostgreSQL bağlantı yöneticisi"""
    
    def __init__(self):
        # .env dosyasını yükle
        load_dotenv()
        
        self.host = os.getenv('POSTGRES_HOST')
        self.database = os.getenv('POSTGRES_DB')
        self.port = int(os.getenv('POSTGRES_PORT', 5432))
        self.user = os.getenv('POSTGRES_USER')
        self.password = os.getenv('POSTGRES_PASS')
        
        self.connection_string = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
    
    def get_connection(self):
        """Psycopg2 bağlantısı al"""
        try:
            conn = psycopg2.connect(
                host=self.host,
                database=self.database,
                port=self.port,
                user=self.user,
                password=self.password,
                cursor_factory=RealDictCursor
            )
            return conn
        except Exception as e:
            logger.error(f"Veritabanı bağlantısı kurulurken hata: {e}")
            raise
    
    def test_connection(self) -> bool:
        """Veritabanı bağlantısını test et"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT version();")
                    version = cursor.fetchone()
                    logger.info(f"Veritabanı bağlantısı başarılı: {version['version']}")
                    return True
        except Exception as e:
            logger.error(f"Veritabanı bağlantı testi başarısız: {e}")
            return False
    
    def check_postgis_extension(self) -> bool:
        """PostGIS uzantısının yüklü olup olmadığını kontrol et"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT EXISTS(
                            SELECT 1 FROM pg_extension WHERE extname = 'postgis'
                        );
                    """)
                    exists = cursor.fetchone()['exists']
                    if exists:
                        logger.info("PostGIS uzantısı mevcut")
                    else:
                        logger.warning("PostGIS uzantısı bulunamadı")
                    return exists
        except Exception as e:
            logger.error(f"PostGIS kontrolü sırasında hata: {e}")
            return False
