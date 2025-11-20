"""PostgreSQL Connection Management

Bu modül veritabanı bağlantı yönetiminden sorumludur.
"""

import psycopg2
from psycopg2.extras import RealDictCursor
from loguru import logger
from ..config import settings


class DatabaseConnection:
    """PostgreSQL bağlantı yöneticisi"""
    
    def __init__(self):
        # Pydantic Settings kullan (type-safe, validated)
        self.host = settings.POSTGRES_HOST
        self.database = settings.POSTGRES_DB
        self.port = settings.POSTGRES_PORT
        self.user = settings.POSTGRES_USER
        self.password = settings.POSTGRES_PASS
        
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
