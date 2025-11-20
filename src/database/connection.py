"""PostgreSQL Connection Management with Connection Pooling

Bu modül veritabanı bağlantı yönetiminden sorumludur.
Connection pooling ile performans artışı sağlar.
"""

import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
from loguru import logger
from ..config import settings


class DatabaseConnection:
    """
    PostgreSQL bağlantı yöneticisi - Connection Pooling ile
    
    Connection Pool Benefits:
    - Reuses connections instead of creating new ones
    - Reduces connection overhead
    - Better resource management
    - Thread-safe
    """
    
    # Class-level connection pool (singleton)
    _pool = None
    
    def __init__(self):
        # Pydantic Settings kullan (type-safe, validated)
        self.host = settings.POSTGRES_HOST
        self.database = settings.POSTGRES_DB
        self.port = settings.POSTGRES_PORT
        self.user = settings.POSTGRES_USER
        self.password = settings.POSTGRES_PASS
        
        self.connection_string = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        
        # Initialize connection pool if not already created
        if DatabaseConnection._pool is None:
            try:
                DatabaseConnection._pool = psycopg2.pool.SimpleConnectionPool(
                    minconn=1,      # Minimum connections in pool
                    maxconn=10,     # Maximum connections in pool
                    host=self.host,
                    database=self.database,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    cursor_factory=RealDictCursor
                )
                logger.info("Connection pool created (min=1, max=10)")
            except Exception as e:
                logger.error(f"Connection pool oluşturulamadı: {e}")
                raise
    
    def get_connection(self):
        """
        Connection pool'dan bağlantı al
        
        Returns pooled connection instead of creating new one each time.
        Connection must be returned to pool using putconn() or by closing with context manager.
        """
        try:
            if DatabaseConnection._pool is None:
                raise Exception("Connection pool henüz başlatılmadı")
            
            conn = DatabaseConnection._pool.getconn()
            if conn:
                return conn
            else:
                raise Exception("Pool'dan bağlantı alınamadı")
        except Exception as e:
            logger.error(f"Connection pool'dan bağlantı alınırken hata: {e}")
            raise
    
    def return_connection(self, conn):
        """
        Bağlantıyı pool'a geri ver
        
        Args:
            conn: psycopg2 connection objesi
        """
        if conn and DatabaseConnection._pool:
            DatabaseConnection._pool.putconn(conn)
    
    def test_connection(self) -> bool:
        """Veritabanı bağlantısını test et"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute("SELECT version();")
                version = cursor.fetchone()
                logger.info(f"Veritabanı bağlantısı başarılı (pool): {version['version']}")
            return True
        except Exception as e:
            logger.error(f"Veritabanı bağlantı testi başarısız: {e}")
            return False
        finally:
            if conn:
                self.return_connection(conn)
    
    def check_postgis_extension(self) -> bool:
        """PostGIS uzantısının yüklü olup ol madığını kontrol et"""
        conn = None
        try:
            conn = self.get_connection()
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
        finally:
            if conn:
                self.return_connection(conn)
    
    @classmethod
    def close_all_connections(cls):
        """
        Pool'daki tüm bağlantıları kapat
        
        Uygulama kapanırken çağrılmalı.
        """
        if cls._pool:
            cls._pool.closeall()
            logger.info("Connection pool kapatıldı")
