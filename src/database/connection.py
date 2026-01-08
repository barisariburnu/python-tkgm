"""PostgreSQL Connection Management with Connection Pooling

Bu modül veritabanı bağlantı yönetiminden sorumludur.
Connection pooling ile performans artışı sağlar.
"""

import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
from loguru import logger
from ..config import settings
import time


class DatabaseConnection:
    """
    PostgreSQL bağlantı yöneticisi - Connection Pooling ile

    Connection Pool Benefits:
    - Reuses connections instead of creating new ones
    - Reduces connection overhead
    - Better resource management
    - Thread-safe
    - Health checks for stale connections
    """

    # Class-level connection pool (singleton)
    _pool = None
    _last_health_check = 0
    _HEALTH_CHECK_INTERVAL = 300  # 5 minutes
    
    def __init__(self):
        # Pydantic Settings kullan (type-safe, validated)
        self.host = settings.POSTGRES_SOURCE_HOST
        self.database = settings.POSTGRES_SOURCE_DB
        self.port = settings.POSTGRES_SOURCE_PORT
        self.user = settings.POSTGRES_SOURCE_USER
        self.password = settings.POSTGRES_SOURCE_PASS
        
        self.connection_string = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        
        # Initialize connection pool if not already created
        if DatabaseConnection._pool is None:
            try:
                DatabaseConnection._pool = psycopg2.pool.SimpleConnectionPool(
                    minconn=2,      # Minimum connections in pool (increased from 1)
                    maxconn=20,     # Maximum connections in pool (increased from 10)
                    host=self.host,
                    database=self.database,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    cursor_factory=RealDictCursor,
                    # Connection options for better reliability
                    options="-c statement_timeout=300000 -c idle_in_transaction_session_timeout=180000"
                )
                logger.info("Connection pool created (min=2, max=20)")
            except Exception as e:
                logger.error(f"Connection pool oluşturulamadı: {e}")
                raise

    def _check_connection_health(self, conn) -> bool:
        """
        Check if a connection is still alive and healthy

        Args:
            conn: psycopg2 connection objesi

        Returns:
            bool: True if connection is healthy, False otherwise
        """
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
            return True
        except Exception:
            return False

    def get_connection(self, max_retries=3):
        """
        Connection pool'dan bağlantı al

        Returns pooled connection instead of creating new one each time.
        Connection must be returned to pool using putconn() or by closing with context manager.

        Args:
            max_retries: Maximum number of retries if connection fails (default: 3)

        Returns:
            Active, healthy database connection
        """
        if DatabaseConnection._pool is None:
            raise Exception("Connection pool henüz başlatılmadı")

        for attempt in range(max_retries):
            try:
                conn = DatabaseConnection._pool.getconn()
                if conn:
                    # Check connection health on first attempt
                    if attempt == 0 and not self._check_connection_health(conn):
                        logger.warning("Stale connection detected, returning to pool and retrying...")
                        DatabaseConnection._pool.putconn(conn, close=True)
                        continue

                    return conn
                else:
                    raise Exception("Pool'dan bağlantı alınamadı")
            except psycopg2.OperationalError as e:
                logger.warning(f"Connection attempt {attempt + 1}/{max_retries} failed: {e}")
                if attempt == max_retries - 1:
                    logger.error(f"Failed to get connection after {max_retries} attempts")
                    raise
                time.sleep(0.5)  # Brief pause before retry
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
