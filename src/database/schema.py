"""Database Schema Management

Bu modül veritabanı şemasının (tablolar, indeksler) oluşturulmasından sorumludur.
"""

from loguru import logger
from .connection import DatabaseConnection


class SchemaManager:
    """Veritabanı şema yöneticisi"""
    
    def __init__(self, db_connection: DatabaseConnection):
        self.db = db_connection
    
    def create_all_tables(self):
        """Tüm tabloları ve indeksleri oluştur"""
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cursor:
                    self._create_parcel_table(cursor)
                    self._create_log_table(cursor)
                    self._create_district_table(cursor)
                    self._create_neighbourhood_table(cursor)
                    self._create_settings_table(cursor)
                    self._create_failed_records_table(cursor)
                    
                    conn.commit()
                    logger.info("Veritabanı tabloları başarıyla oluşturuldu")
                    
        except Exception as e:
            logger.error(f"Tablo oluşturma sırasında hata: {e}")
            raise
    
    def _create_parcel_table(self, cursor):
        """Parsel tablosunu oluştur"""
        # Tablo
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tk_parsel (
                id SERIAL PRIMARY KEY,
                fid BIGINT,
                parselno BIGINT,
                adano BIGINT,
                tapukimlikno BIGINT,
                tapucinsaciklama TEXT,
                tapuzeminref BIGINT,
                tapumahalleref BIGINT,
                tapualan DECIMAL(15,2),
                tip VARCHAR(100),
                belirtmetip VARCHAR(100),
                durum VARCHAR(100),
                geom GEOMETRY(MULTIPOLYGON, 2320),
                sistemkayittarihi TIMESTAMP,
                onaydurum BIGINT,
                kadastroalan DECIMAL(15,2),
                tapucinsid BIGINT,
                sistemguncellemetarihi TIMESTAMP,
                kmdurum VARCHAR(100),
                hazineparseldurum VARCHAR(100),
                terksebep VARCHAR(200),
                detayuretimyontem VARCHAR(100),
                orjinalgeomwkt TEXT,
                orjinalgeomkoordinatsistem VARCHAR(50),
                orjinalgeomuretimyontem VARCHAR(100),
                dom VARCHAR(100),
                epok VARCHAR(50),
                detayverikalite VARCHAR(100),
                orjinalgeomepok VARCHAR(50),
                parseltescildurum VARCHAR(100),
                olcuyontem VARCHAR(100),
                detayarsivonaylikoordinat VARCHAR(100),
                detaypaftazeminuyumluluk VARCHAR(100),
                tesisislemfenkayitref VARCHAR(100),
                terkinislemfenkayitref VARCHAR(100),
                yanilmasiniri DECIMAL(10,2),
                hesapverikalite VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(tapukimlikno, tapuzeminref)
            );
        """)
        
        # İndeksler
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_tk_parsel_geom ON tk_parsel USING GIST (geom);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_tk_parsel_tapukimlikno ON tk_parsel (tapukimlikno);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_tk_parsel_parselno ON tk_parsel (parselno);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_tk_parsel_adano ON tk_parsel (adano);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_tk_parsel_sistemkayittarihi ON tk_parsel (sistemkayittarihi);")
    
    def _create_log_table(self, cursor):
        """Log tablosunu oluştur"""
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tk_logs (
                id SERIAL PRIMARY KEY,
                typename VARCHAR(100) NOT NULL,
                url TEXT NOT NULL,
                feature_count INTEGER DEFAULT 0,
                is_empty BOOLEAN DEFAULT FALSE,
                is_successful BOOLEAN DEFAULT FALSE,
                error_message TEXT,
                http_status_code INTEGER,
                response_xml TEXT,
                response_size INTEGER,
                query_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                execution_duration INTERVAL,
                notes TEXT
            );
        """)
        
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_tk_logs_typename ON tk_logs (typename);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_tk_logs_query_time ON tk_logs (query_time);")
    
    def _create_district_table(self, cursor):
        """İlçe tablosunu oluştur"""
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tk_ilce (
                id SERIAL PRIMARY KEY,
                fid BIGINT,
                ilref BIGINT,
                ad VARCHAR(50),
                tapukimlikno BIGINT,
                durum INTEGER,
                geom GEOMETRY(MULTIPOLYGON, 2320),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(tapukimlikno)
            );
        """)
        
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_tk_ilce_geom ON tk_ilce USING GIST (geom);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_tk_ilce_tapukimlikno ON tk_ilce (tapukimlikno);")
    
    def _create_neighbourhood_table(self, cursor):
        """Mahalle tablosunu oluştur"""
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tk_mahalle (
                id SERIAL PRIMARY KEY,
                fid BIGINT,
                ilceref BIGINT,
                tapukimlikno BIGINT,
                durum INTEGER,
                geom GEOMETRY(MULTIPOLYGON, 2320),
                sistemkayittarihi TIMESTAMP,
                tip INTEGER,
                tapumahallead VARCHAR(50),
                kadastromahallead VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(tapukimlikno)
            );
        """)
        
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_tk_mahalle_geom ON tk_mahalle USING GIST (geom);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_tk_mahalle_tapukimlikno ON tk_mahalle (tapukimlikno);")
    
    def _create_settings_table(self, cursor):
        """Ayarlar tablosunu oluştur"""
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tk_settings (
                id SERIAL PRIMARY KEY,
                query_date TIMESTAMP,
                start_index INTEGER DEFAULT 0,
                neighbourhood_id BIGINT,
                scrape_type BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(scrape_type)
            );
        """)
        
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_tk_settings_query_date ON tk_settings (query_date);")
    
    def _create_failed_records_table(self, cursor):
        """
        Başarısız kayıtlar tablosu - VERİ KAYBI ÖNLENDİ!
        
        UNIQUE constraint ile duplicate prevention:
        - Aynı entity_id + status kombinasyonu 2 kere eklenemez
        - Rollback sonrası tekrar insert denemesi duplicate oluşturmaz
        """
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tk_failed_records (
                id SERIAL PRIMARY KEY,
                entity_type VARCHAR(50) NOT NULL,
                entity_id VARCHAR(255),
                raw_data JSONB NOT NULL,
                error_type VARCHAR(100),
                error_message TEXT,
                stack_trace TEXT,
                retry_count INTEGER DEFAULT 0,
                last_retry_at TIMESTAMP,
                status VARCHAR(50) DEFAULT 'failed',
                resolved_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                -- DUPLICATE ÖNLENDİ!
                UNIQUE(entity_type, entity_id, status)
            );
        """)
        
        # İndeksler - hızlı sorgulama için
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_tk_failed_records_entity_type ON tk_failed_records (entity_type);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_tk_failed_records_status ON tk_failed_records (status);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_tk_failed_records_created_at ON tk_failed_records (created_at);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_tk_failed_records_retry_count ON tk_failed_records (retry_count);")

