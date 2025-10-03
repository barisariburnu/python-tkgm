"""PostgreSQL Veritabanı Bağlantı ve İşlem Modülü

Bu modül PostgreSQL veritabanı bağlantısını yönetir ve
parsel verilerini kaydetme işlemlerini gerçekleştirir.
"""

import os
import psycopg2
from psycopg2.extras import RealDictCursor
from loguru import logger
from typing import List, Dict, Any
from dotenv import load_dotenv


class DatabaseManager:
    """PostgreSQL veritabanı yönetim sınıfı"""
    
    def __init__(self):
        # .env dosyasını yükle
        load_dotenv()
        
        self.host = os.getenv('DB_HOST')
        self.database = os.getenv('DB_NAME')
        self.port = int(os.getenv('DB_PORT', 5432))
        self.user = os.getenv('DB_USER')
        self.password = os.getenv('DB_PASSWORD')
        
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


    def create_tables(self):
        """Gerekli tabloları oluştur"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Parseller tablosu - tüm alanları içeren yeni yapı
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS tk_parseller (
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
                            onaydurum INTEGER,
                            kadastroalan DECIMAL(15,2),
                            tapucinsid INTEGER,
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
                    
                    # Geometri indeksleri
                    cursor.execute("""
                        CREATE INDEX IF NOT EXISTS idx_tk_parseller_geom 
                        ON tk_parseller USING GIST (geom);
                    """)
                    
                    # Diğer indeksler
                    cursor.execute("""
                        CREATE INDEX IF NOT EXISTS idx_tk_parseller_tapukimlikno 
                        ON tk_parseller (tapukimlikno);
                    """)
                    
                    cursor.execute("""
                        CREATE INDEX IF NOT EXISTS idx_tk_parseller_parselno 
                        ON tk_parseller (parselno);
                    """)
                    
                    cursor.execute("""
                        CREATE INDEX IF NOT EXISTS idx_tk_parseller_adano 
                        ON tk_parseller (adano);
                    """)
                    
                    cursor.execute("""
                        CREATE INDEX IF NOT EXISTS idx_tk_parseller_sistemkayittarihi 
                        ON tk_parseller (sistemkayittarihi);
                    """)
                    
                    # Sorgu geçmişi tablosu
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS tk_logs (
                            id SERIAL PRIMARY KEY,
                            
                            -- Sorgu parametreleri                   
                            typename VARCHAR(100) NOT NULL,
                            url TEXT NOT NULL,
                            
                            -- Sorgu sonuçları
                            feature_count INTEGER DEFAULT 0,
                            is_empty BOOLEAN DEFAULT FALSE,
                            is_successful BOOLEAN DEFAULT FALSE,
                            
                            -- Hata bilgileri
                            error_message TEXT,
                            http_status_code INTEGER,
                            
                            -- Yanıt bilgileri
                            response_xml TEXT,
                            response_size INTEGER,
                            
                            -- Zaman bilgileri
                            query_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            execution_duration INTERVAL,
                            
                            -- Ek bilgiler
                            notes TEXT
                        );
                    """)
                    
                    # Sorgu geçmişi indeksleri
                    cursor.execute("""
                        CREATE INDEX IF NOT EXISTS idx_tk_logs_typename 
                        ON tk_logs (typename);
                    """)
                    
                    cursor.execute("""
                        CREATE INDEX IF NOT EXISTS idx_tk_logs_query_time 
                        ON tk_logs (query_time);
                    """)
                    
                    # İlçe tablosu
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS tk_ilceler (
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
                    
                    # Geometri indeksleri
                    cursor.execute("""
                        CREATE INDEX IF NOT EXISTS idx_tk_ilceler_geom 
                        ON tk_ilceler USING GIST (geom);
                    """)
                    
                    # Diğer indeksler
                    cursor.execute("""
                        CREATE INDEX IF NOT EXISTS idx_tk_ilceler_tapukimlikno 
                        ON tk_ilceler (tapukimlikno);
                    """)
                    
                    # Mahalle tablosu
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS tk_mahalleler (
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
                    
                    # Geometri indeksleri
                    cursor.execute("""
                        CREATE INDEX IF NOT EXISTS idx_tk_mahalleler_geom 
                        ON tk_mahalleler USING GIST (geom);
                    """)
                    
                    # Diğer indeksler
                    cursor.execute("""
                        CREATE INDEX IF NOT EXISTS idx_tk_mahalleler_tapukimlikno 
                        ON tk_mahalleler (tapukimlikno);
                    """)
                    
                    conn.commit()
                    logger.info("Veritabanı tabloları başarıyla oluşturuldu")
                    
        except Exception as e:
            logger.error(f"Tablo oluşturma sırasında hata: {e}")
            raise

    
    def insert_districts(self, features: List[Dict[str, Any]]) -> int:
        """Mahalle verilerini veritabanına kaydet"""
        if not features:
            logger.warning("Kayıt yapılacak ilçe verisi bulunamadı")
            return 0

        saved_count = 0
        skipped_count = 0
        error_count = 0
        
        for feature in features:
            geom = None

            try:
                # Gerekli alanları kontrol et
                if 'fid' not in feature or not feature['fid']:
                    logger.warning(f"İlçe fid değeri eksik, atlanıyor: {feature}")
                    skipped_count += 1
                    continue

                # Geometri verilerini oluştur
                try:
                    # İlçe geometri verilerini kontrol et
                    if 'wkt' in feature and isinstance(feature['wkt'], str):
                        # GML Parser'dan gelen geometri verilerini kullan
                        geom = feature.get('wkt')
                        
                        # Geometri verilerinin geçerli olduğunu kontrol et
                        if not geom:
                            raise ValueError("Geçerli geometri verileri bulunamadı")
                except Exception as e:
                    logger.error(f"Geometri oluşturulurken hata: {e}")
                    logger.warning(f"İlçe geometri değeri oluşturulamadı, atlanıyor: {feature}")
                    skipped_count += 1
                    continue

                # Her ilçe için ayrı transaction kullan
                try:
                    with self.get_connection() as conn:
                        with conn.cursor() as cursor:
                            # tk_ilceler tablosuna ekle/güncelle
                            cursor.execute("""
                            INSERT INTO tk_ilceler (fid, tapukimlikno, ilref, ad, durum, geom)
                            VALUES (%s, %s, %s, %s, %s, ST_GeomFromText(%s, 2320))
                            ON CONFLICT (tapukimlikno) DO UPDATE SET
                                fid = EXCLUDED.fid,
                                ilref = EXCLUDED.ilref,
                                ad = EXCLUDED.ad,
                                durum = EXCLUDED.durum,
                                geom = ST_GeomFromText(%s, 2320)
                            """, (
                                feature.get('fid'),
                                feature.get('tapukimlikno', 0),
                                feature.get('ilref', 0),
                                feature.get('ad', ''),
                                feature.get('durum', 0),
                                geom,
                                geom
                            ))

                            conn.commit()
                            cursor.close()
                            saved_count += 1
                        
                except Exception as e:
                    logger.error(f"İlçe kaydedilirken hata: {e}")
                    import traceback
                    logger.error(f"Hata detayı: {traceback.format_exc()}")
                    logger.debug(f"Hatalı ilçe fid: {feature.get('fid', 'N/A')}, ad: {feature.get('ad', 'N/A')}")
                    error_count += 1
                    # Tek bir ilçe hatası tüm işlemi durdurmaz, devam et
                    continue
                    
            except Exception as e:
                logger.error(f"İlçe işlenirken hata: {e}")
                error_count += 1
                continue
        
        logger.info(f"{saved_count} ilçe başarıyla veritabanına kaydedildi")
        
        if skipped_count > 0:
            logger.warning(f"{skipped_count} ilçe atlandı (eksik veri)")
        if error_count > 0:
            logger.warning(f"{error_count} ilçe hata nedeniyle kaydedilemedi")
        
        logger.info(f"Toplam işlenen: {len(features)}, Kaydedilen: {saved_count}, Atlanan: {skipped_count}, Hatalı: {error_count}")

        return saved_count

    
    def insert_neighbourhoods(self, features: List[Dict[str, Any]]) -> int:
        """Mahalle verilerini veritabanına kaydet"""
        if not features:
            logger.warning("Kayıt yapılacak mahalle verisi bulunamadı")
            return 0

        saved_count = 0
        skipped_count = 0
        error_count = 0
        
        for feature in features:
            geom = None

            try:
                # Gerekli alanları kontrol et
                if 'fid' not in feature or not feature['fid']:
                    logger.warning(f"Mahalle fid değeri eksik, atlanıyor: {feature}")
                    skipped_count += 1
                    continue

                # Geometri verilerini oluştur
                try:
                    # Mahalle geometri verilerini kontrol et
                    if 'wkt' in feature and isinstance(feature['wkt'], str):
                        # GML Parser'dan gelen geometri verilerini kullan
                        geom = feature.get('wkt')
                        
                        # Geometri verilerinin geçerli olduğunu kontrol et
                        if not geom:
                            raise ValueError("Geçerli geometri verileri bulunamadı")
                except Exception as e:
                    logger.error(f"Geometri oluşturulurken hata: {e}")
                    logger.warning(f"Mahalle geometri değeri oluşturulamadı, atlanıyor: {feature}")
                    skipped_count += 1
                    continue

                # Her mahalle için ayrı transaction kullan
                try:
                    with self.get_connection() as conn:
                        with conn.cursor() as cursor:
                            # tk_mahalle tablosuna ekle/güncelle
                            cursor.execute("""
                            INSERT INTO tk_mahalleler (
                                fid, ilceref, tapukimlikno, durum, sistemkayittarihi,
                                tip, tapumahallead, kadastromahallead, geom
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s, %s, %s, ST_GeomFromText(%s, 2320)
                            ) ON CONFLICT (tapukimlikno) DO UPDATE SET
                                fid = EXCLUDED.fid,
                                ilceref = EXCLUDED.ilceref,
                                durum = EXCLUDED.durum,
                                sistemkayittarihi = EXCLUDED.sistemkayittarihi,
                                tip = EXCLUDED.tip,
                                tapumahallead = EXCLUDED.tapumahallead,
                                kadastromahallead = EXCLUDED.kadastromahallead,
                                geom = ST_GeomFromText(%s, 2320),
                                updated_at = CURRENT_TIMESTAMP
                            """, (
                                feature.get('fid'),
                                feature.get('ilceref', 0),
                                feature.get('tapukimlikno', 0),
                                feature.get('durum', 0),
                                feature.get('sistemkayittarihi'),
                                feature.get('tip', 0),
                                feature.get('tapumahallead', ''),
                                feature.get('kadastromahallead', ''),
                                geom,
                                geom
                            ))

                            conn.commit()
                            cursor.close()
                            saved_count += 1
                        
                except Exception as e:
                    logger.error(f"Mahalle kaydedilirken hata: {e}")
                    import traceback
                    logger.error(f"Hata detayı: {traceback.format_exc()}")
                    logger.debug(f"Hatalı mahalle fid: {feature.get('fid', 'N/A')}, tapumahallead: {feature.get('tapumahallead', 'N/A')}")
                    error_count += 1
                    # Tek bir mahalle hatası tüm işlemi durdurmaz, devam et
                    continue
                    
            except Exception as e:
                logger.error(f"Mahalle işlenirken hata: {e}")
                error_count += 1
                continue
        
        logger.info(f"{saved_count} mahalle başarıyla veritabanına kaydedildi")
        
        if skipped_count > 0:
            logger.warning(f"{skipped_count} mahalle atlandı (eksik veri)")
        if error_count > 0:
            logger.warning(f"{error_count} mahalle hata nedeniyle kaydedilemedi")
        
        logger.info(f"Toplam işlenen: {len(features)}, Kaydedilen: {saved_count}, Atlanan: {skipped_count}, Hatalı: {error_count}")

        return saved_count