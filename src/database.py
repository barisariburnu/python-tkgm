"""PostgreSQL Veritabanı Bağlantı ve İşlem Modülü

Bu modül PostgreSQL veritabanı bağlantısını yönetir ve
parsel verilerini kaydetme işlemlerini gerçekleştirir.
"""

import os
import psycopg2
from psycopg2.extras import RealDictCursor
from loguru import logger
from typing import List, Dict, Any, Optional
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
                    
                    # Ayarlar tablosu
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS tk_settings (
                            id SERIAL PRIMARY KEY,
                            query_date TIMESTAMP,
                            start_index INTEGER DEFAULT 0,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        );
                    """)
                    
                    # Ayarlar indeksleri
                    cursor.execute("""
                        CREATE INDEX IF NOT EXISTS idx_tk_settings_query_date 
                        ON tk_settings (query_date);
                    """)

                    cursor.execute("""
                        INSERT INTO tk_settings (query_date, start_index)
                        VALUES ('1900-01-01', 0)
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
                            # tk_mahalleler tablosuna ekle/güncelle
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

    
    def insert_parcels(self, features: List[Dict[str, Any]]) -> int:
        """Parsel verilerini veritabanına kaydet"""
        if not features:
            logger.warning("Kayıt yapılacak parsel verisi bulunamadı")
            return 0

        saved_count = 0
        skipped_count = 0
        error_count = 0
        
        for feature in features:
            geom = None

            try:
                # Gerekli alanları kontrol et
                if 'fid' not in feature or not feature['fid']:
                    logger.warning(f"Parsel fid değeri eksik, atlanıyor: {feature}")
                    skipped_count += 1
                    continue

                # Geometri verilerini oluştur
                try:
                    # Parsel geometri verilerini kontrol et
                    if 'wkt' in feature and isinstance(feature['wkt'], str):
                        # GML Parser'dan gelen geometri verilerini kullan
                        geom = feature.get('wkt')
                        
                        # Geometri verilerinin geçerli olduğunu kontrol et
                        if not geom:
                            raise ValueError("Geçerli geometri verileri bulunamadı")
                except Exception as e:
                    logger.error(f"Geometri oluşturulurken hata: {e}")
                    logger.warning(f"Parsel geometri değeri oluşturulamadı, atlanıyor: {feature}")
                    skipped_count += 1
                    continue

                # Her parsel için ayrı transaction kullan
                try:
                    with self.get_connection() as conn:
                        with conn.cursor() as cursor:
                            # tk_parseller tablosuna ekle/güncelle
                            cursor.execute("""
                            INSERT INTO tk_parseller (
                                fid, parselno, adano, tapukimlikno, tapucinsaciklama,
                                tapuzeminref, tapumahalleref, tapualan, tip, belirtmetip,
                                durum, sistemkayittarihi, onaydurum, kadastroalan,
                                tapucinsid, sistemguncellemetarihi, kmdurum, hazineparseldurum,
                                terksebep, detayuretimyontem, orjinalgeomwkt, 
                                orjinalgeomkoordinatsistem, orjinalgeomuretimyontem, dom,
                                epok, detayverikalite, orjinalgeomepok, parseltescildurum,
                                olcuyontem, detayarsivonaylikoordinat, detaypaftazeminuyumluluk,
                                tesisislemfenkayitref, terkinislemfenkayitref, yanilmasiniri,
                                hesapverikalite, geom
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, ST_GeomFromText(%s, 2320)
                            ) ON CONFLICT (tapukimlikno, tapuzeminref) DO UPDATE SET
                                fid = EXCLUDED.fid,
                                parselno = EXCLUDED.parselno,
                                adano = EXCLUDED.adano,
                                tapucinsaciklama = EXCLUDED.tapucinsaciklama,
                                tapumahalleref = EXCLUDED.tapumahalleref,
                                tapualan = EXCLUDED.tapualan,
                                tip = EXCLUDED.tip,
                                belirtmetip = EXCLUDED.belirtmetip,
                                durum = EXCLUDED.durum,
                                sistemkayittarihi = EXCLUDED.sistemkayittarihi,
                                onaydurum = EXCLUDED.onaydurum,
                                kadastroalan = EXCLUDED.kadastroalan,
                                tapucinsid = EXCLUDED.tapucinsid,
                                sistemguncellemetarihi = EXCLUDED.sistemguncellemetarihi,
                                kmdurum = EXCLUDED.kmdurum,
                                hazineparseldurum = EXCLUDED.hazineparseldurum,
                                terksebep = EXCLUDED.terksebep,
                                detayuretimyontem = EXCLUDED.detayuretimyontem,
                                orjinalgeomwkt = EXCLUDED.orjinalgeomwkt,
                                orjinalgeomkoordinatsistem = EXCLUDED.orjinalgeomkoordinatsistem,
                                orjinalgeomuretimyontem = EXCLUDED.orjinalgeomuretimyontem,
                                dom = EXCLUDED.dom,
                                epok = EXCLUDED.epok,
                                detayverikalite = EXCLUDED.detayverikalite,
                                orjinalgeomepok = EXCLUDED.orjinalgeomepok,
                                parseltescildurum = EXCLUDED.parseltescildurum,
                                olcuyontem = EXCLUDED.olcuyontem,
                                detayarsivonaylikoordinat = EXCLUDED.detayarsivonaylikoordinat,
                                detaypaftazeminuyumluluk = EXCLUDED.detaypaftazeminuyumluluk,
                                tesisislemfenkayitref = EXCLUDED.tesisislemfenkayitref,
                                terkinislemfenkayitref = EXCLUDED.terkinislemfenkayitref,
                                yanilmasiniri = EXCLUDED.yanilmasiniri,
                                hesapverikalite = EXCLUDED.hesapverikalite,
                                updated_at = CURRENT_TIMESTAMP,
                                geom = EXCLUDED.geom
                            """, (
                                feature.get('fid'),
                                feature.get('parselno'),
                                feature.get('adano'),
                                feature.get('tapukimlikno'),
                                feature.get('tapucinsaciklama'),
                                feature.get('tapuzeminref'),
                                feature.get('tapumahalleref'),
                                feature.get('tapualan'),
                                feature.get('tip'),
                                feature.get('belirtmetip'),
                                feature.get('durum'),
                                feature.get('sistemkayittarihi'),
                                feature.get('onaydurum'),
                                feature.get('kadastroalan'),
                                feature.get('tapucinsid'),
                                feature.get('sistemguncellemetarihi'),
                                feature.get('kmdurum'),
                                feature.get('hazineparseldurum'),
                                feature.get('terksebep'),
                                feature.get('detayuretimyontem'),
                                feature.get('orjinalgeomwkt'),
                                feature.get('orjinalgeomkoordinatsistem'),
                                feature.get('orjinalgeomuretimyontem'),
                                feature.get('dom'),
                                feature.get('epok'),
                                feature.get('detayverikalite'),
                                feature.get('orjinalgeomepok'),
                                feature.get('parseltescildurum'),
                                feature.get('olcuyontem'),
                                feature.get('detayarsivonaylikoordinat'),
                                feature.get('detaypaftazeminuyumluluk'),
                                feature.get('tesisislemfenkayitref'),
                                feature.get('terkinislemfenkayitref'),
                                feature.get('yanilmasiniri'),
                                feature.get('hesapverikalite'),
                                geom
                            ))

                            conn.commit()
                            cursor.close()
                            saved_count += 1
                        
                except Exception as e:
                    logger.error(f"Parsel kaydedilirken hata: {e}")
                    import traceback
                    logger.error(f"Hata detayı: {traceback.format_exc()}")
                    logger.debug(f"Hatalı parsel fid: {feature.get('fid', 'N/A')}, tapuzeminref: {feature.get('tapuzeminref', 'N/A')}")
                    error_count += 1
                    # Tek bir parsel hatası tüm işlemi durdurmaz, devam et
                    continue
                    
            except Exception as e:
                logger.error(f"Parsel işlenirken hata: {e}")
                error_count += 1
                continue
        
        logger.info(f"{saved_count} parsel başarıyla veritabanına kaydedildi")
        
        if skipped_count > 0:
            logger.warning(f"{skipped_count} parsel atlandı (eksik veri)")
        if error_count > 0:
            logger.warning(f"{error_count} parsel hata nedeniyle kaydedilemedi")
        
        logger.info(f"Toplam işlenen: {len(features)}, Kaydedilen: {saved_count}, Atlanan: {skipped_count}, Hatalı: {error_count}")

        return saved_count


    def get_last_setting(self) -> Optional[Dict[str, Any]]:
        """tk_settings tablosundan son kaydı getir"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT id, query_date, start_index, created_at, updated_at
                        FROM tk_settings 
                        ORDER BY id DESC 
                        LIMIT 1
                    """)
                    
                    result = cursor.fetchone()
                    
                    if result:
                        # RealDictRow kullanıldığı için sütun adlarıyla erişim yapıyoruz
                        return {
                            'id': result['id'],
                            'query_date': result['query_date'],
                            'start_index': result['start_index'],
                            'created_at': result['created_at'],
                            'updated_at': result['updated_at']
                        }
                    else:
                        logger.info("tk_settings tablosunda kayıt bulunamadı")
                        return None
                        
        except Exception as e:
            logger.error(f"Son ayar kaydı getirilirken hata: {e}")
            return None


    def update_setting(self, setting_id: int, **kwargs) -> bool:
        """tk_settings tablosundaki belirli bir kaydı güncelle
        
        Args:
            setting_id: Güncellenecek kaydın ID'si
            **kwargs: Güncellenecek alanlar (query_date, start_index)
            
        Returns:
            bool: Güncelleme başarılı ise True, değilse False
        """
        if not kwargs:
            logger.warning("Güncelleme için hiç alan belirtilmedi")
            return False
            
        # Güncellenebilir alanları kontrol et
        allowed_fields = {'query_date', 'start_index'}
        update_fields = {k: v for k, v in kwargs.items() if k in allowed_fields}
        
        if not update_fields:
            logger.warning("Geçerli güncelleme alanı bulunamadı")
            return False
            
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Dinamik UPDATE sorgusu oluştur
                    set_clauses = []
                    values = []
                    
                    for field, value in update_fields.items():
                        set_clauses.append(f"{field} = %s")
                        values.append(value)
                    
                    # updated_at alanını otomatik güncelle
                    set_clauses.append("updated_at = CURRENT_TIMESTAMP")
                    values.append(setting_id)
                    
                    query = f"""
                        UPDATE tk_settings 
                        SET {', '.join(set_clauses)}
                        WHERE id = %s
                    """
                    
                    cursor.execute(query, values)
                    
                    if cursor.rowcount > 0:
                        conn.commit()
                        logger.info(f"Ayar kaydı (ID: {setting_id}) başarıyla güncellendi")
                        return True
                    else:
                        logger.warning(f"Güncellenecek kayıt bulunamadı (ID: {setting_id})")
                        return False
                        
        except Exception as e:
            logger.error(f"Ayar kaydı güncellenirken hata: {e}")
            return False

    def get_statistics(self) -> Dict[str, Any]:
        """Veritabanı istatistiklerini getir"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    stats = {}
                    
                    # Parsel istatistikleri
                    cursor.execute("SELECT COUNT(*) as count FROM tk_parseller")
                    result = cursor.fetchone()
                    stats['total_parcels'] = result['count'] if result else 0
                    
                    cursor.execute("""
                        SELECT COUNT(*) as count FROM tk_parseller 
                        WHERE created_at >= CURRENT_DATE
                    """)
                    result = cursor.fetchone()
                    stats['parcels_today'] = result['count'] if result else 0
                    
                    cursor.execute("""
                        SELECT COUNT(*) as count FROM tk_parseller 
                        WHERE created_at >= CURRENT_DATE - INTERVAL '7 days'
                    """)
                    result = cursor.fetchone()
                    stats['parcels_last_week'] = result['count'] if result else 0
                    
                    cursor.execute("""
                        SELECT COALESCE(SUM(tapualan), 0) as total_area FROM tk_parseller 
                        WHERE tapualan IS NOT NULL
                    """)
                    result = cursor.fetchone()
                    stats['total_area'] = float(result['total_area']) if result and result['total_area'] else 0.0
                    
                    cursor.execute("""
                        SELECT MIN(sistemkayittarihi) as min_date, MAX(sistemkayittarihi) as max_date 
                        FROM tk_parseller 
                        WHERE sistemkayittarihi IS NOT NULL
                    """)
                    date_range = cursor.fetchone()
                    stats['date_range'] = {
                        'min_date': date_range['min_date'].strftime('%Y-%m-%d') if date_range and date_range['min_date'] else None,
                        'max_date': date_range['max_date'].strftime('%Y-%m-%d') if date_range and date_range['max_date'] else None
                    }
                    
                    # İlçe istatistikleri
                    cursor.execute("SELECT COUNT(*) as count FROM tk_ilceler")
                    result = cursor.fetchone()
                    stats['total_districts'] = result['count'] if result else 0
                    
                    # Mahalle istatistikleri
                    cursor.execute("SELECT COUNT(*) as count FROM tk_mahalleler")
                    result = cursor.fetchone()
                    stats['total_neighbourhoods'] = result['count'] if result else 0
                    
                    # Log istatistikleri
                    cursor.execute("SELECT COUNT(*) as count FROM tk_logs")
                    result = cursor.fetchone()
                    stats['total_queries'] = result['count'] if result else 0
                    
                    cursor.execute("""
                        SELECT COUNT(*) as count FROM tk_logs 
                        WHERE query_time >= CURRENT_DATE
                    """)
                    result = cursor.fetchone()
                    stats['queries_today'] = result['count'] if result else 0
                    
                    cursor.execute("""
                        SELECT COALESCE(AVG(feature_count), 0) as avg_features FROM tk_logs 
                        WHERE feature_count > 0
                    """)
                    result = cursor.fetchone()
                    stats['avg_features_per_query'] = float(result['avg_features']) if result and result['avg_features'] else 0.0
                    
                    # En son güncelleme tarihi
                    cursor.execute("""
                        SELECT MAX(updated_at) as last_update FROM tk_parseller
                    """)
                    last_update = cursor.fetchone()
                    stats['last_update'] = last_update['last_update'].strftime('%Y-%m-%d %H:%M:%S') if last_update and last_update['last_update'] else None
                    
                    # Ayar bilgileri
                    cursor.execute("""
                        SELECT query_date, start_index, updated_at 
                        FROM tk_settings 
                        ORDER BY updated_at DESC 
                        LIMIT 1
                    """)
                    setting = cursor.fetchone()
                    if setting:
                        stats['current_settings'] = {
                            'query_date': setting['query_date'].strftime('%Y-%m-%d') if setting['query_date'] else None,
                            'start_index': setting['start_index'] or 0,
                            'last_updated': setting['updated_at'].strftime('%Y-%m-%d %H:%M:%S') if setting['updated_at'] else None
                        }
                    else:
                        stats['current_settings'] = {
                            'query_date': None,
                            'start_index': 0,
                            'last_updated': None
                        }
                    
                    logger.info(f"İstatistikler başarıyla alındı: {len(stats)} adet")
                    return stats
                    
        except Exception as e:
            logger.error(f"İstatistik verilerini alırken hata: {e}")
            logger.error(f"Hata türü: {type(e).__name__}")
            import traceback
            logger.error(f"Hata detayı: {traceback.format_exc()}")
            return {}
