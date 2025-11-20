"""District Repository

İlçe verilerinin database işlemleri.
"""

from typing import Any, Dict, List
from loguru import logger
from .base_repository import BaseRepository


class DistrictRepository(BaseRepository):
    """İlçe repository - OPTIMIZED with single transaction"""
    
    def insert_districts(self, features: List[Dict[str, Any]]) -> int:
        """İlçe verilerini veritabanına kaydet - OPTIMIZED"""
        if not features:
            logger.warning("Kayıt yapılacak ilçe verisi bulunamadı")
            return 0

        saved_count = 0
        skipped_count = 0
        error_count = 0
        
        conn = None
        cursor = None
        try:
            conn = self.db.get_connection()
            cursor = conn.cursor()
            
            for feature in features:
                geom = None

                try:
                    if 'fid' not in feature or not feature['fid']:
                        logger.debug("İlçe fid değeri eksik, atlanıyor")
                        skipped_count += 1
                        continue

                    try:
                        if 'wkt' in feature and isinstance(feature['wkt'], str):
                            geom = feature.get('wkt')
                            if not geom:
                                raise ValueError("Geçerli geometri verileri bulunamadı")
                    except Exception as e:
                        logger.debug(f"Geometri oluşturulurken hata: {e}")
                        skipped_count += 1
                        continue

                    try:
                        cursor.execute("""
                        INSERT INTO tk_ilce (fid, tapukimlikno, ilref, ad, durum, geom)
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
                        saved_count += 1
                        
                        if saved_count % 50 == 0:
                            logger.info(f"Progress: {saved_count}/{len(features)} districts processed")
                        
                    except Exception as e:
                        logger.error(f"İlçe kaydedilirken hata: {e}")
                        logger.debug(f"Hatalı ilçe fid: {feature.get('fid', 'N/A')}, ad: {feature.get('ad', 'N/A')}")
                        error_count += 1
                        continue
                        
                except Exception as e:
                    logger.debug(f"İlçe işlenirken hata: {e}")
                    error_count += 1
                    continue
            
            if conn:
                conn.commit()
                logger.info(f"✅ {saved_count} ilçe başarıyla commit edildi")
            
            if skipped_count > 0:
                logger.warning(f"{skipped_count} ilçe atlandı (eksik veri)")
            if error_count > 0:
                logger.warning(f"{error_count} ilçe hata nedeniyle kaydedilemedi")
            
            logger.info(f"Toplam işlenen: {len(features)}, Kaydedilen: {saved_count}, Atlanan: {skipped_count}, Hatalı: {error_count}")

        except Exception as e:
            logger.error(f"Toplu insert sırasında kritik hata: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

        return saved_count
