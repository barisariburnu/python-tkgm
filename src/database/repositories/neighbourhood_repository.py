"""Neighbourhood Repository

Mahalle verilerinin database işlemleri.
"""

from typing import Any, Dict, List
from loguru import logger
from .base_repository import BaseRepository
from ..logging_utils import BatchLogger


class NeighbourhoodRepository(BaseRepository):
    """Mahalle repository - OPTIMIZED with single transaction"""
    
    def insert_neighbourhoods(self, features: List[Dict[str, Any]]) -> int:
        """Mahalle verilerini veritabanına kaydet - OPTIMIZED"""
        if not features:
            logger.warning("Kayıt yapılacak mahalle verisi bulunamadı")
            return 0

        saved_count = 0
        skipped_count = 0
        error_count = 0
        
        batch_logger = BatchLogger("Inserting neighbourhoods", total=len(features), interval=50)
        
        conn = None
        cursor = None
        try:
            conn = self.db.get_connection()
            cursor = conn.cursor()
            
            for feature in features:
                geom = None

                try:
                    if 'fid' not in feature or not feature['fid']:
                        logger.debug("Mahalle fid değeri eksik, atlanıyor")
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
                        INSERT INTO tk_mahalle (
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
                        saved_count += 1
                        batch_logger.log_progress(saved_count)
                        
                    except Exception as e:
                        logger.error(f"Mahalle kaydedilirken hata: {e}")
                        logger.debug(f"Hatalı mahalle fid: {feature.get('fid', 'N/A')}, tapumahallead: {feature.get('tapumahallead', 'N/A')}")
                        error_count += 1
                        continue
                        
                except Exception as e:
                    logger.debug(f"Mahalle işlenirken hata: {e}")
                    error_count += 1
                    continue
            
            if conn:
                conn.commit()
            
            batch_logger.finalize(
                success_count=saved_count,
                error_count=error_count,
                skip_count=skipped_count
            )

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
    
    def get_neighbourhoods(self) -> List[Dict[str, Any]]:
        """Tüm mahalleleri tapukimlikno ile birlikte getir"""
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT tapukimlikno, tapumahallead, kadastromahallead, ilceref
                        FROM tk_mahalle 
                        WHERE tapukimlikno IS NOT NULL
                        ORDER BY tapukimlikno
                    """)
                    
                    neighbourhoods = []
                    for row in cursor.fetchall():
                        neighbourhoods.append({
                            'tapukimlikno': row['tapukimlikno'],
                            'tapumahallead': row['tapumahallead'],
                            'kadastromahallead': row['kadastromahallead'],
                            'ilceref': row['ilceref']
                        })
                    
                    logger.info(f"{len(neighbourhoods)} mahalle bilgisi alındı")
                    return neighbourhoods
                    
        except Exception as e:
            logger.error(f"Mahalle bilgileri alınırken hata: {e}")
            return []
