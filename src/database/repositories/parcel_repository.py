"""Parcel Repository

Parsel verilerinin database işlemleri.
"""

from typing import Any, Dict, List
from loguru import logger
from .base_repository import BaseRepository


class ParcelRepository(BaseRepository):
    """Parsel repository - OPTIMIZED with single transaction"""
    
    def insert_parcels(self, features: List[Dict[str, Any]]) -> int:
        """Parsel verilerini veritabanına kaydet - OPTIMIZED: Single transaction"""
        if not features:
            logger.warning("Kayıt yapılacak parsel verisi bulunamadı")
            return 0

        saved_count = 0
        skipped_count = 0
        error_count = 0
        
        # OPTIMIZATION: Single connection for all inserts
        conn = None
        cursor = None
        try:
            conn = self.db.get_connection()
            cursor = conn.cursor()
            
            for feature in features:
                geom = None

                try:
                    # Gerekli alanları kontrol et
                    if 'fid' not in feature or not feature['fid']:
                        logger.debug("Parsel fid değeri eksik, atlanıyor")
                        skipped_count += 1
                        continue

                    # Geometri verilerini oluştur
                    try:
                        if 'wkt' in feature and isinstance(feature['wkt'], str):
                            geom = feature.get('wkt')
                            
                            if not geom:
                                raise ValueError("Geçerli geometri verileri bulunamadı")
                    except Exception as e:
                        logger.debug(f"Geometri oluşturulurken hata: {e}")
                        skipped_count += 1
                        continue

                    # Insert with single transaction
                    try:
                        cursor.execute("""
                        INSERT INTO tk_parsel (
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
                            feature.get('fid'), feature.get('parselno'), feature.get('adano'),
                            feature.get('tapukimlikno'), feature.get('tapucinsaciklama'),
                            feature.get('tapuzeminref'), feature.get('tapumahalleref'),
                            feature.get('tapualan'), feature.get('tip'), feature.get('belirtmetip'),
                            feature.get('durum'), feature.get('sistemkayittarihi'),
                            feature.get('onaydurum'), feature.get('kadastroalan'),
                            feature.get('tapucinsid'), feature.get('sistemguncellemetarihi'),
                            feature.get('kmdurum'), feature.get('hazineparseldurum'),
                            feature.get('terksebep'), feature.get('detayuretimyontem'),
                            feature.get('orjinalgeomwkt'), feature.get('orjinalgeomkoordinatsistem'),
                            feature.get('orjinalgeomuretimyontem'), feature.get('dom'),
                            feature.get('epok'), feature.get('detayverikalite'),
                            feature.get('orjinalgeomepok'), feature.get('parseltescildurum'),
                            feature.get('olcuyontem'), feature.get('detayarsivonaylikoordinat'),
                            feature.get('detaypaftazeminuyumluluk'),
                            feature.get('tesisislemfenkayitref'),
                            feature.get('terkinislemfenkayitref'),
                            feature.get('yanilmasiniri'), feature.get('hesapverikalite'),
                            geom
                        ))
                        saved_count += 1
                        
                        # Log progress every 100 records
                        if saved_count % 100 == 0:
                            logger.info(f"Progress: {saved_count}/{len(features)} parcels inserted")
                        
                    except Exception as e:
                        logger.error(f"Parsel kaydedilirken hata: {e}")
                        logger.debug(f"Hatalı parsel fid: {feature.get('fid', 'N/A')}")
                        error_count += 1
                        continue
                        
                except Exception as e:
                    logger.debug(f"Parsel işlenirken hata: {e}")
                    error_count += 1
                    continue
            
            # OPTIMIZATION: Single commit for all inserts
            if conn:
                conn.commit()
                logger.info(f"✅ {saved_count} parsel başarıyla commit edildi")
            
            if skipped_count > 0:
                logger.warning(f"{skipped_count} parsel atlandı (eksik veri)")
            if error_count > 0:
                logger.warning(f"{error_count} parsel hata nedeniyle kaydedilemedi")
            
            logger.info(f"Toplam işlenen: {len(features)}, Kaydedilen: {saved_count}, Atlanan: {skipped_count}, Hatalı: {error_count}")

        except Exception as e:
            logger.error(f"Toplu insert sırasında kritik hata: {e}")
            if conn:
                conn.rollback()
                logger.warning("Transaction rollback yapıldı")
            raise
        finally:
            # Cleanup
            if cursor:
                cursor.close()
            if conn:
                conn.close()

        return saved_count
