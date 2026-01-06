"""Settings Repository

Ayarların database işlemleri.
"""

from typing import Any, Dict, Optional
from datetime import datetime, date
from loguru import logger
from .base_repository import BaseRepository


class SettingsRepository(BaseRepository):
    """Ayarlar repository"""
    
    # Scrape Types
    TYPE_DAILY_SYNC = "daily_sync"
    TYPE_FULLY_SYNC = "fully_sync"
    TYPE_DAILY_INACTIVE_SYNC = "daily_inactive_sync"
    TYPE_DAILY_LIMIT_REACHED = "daily_limit_reached"
    
    def get_last_setting(self, scrape_type: str = TYPE_DAILY_SYNC) -> Optional[Dict[str, Any]]:
        """tk_settings tablosundan son kaydı getir"""
        conn = None
        try:
            conn = self.db.get_connection()
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT id, query_date, start_index, scrape_type, created_at, updated_at
                    FROM tk_settings 
                    WHERE scrape_type = %s
                    ORDER BY id DESC 
                    LIMIT 1
                """, (scrape_type,))
                
                result = cursor.fetchone()
                
                if result:
                    return {
                        'id': result['id'],
                        'query_date': result['query_date'],
                        'start_index': result['start_index'],
                        'scrape_type': result['scrape_type'],
                        'created_at': result['created_at'],
                        'updated_at': result['updated_at']
                    }
                else:
                    logger.info("tk_settings tablosunda kayıt bulunamadı")
                    return {}
                    
        except Exception as e:
            logger.error(f"Son ayar kaydı getirilirken hata: {e}")
            return {}
        finally:
            if conn:
                self.db.return_connection(conn)
    
    def update_setting(self, **kwargs) -> bool:
        """tk_settings tablosuna kayıt ekle veya güncelle (UPSERT)"""
        if not kwargs:
            logger.warning("Güncelleme için hiç alan belirtilmedi")
            return False
            
        required_fields = {'scrape_type'}
        if not all(field in kwargs for field in required_fields):
            logger.warning(f"Gerekli alanlar eksik: {required_fields}")
            return False
            
        allowed_fields = {'query_date', 'start_index', 'scrape_type'}
        update_fields = {k: v for k, v in kwargs.items() if k in allowed_fields}
        
        if not update_fields:
            logger.warning("Geçerli güncelleme alanı bulunamadı")
            return False
        
        conn = None
        try:
            conn = self.db.get_connection()
            with conn.cursor() as cursor:
                insert_fields = list(update_fields.keys())
                insert_values = list(update_fields.values())
                
                update_clauses = []
                for field in insert_fields:
                    if field not in ['scrape_type']:
                        update_clauses.append(f"{field} = EXCLUDED.{field}")
                
                update_clauses.append("updated_at = CURRENT_TIMESTAMP")
                
                query = f"""
                    INSERT INTO tk_settings ({', '.join(insert_fields)})
                    VALUES ({', '.join(['%s'] * len(insert_values))})
                    ON CONFLICT (scrape_type)
                    DO UPDATE SET {', '.join(update_clauses)}
                """
                
                cursor.execute(query, insert_values)
                
                if cursor.rowcount > 0:
                    conn.commit()
                    logger.info(f"Ayar kaydı başarıyla eklendi/güncellendi (scrape_type: {kwargs.get('scrape_type')})")
                    return True
                else:
                    logger.warning("Hiçbir kayıt etkilenmedi")
                    return False
                    
        except Exception as e:
            logger.error(f"Ayar kaydı eklenirken/güncellenirken hata: {e}")
            return False
        finally:
            if conn:
                self.db.return_connection(conn)


    def is_daily_limit_reached(self) -> bool:
        """
        Günlük limit flag'ini kontrol et
        
        Returns:
            True if limit was reached today, False otherwise
        """
        conn = None
        try:
            conn = self.db.get_connection()
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT query_date
                    FROM tk_settings 
                    WHERE scrape_type = %s
                """, (self.TYPE_DAILY_LIMIT_REACHED,))
                
                result = cursor.fetchone()
                
                if not result:
                    return False
                
                limit_date = result['query_date']
                if isinstance(limit_date, datetime):
                    limit_date = limit_date.date()
                
                today = date.today()
                
                # Eğer limit tarih bugün ise, limit aktif
                if limit_date == today:
                    logger.warning(f"Günlük limit aktif (tarih: {limit_date})")
                    return True
                else:
                    logger.debug(f"Günlük limit geçersiz, eski tarih: {limit_date}")
                    return False
                    
        except Exception as e:
            logger.error(f"Günlük limit kontrolü sırasında hata: {e}")
            return False
        finally:
            if conn:
                self.db.return_connection(conn)


    def set_daily_limit_reached(self) -> bool:
        """
        Günlük limit flag'ini set et (bugünün tarihi ile)
        
        Returns:
            True if successful, False otherwise
        """
        try:
            today = datetime.now()
            result = self.update_setting(
                scrape_type=self.TYPE_DAILY_LIMIT_REACHED,
                query_date=today,
                start_index=0
            )
            
            if result:
                logger.warning(f"Günlük limit flag'i set edildi: {today.date()}")
            
            return result
        except Exception as e:
            logger.error(f"Günlük limit flag'i set edilirken hata: {e}")
            return False


    def clear_daily_limit(self) -> bool:
        """
        Günlük limit flag'ini temizle (manual kullanım için)
        
        Returns:
            True if successful, False otherwise
        """
        conn = None
        try:
            conn = self.db.get_connection()
            with conn.cursor() as cursor:
                cursor.execute("""
                    DELETE FROM tk_settings 
                    WHERE scrape_type = %s
                """, (self.TYPE_DAILY_LIMIT_REACHED,))
                
                conn.commit()
                logger.info("Günlük limit flag'i temizlendi")
                return True
                
        except Exception as e:
            logger.error(f"Günlük limit flag'i temizlenirken hata: {e}")
            return False
        finally:
            if conn:
                self.db.return_connection(conn)
