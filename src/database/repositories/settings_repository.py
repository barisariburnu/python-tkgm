"""Settings Repository

Ayarların database işlemleri.
"""

from typing import Any, Dict, Optional
from loguru import logger
from .base_repository import BaseRepository


class SettingsRepository(BaseRepository):
    """Ayarlar repository"""
    
    def get_last_setting(self, scrape_type: bool = False) -> Optional[Dict[str, Any]]:
        """tk_settings tablosundan son kaydı getir"""
        try:
            with self.db.get_connection() as conn:
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
            
        try:
            with self.db.get_connection() as conn:
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
