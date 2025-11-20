"""Failed Records Repository

Başarısız kayıtların yönetimi - veri kaybını önle!
"""

import json
import traceback
from typing import Any, Dict, List, Optional
from datetime import datetime
from loguru import logger
from .base_repository import BaseRepository


class FailedRecordsRepository(BaseRepository):
    """
    Başarısız kayıtları takip et ve yönet
    
    Günlük 10k kayıt limiti olduğu için servisten çekilen her veri değerli!
    """
    
    def insert_failed_record(
        self,
        entity_type: str,
        raw_data: Dict[str, Any],
        error: Exception,
        entity_id: Optional[str] = None
    ) -> bool:
        """
        Başarısız kaydı veritabanına kaydet
        
        Args:
            entity_type: 'parcel', 'district', 'neighbourhood'
            raw_data: Orijinal feature data (dict)
            error: Exception instance
            entity_id: fid, tapukimlikno vs.
        """
        try:
            # Error type classification
            error_type = type(error).__name__
            error_message = str(error)
            stack_trace_str = traceback.format_exc()
            
            with self.db.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO tk_failed_records (
                            entity_type, entity_id, raw_data,
                            error_type, error_message, stack_trace,
                            status
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (entity_type, entity_id, status) DO NOTHING
                    """, (
                        entity_type,
                        entity_id or raw_data.get('fid', 'unknown'),
                        json.dumps(raw_data),  # JSON olarak sakla
                        error_type,
                        error_message,
                        stack_trace_str,
                        'failed'
                    ))
                    
                    conn.commit()
                    logger.warning(
                        f"Failed record saved: {entity_type} - {entity_id} - {error_type}"
                    )
                    return True
                    
        except Exception as e:
            logger.error(f"Failed record'u bile kaydedemedik! {e}")
            # Critical! Log to file at least
            logger.critical(f"LOST DATA: {entity_type} - {entity_id} - {raw_data}")
            return False
    
    def get_failed_records(
        self,
        entity_type: Optional[str] = None,
        status: str = 'failed',
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Başarısız kayıtları getir (retry için)
        """
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cursor:
                    if entity_type:
                        cursor.execute("""
                            SELECT id, entity_type, entity_id, raw_data, 
                                   error_type, error_message, retry_count
                            FROM tk_failed_records
                            WHERE entity_type = %s AND status = %s
                            ORDER BY created_at ASC
                            LIMIT %s
                        """, (entity_type, status, limit))
                    else:
                        cursor.execute("""
                            SELECT id, entity_type, entity_id, raw_data,
                                   error_type, error_message, retry_count
                            FROM tk_failed_records
                            WHERE status = %s
                            ORDER BY created_at ASC
                            LIMIT %s
                        """, (status, limit))
                    
                    results = []
                    for row in cursor.fetchall():
                        results.append({
                            'id': row['id'],
                            'entity_type': row['entity_type'],
                            'entity_id': row['entity_id'],
                            'raw_data': row['raw_data'],  # Already dict from JSONB
                            'error_type': row['error_type'],
                            'error_message': row['error_message'],
                            'retry_count': row['retry_count']
                        })
                    
                    return results
                    
        except Exception as e:
            logger.error(f"Failed records getirilemedi: {e}")
            return []
    
    def mark_as_resolved(self, record_id: int) -> bool:
        """Başarıyla retry edildi, resolved olarak işaretle"""
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        UPDATE tk_failed_records
                        SET status = 'resolved',
                            resolved_at = CURRENT_TIMESTAMP,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                    """, (record_id,))
                    
                    conn.commit()
                    return True
        except Exception as e:
            logger.error(f"Mark as resolved failed: {e}")
            return False
    
    def increment_retry_count(self, record_id: int) -> bool:
        """Retry count'u artır"""
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        UPDATE tk_failed_records
                        SET retry_count = retry_count + 1,
                            last_retry_at = CURRENT_TIMESTAMP,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                    """, (record_id,))
                    
                    conn.commit()
                    return True
        except Exception as e:
            logger.error(f"Increment retry count failed: {e}")
            return False
    
    def get_statistics(self) -> Dict[str, Any]:
        """Failed records istatistikleri"""
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Toplam failed records
                    cursor.execute("SELECT COUNT(*) as count FROM tk_failed_records WHERE status = 'failed'")
                    total_failed = cursor.fetchone()['count']
                    
                    # Entity type'a göre dağılım
                    cursor.execute("""
                        SELECT entity_type, COUNT(*) as count
                        FROM tk_failed_records
                        WHERE status = 'failed'
                        GROUP BY entity_type
                    """)
                    by_type = {row['entity_type']: row['count'] for row in cursor.fetchall()}
                    
                    # Bugünkü failed count
                    cursor.execute("""
                        SELECT COUNT(*) as count
                        FROM tk_failed_records
                        WHERE status = 'failed' AND created_at >= CURRENT_DATE
                    """)
                    today_failed = cursor.fetchone()['count']
                    
                    return {
                        'total_failed': total_failed,
                        'today_failed': today_failed,
                        'by_type': by_type
                    }
        except Exception as e:
            logger.error(f"Failed records stats error: {e}")
            return {}
