"""Log Repository

Log kayıtlarının database işlemleri.
"""

from loguru import logger
from .base_repository import BaseRepository


class LogRepository(BaseRepository):
    """Log repository"""
    
    def insert_log(self, typename: str, url: str, feature_count: int = 0,
                   is_empty: bool = False, is_successful: bool = False,
                   error_message: str = None, http_status_code: int = None,
                   response_xml: str = None, response_size: int = None,
                   execution_duration: float = None, notes: str = None) -> bool:
        """TKGM servis sorgusunu tk_logs tablosuna kaydet"""
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO tk_logs (
                            typename, url, feature_count, is_empty, is_successful,
                            error_message, http_status_code, response_xml, response_size,
                            execution_duration, notes, query_time
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP
                        )
                    """, (
                        typename,
                        url,
                        feature_count,
                        is_empty,
                        is_successful,
                        error_message,
                        http_status_code,
                        response_xml,
                        response_size,
                        execution_duration,
                        notes
                    ))
                    
                    conn.commit()
                    return True
                    
        except Exception as e:
            logger.error(f"Log kaydı eklenirken hata: {e}")
            return False
