"""Base Repository Pattern

Tüm repository'ler için base class.
"""

from typing import Any, Dict, List, Optional
from loguru import logger
from ..connection import DatabaseConnection


class BaseRepository:
    """Base repository - ortak fonks

iyonlar"""
    
    def __init__(self, db_connection: DatabaseConnection):
        self.db = db_connection
    
    def _execute_query(self, query: str, params: tuple = None) -> Optional[List[Dict[str, Any]]]:
        """Query çalıştır ve sonuç dön"""
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, params)
                    return cursor.fetchall()
        except Exception as e:
            logger.error(f"Query execution error: {e}")
            return None
    
    def _execute_insert(self, query: str, params: tuple) -> bool:
        """Insert/Update query çalıştır"""
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, params)
                    conn.commit()
                    return True
        except Exception as e:
            logger.error(f"Insert/Update error: {e}")
            return False
