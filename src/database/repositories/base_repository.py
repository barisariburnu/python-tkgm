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
        conn = None
        try:
            conn = self.db.get_connection()
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                return cursor.fetchall()
        except Exception as e:
            logger.error(f"Query execution error: {e}")
            return None
        finally:
            if conn:
                self.db.return_connection(conn)
    
    def _execute_insert(self, query: str, params: tuple) -> bool:
        """Insert/Update query çalıştır"""
        conn = None
        try:
            conn = self.db.get_connection()
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"Insert/Update error: {e}")
            return False
        finally:
            if conn:
                self.db.return_connection(conn)
