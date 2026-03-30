"""Log Repository

Log kayıtlarının database işlemleri.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional
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
            # execution_duration float (saniye) ise PostgreSQL INTERVAL tipine çevir
            duration_interval = None
            if execution_duration is not None:
                duration_interval = f'{execution_duration} seconds'

            with self.db.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO tk_logs (
                            typename, url, feature_count, is_empty, is_successful,
                            error_message, http_status_code, response_xml, response_size,
                            execution_duration, notes, query_time
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::interval, %s, CURRENT_TIMESTAMP
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
                        duration_interval,
                        notes
                    ))

                    conn.commit()
                    return True

        except Exception as e:
            logger.error(f"Log kaydı eklenirken hata: {e}")
            return False

    def search_logs_by_parcel(
        self,
        adano: int = None,
        parselno: int = None,
        tapukimlikno: int = None,
        durum: str = None,
        date_from: datetime = None,
        date_to: datetime = None,
        typename: str = None,
        successful_only: bool = True,
        limit: int = 20
    ) -> Optional[List[Dict[str, Any]]]:
        """response_xml içinde parsel bilgilerine göre log ara

        XML tag pattern'i ile kesin eşleşme yapar:
        LIKE '%<TKGM:adano>7271</TKGM:adano>%'
        """
        conditions = []
        params = []

        # Parsel attribute filtreleri - XML tag pattern
        if adano is not None:
            conditions.append("response_xml LIKE %s")
            params.append(f"%<TKGM:adano>{adano}</TKGM:adano>%")

        if parselno is not None:
            conditions.append("response_xml LIKE %s")
            params.append(f"%<TKGM:parselno>{parselno}</TKGM:parselno>%")

        if tapukimlikno is not None:
            conditions.append("response_xml LIKE %s")
            params.append(f"%<TKGM:tapukimlikno>{tapukimlikno}</TKGM:tapukimlikno>%")

        if durum is not None:
            conditions.append("response_xml LIKE %s")
            params.append(f"%<TKGM:durum>{durum}</TKGM:durum>%")

        # Metadata filtreleri
        if successful_only:
            conditions.append("is_successful = TRUE")

        if date_from is not None:
            conditions.append("query_time >= %s")
            params.append(date_from)

        if date_to is not None:
            conditions.append("query_time <= %s")
            params.append(date_to)

        if typename is not None:
            conditions.append("typename = %s")
            params.append(typename)

        where_clause = " AND ".join(conditions) if conditions else "1=1"
        params.append(limit)

        query = f"""
            SELECT id, typename, url, feature_count, is_empty, is_successful,
                   error_message, http_status_code, response_xml, response_size,
                   query_time, execution_duration, notes
            FROM tk_logs
            WHERE {where_clause}
            ORDER BY query_time DESC
            LIMIT %s
        """

        return self._execute_query(query, tuple(params))

    def get_log_by_id(self, log_id: int) -> Optional[Dict[str, Any]]:
        """Belirli bir log kaydını ID ile getir"""
        results = self._execute_query(
            "SELECT * FROM tk_logs WHERE id = %s",
            (log_id,)
        )
        if results and len(results) > 0:
            return results[0]
        return None

    def get_log_summary(
        self,
        date_from: datetime = None,
        date_to: datetime = None,
        successful_only: bool = None,
        typename: str = None,
        limit: int = 50
    ) -> Optional[List[Dict[str, Any]]]:
        """Log özet listesi (response_xml hariç, hafif sorgu)"""
        conditions = []
        params = []

        if successful_only is not None:
            conditions.append("is_successful = %s")
            params.append(successful_only)

        if date_from is not None:
            conditions.append("query_time >= %s")
            params.append(date_from)

        if date_to is not None:
            conditions.append("query_time <= %s")
            params.append(date_to)

        if typename is not None:
            conditions.append("typename = %s")
            params.append(typename)

        where_clause = " AND ".join(conditions) if conditions else "1=1"
        params.append(limit)

        query = f"""
            SELECT id, typename, url, feature_count, is_empty, is_successful,
                   error_message, http_status_code, response_size,
                   query_time, execution_duration, notes
            FROM tk_logs
            WHERE {where_clause}
            ORDER BY query_time DESC
            LIMIT %s
        """

        return self._execute_query(query, tuple(params))
