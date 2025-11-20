"""Statistics Module

Veritabanı istatistikleri sorgular.
"""

from typing import Any, Dict
from loguru import logger
from .connection import DatabaseConnection


class Statistics:
    """İstatistik sorguları"""
    
    def __init__(self, db_connection: DatabaseConnection):
        self.db = db_connection
    
    def get_statistics(self) -> Dict[str, Any]:
        """Veritabanı istatistiklerini getir"""
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cursor:
                    stats = {}
                    
                    # Parsel istatistikleri
                    cursor.execute("SELECT COUNT(*) as count FROM tk_parsel")
                    result = cursor.fetchone()
                    stats['total_parcels'] = result['count'] if result else 0
                    
                    cursor.execute("""
                        SELECT COUNT(*) as count FROM tk_parsel 
                        WHERE created_at >= CURRENT_DATE
                    """)
                    result = cursor.fetchone()
                    stats['parcels_today'] = result['count'] if result else 0
                    
                    cursor.execute("""
                        SELECT COUNT(*) as count FROM tk_parsel 
                        WHERE created_at >= CURRENT_DATE - INTERVAL '7 days'
                    """)
                    result = cursor.fetchone()
                    stats['parcels_last_week'] = result['count'] if result else 0
                    
                    cursor.execute("""
                        SELECT COALESCE(SUM(tapualan), 0) as total_area FROM tk_parsel 
                        WHERE tapualan IS NOT NULL
                    """)
                    result = cursor.fetchone()
                    stats['total_area'] = float(result['total_area']) if result and result['total_area'] else 0.0
                    
                    cursor.execute("""
                        SELECT MIN(sistemkayittarihi) as min_date, MAX(sistemkayittarihi) as max_date 
                        FROM tk_parsel 
                        WHERE sistemkayittarihi IS NOT NULL
                    """)
                    date_range = cursor.fetchone()
                    stats['date_range'] = {
                        'min_date': date_range['min_date'].strftime('%Y-%m-%d') if date_range and date_range['min_date'] else None,
                        'max_date': date_range['max_date'].strftime('%Y-%m-%d') if date_range and date_range['max_date'] else None
                    }
                    
                    # İlçe istatistikleri
                    cursor.execute("SELECT COUNT(*) as count FROM tk_ilce")
                    result = cursor.fetchone()
                    stats['total_districts'] = result['count'] if result else 0
                    
                    # Mahalle istatistikleri
                    cursor.execute("SELECT COUNT(*) as count FROM tk_mahalle")
                    result = cursor.fetchone()
                    stats['total_neighbourhoods'] = result['count'] if result else 0
                    
                    # Log istatistikleri
                    cursor.execute("SELECT COUNT(*) as count FROM tk_logs")
                    result = cursor.fetchone()
                    stats['total_queries'] = result['count'] if result else 0
                    
                    cursor.execute("""
                        SELECT COUNT(*) as count FROM tk_logs 
                        WHERE query_time >= CURRENT_DATE
                    """)
                    result = cursor.fetchone()
                    stats['queries_today'] = result['count'] if result else 0
                    
                    cursor.execute("""
                        SELECT COALESCE(AVG(feature_count), 0) as avg_features FROM tk_logs 
                        WHERE feature_count > 0
                    """)
                    result = cursor.fetchone()
                    stats['avg_features_per_query'] = float(result['avg_features']) if result and result['avg_features'] else 0.0
                    
                    # En son güncelleme tarihi
                    cursor.execute("""
                        SELECT MAX(updated_at) as last_update FROM tk_parsel
                    """)
                    last_update = cursor.fetchone()
                    stats['last_update'] = last_update['last_update'].strftime('%Y-%m-%d %H:%M:%S') if last_update and last_update['last_update'] else None
                    
                    # Ayar bilgileri
                    cursor.execute("""
                        SELECT query_date, start_index, updated_at 
                        FROM tk_settings 
                        ORDER BY updated_at DESC 
                        LIMIT 1
                    """)
                    setting = cursor.fetchone()
                    if setting:
                        stats['current_settings'] = {
                            'query_date': setting['query_date'].strftime('%Y-%m-%d') if setting['query_date'] else None,
                            'start_index': setting['start_index'] or 0,
                            'last_updated': setting['updated_at'].strftime('%Y-%m-%d %H:%M:%S') if setting['updated_at'] else None
                        }
                    else:
                        stats['current_settings'] = {
                            'query_date': None,
                            'start_index': 0,
                            'last_updated': None
                        }
                    
                    logger.info(f"İstatistikler başarıyla alındı: {len(stats)} adet")
                    return stats
                    
        except Exception as e:
            logger.error(f"İstatistikler alınırken hata: {e}")
            return {}
