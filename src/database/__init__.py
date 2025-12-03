"""
Database Package - Public API

Backward compatibility için eski DatabaseManager interface'ini sağlar.
"""

from .connection import DatabaseConnection
from .schema import SchemaManager
from .statistics import Statistics
from .repositories import (
    ParcelRepository,
    DistrictRepository,
    NeighbourhoodRepository,
    SettingsRepository,
    LogRepository,
    FailedRecordsRepository
)

__all__ = [
    'DatabaseConnection',
    'SchemaManager',
    'Statistics',
    'ParcelRepository',
    'DistrictRepository',
    'NeighbourhoodRepository',
    'SettingsRepository',
    'LogRepository',
    'FailedRecordsRepository',
    'DatabaseManager',  # Backward compatibility
]


class DatabaseManager:
    """
    Backward compatibility wrapper.
    
    Eski kod bu class'ı kullanıyorsa çalışmaya devam etsin diye.
    Yeni kod repository pattern'i kullanmalı.
    """
    
    def __init__(self):
        self.connection = DatabaseConnection()
        self.schema = SchemaManager(self.connection)
        self.statistics = Statistics(self.connection)
        
        # Repositories
        self.parcel_repo = ParcelRepository(self.connection)
        self.district_repo = DistrictRepository(self.connection)
        self.neighbourhood_repo = NeighbourhoodRepository(self.connection)
        self.settings_repo = SettingsRepository(self.connection)
        self.log_repo = LogRepository(self.connection)
        self.failed_records_repo = FailedRecordsRepository(self.connection)  # NEW!
    
    # Connection methods
    def get_connection(self):
        return self.connection.get_connection()
    
    def test_connection(self):
        return self.connection.test_connection()
    
    def check_postgis_extension(self):
        return self.connection.check_postgis_extension()
    
    # Schema methods
    def create_tables(self):
        return self.schema.create_all_tables()
    
    # Parcel methods
    def insert_parcels(self, features):
        return self.parcel_repo.insert_parcels(features)
    
    # District methods
    def insert_districts(self, features):
        return self.district_repo.insert_districts(features)
    
    # Neighbourhood methods
    def insert_neighbourhoods(self, features):
        return self.neighbourhood_repo.insert_neighbourhoods(features)
    
    def get_neighbourhoods(self):
        return self.neighbourhood_repo.get_neighbourhoods()
    
    # Settings methods
    def get_last_setting(self, scrape_type=None):
        if scrape_type is None:
            scrape_type = SettingsRepository.TYPE_DAILY_SYNC
        return self.settings_repo.get_last_setting(scrape_type)
    
    def update_setting(self, **kwargs):
        return self.settings_repo.update_setting(**kwargs)
    
    # Daily limit methods
    def is_daily_limit_reached(self):
        """Check if daily API limit has been reached"""
        return self.settings_repo.is_daily_limit_reached()
    
    def set_daily_limit_reached(self):
        """Set the daily limit flag"""
        return self.settings_repo.set_daily_limit_reached()
    
    def clear_daily_limit(self):
        """Clear the daily limit flag (manual override)"""
        return self.settings_repo.clear_daily_limit()
    
    # Statistics methods
    def get_statistics(self):
        return self.statistics.get_statistics()
    
    # Log methods
    def insert_log(self, typename, url, feature_count=0,
                   is_empty=False, is_successful=False,
                   error_message=None, http_status_code=None,
                   response_xml=None, response_size=None,
                   execution_duration=None, notes=None):
        return self.log_repo.insert_log(
            typename, url, feature_count, is_empty, is_successful,
            error_message, http_status_code, response_xml, response_size,
            execution_duration, notes
        )

