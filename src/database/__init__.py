"""
Database Package - Public API

Backward compatibility için eski DatabaseManager interface'ini sağlar.
"""

from .connection import DatabaseConnection
from .schema import SchemaManager
from .repositories import ParcelRepository

__all__ = [
    'DatabaseConnection',
    'SchemaManager',
    'ParcelRepository',
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
        self.parcel_repo = ParcelRepository(self.connection)
    
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
    
    # TODO: Diğer methodlar için repository'ler eklendiğinde buraya eklenecek
    # insert_districts, insert_neighbourhoods, get_statistics, vs.
