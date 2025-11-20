"""
Repository package

Tüm repository'leri buradan export ediyoruz.
"""

from .base_repository import BaseRepository
from .parcel_repository import ParcelRepository

__all__ = [
    'BaseRepository',
    'ParcelRepository',
]
