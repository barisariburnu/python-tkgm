"""
Data Models - Type-safe Feature Classes

Dataclasses for TKGM features (Parcel, District, Neighbourhood).
Provides type safety, validation, and easy serialization.
"""

from dataclasses import dataclass, asdict, field
from typing import Optional
from datetime import datetime


@dataclass
class ParcelFeature:
    """
    TKGM Parsel (Parcel) Feature Model
    
    Type-safe dataclass for parcel data from TKGM WFS service.
    """
    # Primary identifiers
    fid: Optional[int] = None
    tapukimlikno: Optional[int] = None
    
    # Basic info
    parselno: Optional[int] = None
    adano: Optional[int] = None
    tapucinsaciklama: Optional[str] = None
    tapuzeminref: Optional[int] = None
    tapumahalleref: Optional[int] = None
    
    # Area and type
    tapualan: Optional[float] = None
    kadastroalan: Optional[float] = None
    tip: Optional[str] = None
    belirtmetip: Optional[str] = None
    durum: Optional[str] = None
    
    # Status and dates
    sistemkayittarihi: Optional[str] = None  # datetime as string from service
    sistemguncellemetarihi: Optional[str] = None
    onaydurum: Optional[int] = None
    
    # IDs
    tapucinsid: Optional[int] = None
    
    # Status fields
    kmdurum: Optional[str] = None
    hazineparseldurum: Optional[str] = None
    terksebep: Optional[str] = None
    parseltescildurum: Optional[str] = None
    
    # Geometry and quality
    orjinalgeomwkt: Optional[str] = None
    orjinalgeomkoordinatsistem: Optional[str] = None
    orjinalgeomuretimyontem: Optional[str] = None
    orjinalgeomepok: Optional[str] = None
    
    detayuretimyontem: Optional[str] = None
    detayverikalite: Optional[str] = None
    detayarsivonaylikoordinat: Optional[str] = None
    detaypaftazeminuyumluluk: Optional[str] = None
    
    dom: Optional[str] = None
    epok: Optional[str] = None
    olcuyontem: Optional[str] = None
    
    # References
    tesisislemfenkayitref: Optional[str] = None
    terkinislemfenkayitref: Optional[str] = None
    
    # Quality and calculation
    yanilmasiniri: Optional[float] = None
    hesapverikalite: Optional[str] = None
    
    # Geometry (WKT from processor)
    wkt: Optional[str] = None
    
    @classmethod
    def from_dict(cls, data: dict) -> 'ParcelFeature':
        """
        Create ParcelFeature from dictionary
        
        Args:
            data: Dictionary with parcel data
            
        Returns:
            ParcelFeature instance
        """
        # Filter only valid fields
        valid_fields = {k: v for k, v in data.items() if k in cls.__dataclass_fields__}
        return cls(**valid_fields)
    
    def to_dict(self) -> dict:
        """
        Convert ParcelFeature to dictionary
        
        Returns:
            Dictionary representation
        """
        return asdict(self)


@dataclass
class DistrictFeature:
    """
    TKGM İlçe (District) Feature Model
    
    Type-safe dataclass for district data from TKGM WFS service.
    """
    # Primary identifiers
    fid: Optional[int] = None
    tapukimlikno: Optional[int] = None
    
    # Basic info
    ilref: Optional[int] = None
    ad: Optional[str] = None
    durum: Optional[int] = None
    
    # Geometry (WKT from processor)
    wkt: Optional[str] = None
    
    @classmethod
    def from_dict(cls, data: dict) -> 'DistrictFeature':
        """Create DistrictFeature from dictionary"""
        valid_fields = {k: v for k, v in data.items() 
                       if k in cls.__dataclass_fields__}
        return cls(**valid_fields)
    
    def to_dict(self) -> dict:
        """Convert to dictionary"""
        return asdict(self)


@dataclass
class NeighbourhoodFeature:
    """
    TKGM Mahalle (Neighbourhood) Feature Model
    
    Type-safe dataclass for neighbourhood data from TKGM WFS service.
    """
    # Primary identifiers
    fid: Optional[int] = None
    tapukimlikno: Optional[int] = None
    
    # Basic info
    ilceref: Optional[int] = None
    durum: Optional[int] = None
    tip: Optional[int] = None
    
    # Names
    tapumahallead: Optional[str] = None
    kadastromahallead: Optional[str] = None
    
    # Dates
    sistemkayittarihi: Optional[str] = None
    
    # Geometry (WKT from processor)
    wkt: Optional[str] = None
    
    @classmethod
    def from_dict(cls, data: dict) -> 'NeighbourhoodFeature':
        """Create NeighbourhoodFeature from dictionary"""
        valid_fields = {k: v for k, v in data.items() 
                       if k in cls.__dataclass_fields__}
        return cls(**valid_fields)
    
    def to_dict(self) -> dict:
        """Convert to dictionary"""
        return asdict(self)


# Example usage:
"""
from src.models import ParcelFeature

# Create from dict
parcel = ParcelFeature.from_dict({
    'fid': 123,
    'tapukimlikno': 456,
    'parselno': 789,
    'tapualan': 1500.5,
    'wkt': 'MULTIPOLYGON(...)'
})

# Access with type safety
print(parcel.fid)  # IDE autocomplete works!
print(parcel.tapualan)

# Convert back to dict
data = parcel.to_dict()

# Use in repository (backward compatible)
features = [parcel.to_dict() for parcel in parcels]
db.insert_parcels(features)
"""
