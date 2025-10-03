#!/usr/bin/env python3
"""
WFS Geometry Parser and Coordinate Transformer

This script parses WFS FeatureCollection responses, extracts geometry data,
transforms coordinates from EPSG:4326 to EPSG:2320, and converts to WKT format.

Dependencies:
    pip install shapely pyproj

Author: Geospatial Data Processing Expert
"""

from loguru import logger
from typing import List, Dict, Tuple
from xml.etree import ElementTree as ET

try:
    from shapely.geometry import Polygon, MultiPolygon, Point, LineString, MultiPoint, MultiLineString
    from shapely.wkt import dumps as wkt_dumps
    SHAPELY_AVAILABLE = True
except ImportError:
    SHAPELY_AVAILABLE = False
    logger.error("Shapely is required but not available")

try:
    from pyproj import Transformer
    PYPROJ_AVAILABLE = True
except ImportError:
    PYPROJ_AVAILABLE = False
    logger.error("PyProj is required but not available")


class WFSGeometryProcessor:
    """
    A class to process WFS FeatureCollection responses and transform geometries.
    """
    
    def __init__(self, source_crs: str = "EPSG:4326", target_crs: str = "EPSG:2320"):
        """
        Initialize the processor with source and target coordinate systems.
        """
        self.source_crs = source_crs
        self.target_crs = target_crs
        self.transformer = None
        
        # Set up logging
        logger.info("WFSGeometryProcessor başlatıldı")
        
        # Check dependencies
        self._check_dependencies()
        
        # Initialize coordinate transformer
        self._initialize_transformer()
    
    def _check_dependencies(self) -> None:
        """Check if required dependencies are available."""
        missing_deps = []        
                
        # Check dependencies
        if not SHAPELY_AVAILABLE:
            missing_deps.append("shapely")
        if not PYPROJ_AVAILABLE:
            missing_deps.append("pyproj")
            
        if missing_deps:
            raise ImportError(f"Missing required dependencies: {', '.join(missing_deps)}")
    
    
    def _initialize_transformer(self) -> None:
        """Initialize the coordinate transformer."""
        try:
            self.transformer = Transformer.from_crs(
                self.source_crs, 
                self.target_crs, 
                always_xy=True
            )
            logger.info(f"Initialized transformer from {self.source_crs} to {self.target_crs}")
        except Exception as e:
            logger.error(f"Failed to initialize transformer: {e}")
            raise
    
    def parse_wfs_xml(self, xml_content: str) -> ET.Element:
        """Parse WFS XML content."""
        try:
            root = ET.fromstring(xml_content)
            feature_members = root.findall('.//{http://www.opengis.net/gml}featureMember')
            
            logger.info("Successfully parsed WFS XML")
            return feature_members
            
        except Exception as e:
            logger.error(f"Failed to parse XML: {e}")
            raise Exception(f"XML parsing failed: {e}")
    
    def extract_coordinates_from_string(self, coord_string: str) -> List[Tuple[float, float]]:
        """Extract coordinate pairs from GML coordinate string."""
        try:
            coord_pairs = coord_string.strip().split()
            coordinates = []
            
            for pair in coord_pairs:
                if ',' in pair:
                    lon_str, lat_str = pair.split(',')
                    lon = float(lon_str.strip())
                    lat = float(lat_str.strip())
                    coordinates.append((lon, lat))
            
            return coordinates
            
        except Exception as e:
            logger.error(f"Failed to extract coordinates from string: {e}")
            raise ValueError(f"Invalid coordinate string format: {coord_string}")
    
    def transform_coordinates(self, coordinates: List[Tuple[float, float]]) -> List[Tuple[float, float]]:
        """Transform coordinates from source CRS to target CRS."""
        try:
            transformed_coords = []
            
            for lon, lat in coordinates:
                x, y = self.transformer.transform(lon, lat)
                transformed_coords.append((x, y))
            
            return transformed_coords
            
        except Exception as e:
            logger.error(f"Coordinate transformation failed: {e}")
            raise Exception(f"Failed to transform coordinates: {e}")
           
    
    def create_wkt_from_rings(self, geom_type: str, rings: List[List[Tuple[float, float]]]) -> str:
        """
        Create WKT representation from coordinate rings based on geometry type.
        
        Args:
            geom_type: Original geometry type from GML  
            rings: List of coordinate rings
            
        Returns:
            WKT string representation
        """
        try:
            if not rings:
                raise ValueError("No coordinate rings provided")
            
            # Handle different geometry types
            if geom_type == 'Point':
                if rings and rings[0]:
                    point = Point(rings[0][0])
                    return wkt_dumps(point)
            
            elif geom_type in ['LineString', 'MultiLineString']:
                if len(rings) == 1:
                    linestring = LineString(rings[0])
                    return wkt_dumps(linestring)
                else:
                    multilinestring = MultiLineString(rings)
                    return wkt_dumps(multilinestring)
            
            elif geom_type == 'Polygon':
                if rings:
                    polygon = Polygon(rings[0])
                    return wkt_dumps(polygon)
            
            elif geom_type == 'MultiPolygon':
                # For MultiPolygon, create individual polygons from each ring
                polygons = []
                for ring in rings:
                    if len(ring) >= 4:  # At least 4 points for a closed polygon
                        polygons.append(Polygon(ring))
                
                if polygons:
                    if len(polygons) == 1:
                        # If only one polygon, return as POLYGON
                        return wkt_dumps(polygons[0])
                    else:
                        # Multiple polygons, return as MULTIPOLYGON
                        multipolygon = MultiPolygon(polygons)
                        return wkt_dumps(multipolygon)
            
            # Fallback: assume polygon
            if rings and len(rings[0]) >= 3:
                if len(rings) == 1:
                    polygon = Polygon(rings[0])
                    return wkt_dumps(polygon)
                else:
                    polygons = [Polygon(ring) for ring in rings if len(ring) >= 4]
                    if len(polygons) == 1:
                        return wkt_dumps(polygons[0])
                    else:
                        multipolygon = MultiPolygon(polygons)
                        return wkt_dumps(multipolygon)
                        
        except Exception as e:
            logger.error(f"Failed to create WKT from rings: {e}")
            # Manual fallback
            if len(rings) == 1:
                coord_pairs = [f"{x} {y}" for x, y in rings[0]]
                return f"POLYGON(({', '.join(coord_pairs)}))"
            else:
                polygon_parts = []
                for ring in rings:
                    coord_pairs = [f"{x} {y}" for x, y in ring]
                    polygon_parts.append(f"(({', '.join(coord_pairs)}))")
                return f"MULTIPOLYGON({', '.join(polygon_parts)})"
        
        raise ValueError(f"Could not create WKT for geometry type {geom_type}")
    
    def process_parcel_wfs_response(self, xml_content: str) -> List[Dict]:
        """Process complete WFS response and return transformed geometries."""
        results = []
        
        try:
            # Parse XML
            root = self.parse_wfs_xml(xml_content)
            
            # Find all feature members
            feature_members = root.findall('.//{http://www.opengis.net/gml}featureMember')
            
            logger.info(f"Found {len(feature_members)} feature members")
            
            # Process each feature member
            for i, feature_member in enumerate(feature_members):
                try:
                    # Find parseller elements
                    parcel_elements = []
                    for child in feature_member:
                        if 'parseller' in child.tag:
                            parcel_elements.append(child)
                    
                    for parcel_elem in parcel_elements:
                        # Extract FID from parseller element
                        fid_full = parcel_elem.get('fid', '')
                        fid_value = None
                        if fid_full and '.' in fid_full:
                            fid_value = fid_full.split('.')[-1]
                        
                        # Initialize feature data with all TKGM fields
                        feature_data = {
                            'fid': fid_value,
                            'parselno': None,
                            'adano': None,
                            'tapukimlikno': None,
                            'tapucinsaciklama': None,
                            'tapualan': None,
                            'tapuzeminref': None,
                            'tapumahalleref': None,
                            'tip': None,
                            'durum': None,
                            'sistemkayittarihi': None,
                            'onaydurum': None,
                            'kadastroalan': None,
                            'tapucinsid': None,
                            'sistemguncellemetarihi': None,
                            'kmdurum': None,
                            'hazineparseldurum': None,
                        }
                        
                        # Extract all feature attributes
                        for child in parcel_elem:
                            tag_name = child.tag.split('}')[-1]  # Remove namespace
                            if tag_name == 'parselno':
                                feature_data['parselno'] = child.text
                            elif tag_name == 'adano':
                                feature_data['adano'] = child.text
                            elif tag_name == 'tapukimlikno':
                                feature_data['tapukimlikno'] = child.text
                            elif tag_name == 'tapucinsaciklama':
                                feature_data['tapucinsaciklama'] = child.text
                            elif tag_name == 'tapualan':
                                feature_data['tapualan'] = child.text
                            elif tag_name == 'tapuzeminref':
                                feature_data['tapuzeminref'] = child.text
                            elif tag_name == 'tapumahalleref':
                                feature_data['tapumahalleref'] = child.text
                            elif tag_name == 'tip':
                                feature_data['tip'] = child.text
                            elif tag_name == 'durum':
                                feature_data['durum'] = child.text
                            elif tag_name == 'sistemkayittarihi':
                                feature_data['sistemkayittarihi'] = child.text
                            elif tag_name == 'onaydurum':
                                feature_data['onaydurum'] = child.text
                            elif tag_name == 'kadastroalan':
                                feature_data['kadastroalan'] = child.text
                            elif tag_name == 'tapucinsid':
                                feature_data['tapucinsid'] = child.text
                            elif tag_name == 'sistemguncellemetarihi':
                                feature_data['sistemguncellemetarihi'] = child.text
                            elif tag_name == 'kmdurum':
                                feature_data['kmdurum'] = child.text
                            elif tag_name == 'hazineparseldurum':
                                feature_data['hazineparseldurum'] = child.text
                        
                        # Find and process geometry elements (prioritize MultiPolygon over Polygon)
                        geometry_found = False
                        geometry_types = ['MultiPolygon', 'Polygon', 'Point', 'LineString', 'MultiPoint', 'MultiLineString']
                        
                        for geom_type in geometry_types:
                            if geometry_found:
                                break  # Only process the first geometry type found

                            geom_elements = parcel_elem.findall(f'.//{{{namespaces["gml"]}}}{geom_type}')
                            
                            for geom_elem in geom_elements:
                                try:
                                    # Process the geometry element
                                    geometry_data = self.process_geometry_element(geom_elem)
                                    
                                    # Create result dictionary
                                    result = {
                                        **feature_data,
                                        'geometry_type': geometry_data['geometry_type'],
                                        'rings_count': geometry_data['rings_count'],
                                        'original_coords': geometry_data['original_rings'][0] if geometry_data['original_rings'] else [],
                                        'transformed_coords': geometry_data['transformed_rings'][0] if geometry_data['transformed_rings'] else [],
                                        'all_original_rings': geometry_data['original_rings'],
                                        'all_transformed_rings': geometry_data['transformed_rings'],
                                        'original_crs': self.source_crs,
                                        'target_crs': self.target_crs,
                                        'wkt': geometry_data['wkt']
                                    }
                                    
                                    results.append(result)
                                    geometry_found = True
                                    
                                    logger.info(f"Processed {geometry_data['geometry_type']} - Feature {i+1}: Parsel {feature_data['parselno']}, FID: {fid_value}, Rings: {geometry_data['rings_count']}")
                                    break  # Only process the first geometry element of this type
                                    
                                except Exception as e:
                                    logger.error(f"Failed to process {geom_type} geometry: {e}")
                                    continue
                        
                        if not geometry_found:
                            logger.warning(f"No supported geometry found for feature {i+1}: Parsel {feature_data['parselno']}")
                            
                except Exception as e:
                    logger.error(f"Failed to process feature member {i+1}: {e}")
                    continue
            
            logger.info(f"Successfully processed {len(results)} geometries")
            return results
            
        except Exception as e:
            logger.error(f"Failed to process WFS response: {e}")
            raise
    
    def process_district_wfs_response(self, xml_content: str) -> List[Dict]:
        """Process complete WFS response and return transformed geometries."""
        results = []
        
        try:
            # Parse XML
            root = self.parse_wfs_xml(xml_content)
            
            # Find all feature members
            feature_members = root.findall('.//{http://www.opengis.net/gml}featureMember')
            
            logger.info(f"Found {len(feature_members)} feature members")
            
            # Process each feature member
            for i, feature_member in enumerate(feature_members):
                try:
                    # Find ilceler elements
                    district_elements = []
                    for child in feature_member:
                        if 'ilceler' in child.tag:
                            district_elements.append(child)
                    
                    for district_elem in district_elements:
                        # Extract FID from ilceler element
                        fid_full = district_elem.get('fid', '')
                        fid_value = None
                        if fid_full and '.' in fid_full:
                            fid_value = fid_full.split('.')[-1]
                        
                        # Initialize feature data with all TKGM fields
                        feature_data = {
                            'fid': fid_value,
                            'tapukimlikno': None,
                            'ilref': None,
                            'ad': None,
                            'durum': None,
                        }
                        
                        # Extract all feature attributes
                        for child in district_elem:
                            tag_name = child.tag.split('}')[-1]  # Remove namespace
                            if tag_name == 'tapukimlikno':
                                feature_data['tapukimlikno'] = child.text
                            elif tag_name == 'ilref':
                                feature_data['ilref'] = child.text
                            elif tag_name == 'ad':
                                feature_data['ad'] = child.text
                            elif tag_name == 'durum':
                                feature_data['durum'] = child.text
                        
                        # Find and process geometry elements (prioritize MultiPolygon over Polygon)
                        geometry_found = False
                        geometry_types = ['MultiPolygon', 'Polygon', 'Point', 'LineString', 'MultiPoint', 'MultiLineString']
                        
                        for geom_type in geometry_types:
                            if geometry_found:
                                break  # Only process the first geometry type found
                                
                            geom_elements = district_elem.findall(f'.//{{{namespaces["gml"]}}}{geom_type}')
                            
                            for geom_elem in geom_elements:
                                try:
                                    # Process the geometry element
                                    geometry_data = self.process_geometry_element(geom_elem)
                                    
                                    # Create result dictionary
                                    result = {
                                        **feature_data,
                                        'geometry_type': geometry_data['geometry_type'],
                                        'rings_count': geometry_data['rings_count'],
                                        'original_coords': geometry_data['original_rings'][0] if geometry_data['original_rings'] else [],
                                        'transformed_coords': geometry_data['transformed_rings'][0] if geometry_data['transformed_rings'] else [],
                                        'all_original_rings': geometry_data['original_rings'],
                                        'all_transformed_rings': geometry_data['transformed_rings'],
                                        'original_crs': self.source_crs,
                                        'target_crs': self.target_crs,
                                        'wkt': geometry_data['wkt']
                                    }
                                    
                                    results.append(result)
                                    geometry_found = True
                                    
                                    logger.info(f"Processed {geometry_data['geometry_type']} - Feature {i+1}: İlçe {feature_data['ad']}, FID: {fid_value}, Rings: {geometry_data['rings_count']}")
                                    break  # Only process the first geometry element of this type
                                    
                                except Exception as e:
                                    logger.error(f"Failed to process {geom_type} geometry: {e}")
                                    continue
                        
                        if not geometry_found:
                            logger.warning(f"No supported geometry found for feature {i+1}: Parsel {feature_data['parselno']}")
                            
                except Exception as e:
                    logger.error(f"Failed to process feature member {i+1}: {e}")
                    continue
            
            logger.info(f"Successfully processed {len(results)} geometries")
            return results
            
        except Exception as e:
            logger.error(f"Failed to process WFS response: {e}")
            raise
    
    def process_neighbourhood_wfs_response(self, xml_content: str) -> List[Dict]:
        """Process complete WFS response and return transformed geometries."""
        results = []
        
        try:
            # Parse XML
            root = self.parse_wfs_xml(xml_content)
            
            # Find all feature members
            feature_members = root.findall('.//{http://www.opengis.net/gml}featureMember')
            
            logger.info(f"Found {len(feature_members)} feature members")
            
            # Process each feature member
            for i, feature_member in enumerate(feature_members):
                try:
                    # Find mahalleler elements
                    neighbourhood_elements = []
                    for child in feature_member:
                        if 'mahalleler' in child.tag:
                            neighbourhood_elements.append(child)
                    
                    for neighbourhood_elem in neighbourhood_elements:
                        # Extract FID from mahalleler element
                        fid_full = neighbourhood_elem.get('fid', '')
                        fid_value = None
                        if fid_full and '.' in fid_full:
                            fid_value = fid_full.split('.')[-1]
                        
                        # Initialize feature data with all TKGM fields
                        feature_data = {
                            'fid': fid_value,
                            'ilceref': None,
                            'tapukimlikno': None,
                            'durum': None,
                            'sistemkayittarihi': None,
                            'tip': None,
                            'tapumahallead': None,
                            'kadastromahallead': None
                        }
                        
                        # Extract all feature attributes
                        for child in neighbourhood_elem:
                            tag_name = child.tag.split('}')[-1]  # Remove namespace
                            if tag_name == 'ilceref':
                                feature_data['ilceref'] = child.text
                            elif tag_name == 'tapukimlikno':
                                feature_data['tapukimlikno'] = child.text
                            elif tag_name == 'durum':
                                feature_data['durum'] = child.text
                            elif tag_name == 'sistemkayittarihi':
                                feature_data['sistemkayittarihi'] = child.text
                            elif tag_name == 'tip':
                                feature_data['tip'] = child.text
                            elif tag_name == 'tapumahallead':
                                feature_data['tapumahallead'] = child.text
                            elif tag_name == 'kadastromahallead':
                                feature_data['kadastromahallead'] = child.text
                        
                        # Find and process geometry elements (prioritize MultiPolygon over Polygon)
                        geometry_found = False
                        geometry_types = ['MultiPolygon', 'Polygon', 'Point', 'LineString', 'MultiPoint', 'MultiLineString']
                        
                        for geom_type in geometry_types:
                            if geometry_found:
                                break  # Only process the first geometry type found
                                
                            geom_elements = neighbourhood_elem.findall(f'.//{{{namespaces["gml"]}}}{geom_type}')
                            
                            for geom_elem in geom_elements:
                                try:
                                    # Process the geometry element
                                    geometry_data = self.process_geometry_element(geom_elem)
                                    
                                    # Create result dictionary
                                    result = {
                                        **feature_data,
                                        'geometry_type': geometry_data['geometry_type'],
                                        'rings_count': geometry_data['rings_count'],
                                        'original_coords': geometry_data['original_rings'][0] if geometry_data['original_rings'] else [],
                                        'transformed_coords': geometry_data['transformed_rings'][0] if geometry_data['transformed_rings'] else [],
                                        'all_original_rings': geometry_data['original_rings'],
                                        'all_transformed_rings': geometry_data['transformed_rings'],
                                        'original_crs': self.source_crs,
                                        'target_crs': self.target_crs,
                                        'wkt': geometry_data['wkt']
                                    }
                                    
                                    results.append(result)
                                    geometry_found = True
                                    
                                    logger.info(f"Processed {geometry_data['geometry_type']} - Feature {i+1}: Tapu Kimlik No: {feature_data['tapukimlikno']}, FID: {fid_value}, Rings: {geometry_data['rings_count']}")
                                    break  # Only process the first geometry element of this type
                                    
                                except Exception as e:
                                    logger.error(f"Failed to process {geom_type} geometry: {e}")
                                    continue
                        
                        if not geometry_found:
                            logger.warning(f"No supported geometry found for feature {i+1}: Parsel {feature_data['parselno']}")
                            
                except Exception as e:
                    logger.error(f"Failed to process feature member {i+1}: {e}")
                    continue
            
            logger.info(f"Successfully processed {len(results)} geometries")
            return results
            
        except Exception as e:
            logger.error(f"Failed to process WFS response: {e}")
            raise
    

    def process_geometry_element(self, elem):
        """Process a single geometry element and return its details"""
        # Find and process geometry elements (prioritize MultiPolygon over Polygon)
        geometry_found = False
        geometry_types = ['MultiPolygon', 'Polygon', 'Point', 'LineString', 'MultiPoint', 'MultiLineString']
        geometry = []
        
        for geom_type in geometry_types:
            if geometry_found:
                break  # Only process the first geometry type found
                
            geom_elements = elem.findall(f'.//{{{namespaces["gml"]}}}{geom_type}')
            
            for geom_elem in geom_elements:
                try:
                    # Get geometry type from element tag
                    geom_type = geom_elem.tag.split('}')[-1]  # Remove namespace
                    
                    # Find all coordinate elements within this geometry
                    coord_elements = geom_elem.findall('.//{http://www.opengis.net/gml}coordinates')
                    
                    # Extract all coordinate rings
                    all_original_rings = []
                    all_transformed_rings = []
                    
                    for coord_elem in coord_elements:
                        if coord_elem.text:
                            # Extract coordinates from this ring
                            original_coords = self.extract_coordinates_from_string(coord_elem.text)
                            transformed_coords = self.transform_coordinates(original_coords)
                            
                            # Ensure ring is closed for polygons
                            if geom_type in ['MultiPolygon', 'Polygon'] and len(transformed_coords) >= 3:
                                if transformed_coords[0] != transformed_coords[-1]:
                                    transformed_coords.append(transformed_coords[0])
                                    original_coords.append(original_coords[0])
                            
                            all_original_rings.append(original_coords)
                            all_transformed_rings.append(transformed_coords)
                    
                    # Create appropriate WKT based on geometry type and number of rings
                    wkt = self.create_wkt_from_rings(geom_type, all_transformed_rings)
                                        
                    # Create result dictionary
                    result = {
                        'geometry_type': geom_type,
                        'rings_count': len(all_transformed_rings),
                        'original_coords': all_original_rings[0] if all_original_rings else [],
                        'transformed_coords': all_transformed_rings[0] if all_transformed_rings else [],
                        'all_original_rings': all_original_rings,
                        'all_transformed_rings': all_transformed_rings,
                        'original_crs': 'EPSG:4326',
                        'target_crs': 'EPSG:2320',
                        'wkt': wkt
                    }
                    
                    geometry.append(result)
                    geometry_found = True
                    
                    logger.info(f"Processed {geom_type} - Feature {i+1}: Tapu Kimlik No: {feature_data['tapukimlikno']}, FID: {fid_value}, Rings: {len(all_transformed_rings)}")
                    break  # Only process the first geometry element of this type
                    
                except Exception as e:
                    logger.error(f"Failed to process {geom_type} geometry: {e}")
                    continue
                        
            if not geometry_found:
                logger.warning(f"No supported geometry found for feature {i+1}: Parsel {feature_data['parselno']}")
                    
        logger.info(f"Successfully processed {len(geometry)} geometries")    
        return geometry
