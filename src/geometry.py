#!/usr/bin/env python3
"""
WFS Geometri Ayrıştırıcısı ve Koordinat Dönüştürücüsü

Bu betik WFS FeatureCollection yanıtlarını ayrıştırır, geometri verilerini çıkarır,
koordinatları EPSG:4326'dan EPSG:2320'ye dönüştürür ve WKT formatına çevirir.

Bağımlılıklar:
    pip install shapely pyproj
"""

from loguru import logger
from typing import List, Dict, Tuple, Any

# Use lxml for faster XML parsing (2-5x faster than xml.etree)
try:
    from lxml import etree as ET
    LXML_AVAILABLE = True
    logger.debug("lxml kullanılıyor (optimize edilmiş XML parsing)")
except ImportError:
    # Fallback to standard library
    from xml.etree import ElementTree as ET
    LXML_AVAILABLE = False
    logger.warning("lxml bulunamadı, xml.etree kullanılıyor (daha yavaş)")

try:
    from shapely.geometry import Polygon, MultiPolygon, Point, LineString, MultiLineString
    from shapely.wkt import dumps as wkt_dumps, loads as wkt_loads
    SHAPELY_AVAILABLE = True
except ImportError:
    SHAPELY_AVAILABLE = False
    logger.error("Shapely gerekli ancak mevcut değil")

try:
    from pyproj import Transformer
    PYPROJ_AVAILABLE = True
except ImportError:
    PYPROJ_AVAILABLE = False
    logger.error("PyProj gerekli ancak mevcut değil")

# GDAL Python binding (osgeo)
try:
    from osgeo import ogr, osr
    GDAL_AVAILABLE = True
    logger.info("Koordinat dönüşüm yöntemi: GDAL Python bindings (osgeo)")
except ImportError:
    GDAL_AVAILABLE = False
    logger.warning("GDAL bulunamadı, Shapely+PyProj kullanılacak (daha yavaş)")


class WFSGeometryProcessor:
    """
    WFS FeatureCollection yanıtlarını işlemek ve geometrileri dönüştürmek için bir sınıf.
    """
    
    def __init__(self, source_crs: str = "EPSG:4326", target_crs: str = "EPSG:2320") -> None:
        """
        İşlemciyi kaynak ve hedef koordinat sistemleri ile başlatır.
        """
        self.source_crs = source_crs
        self.target_crs = target_crs
        self.transformer = None
        
        # XML ad alanlarını tanımla
        self.namespaces = {
            'gml': 'http://www.opengis.net/gml',
            'wfs': 'http://www.opengis.net/wfs',
            'tkgm': 'http://tkgm.gov.tr'
        }
        
        # Günlükleme ayarla
        logger.info("WFSGeometryProcessor başlatıldı")
        
        # Bağımlılıkları kontrol et
        self._check_dependencies()

        # Dönüştürücüyü seç ve başlat
        if GDAL_AVAILABLE:
            self._init_gdal_transformer()
        else:
            self._init_pyproj_transformer()
    
    def _check_dependencies(self) -> None:
        """Gerekli bağımlılıkların mevcut olup olmadığını kontrol eder."""
        missing_deps = []

        # Shapely WKT üretimi ve WKT'den halkalar için zorunlu
        if not SHAPELY_AVAILABLE:
            missing_deps.append("shapely")

        # PyProj sadece GDAL yoksa zorunlu
        if not PYPROJ_AVAILABLE and not GDAL_AVAILABLE:
            missing_deps.append("pyproj")

        if missing_deps:
            raise ImportError(f"Eksik gerekli bağımlılıklar: {', '.join(missing_deps)}")
    
    
    def _init_pyproj_transformer(self) -> None:
        """PyProj koordinat dönüştürücüsünü başlatır."""
        try:
            self.transformer = Transformer.from_crs(
                self.source_crs,
                self.target_crs,
                always_xy=True
            )
            logger.info(f"PyProj dönüştürücü başlatıldı: {self.source_crs} -> {self.target_crs}")
        except Exception as e:
            logger.error(f"PyProj dönüştürücü başlatılamadı: {e}")
            raise

    def _init_gdal_transformer(self) -> None:
        """GDAL koordinat dönüştürücüsünü başlatır."""
        try:
            def _epsg_code(crs_str: str) -> int:
                if crs_str.upper().startswith("EPSG:"):
                    return int(crs_str.split(":", 1)[1])
                return int(crs_str)

            source = osr.SpatialReference()
            source.ImportFromEPSG(_epsg_code(self.source_crs))

            target = osr.SpatialReference()
            target.ImportFromEPSG(_epsg_code(self.target_crs))

            self.gdal_transformer = osr.CoordinateTransformation(source, target)
            logger.info(f"GDAL dönüştürücü başlatıldı: {self.source_crs} -> {self.target_crs}")
        except Exception as e:
            logger.error(f"GDAL dönüştürücü başlatılamadı: {e}")
            # GDAL başarısız olursa PyProj'a düş
            self._init_pyproj_transformer()

    def transform_geometry_gdal(self, wkt: str) -> str:
        """GDAL ile WKT geometriyi hedef CRS'ye dönüştürür."""
        if not GDAL_AVAILABLE:
            raise RuntimeError("GDAL mevcut değil")
        try:
            geom = ogr.CreateGeometryFromWkt(wkt)
            geom.Transform(self.gdal_transformer)
            return geom.ExportToWkt()
        except Exception as e:
            logger.error(f"GDAL ile geometri dönüştürme başarısız: {e}")
            raise

    def _rings_from_wkt(self, wkt: str, geom_type_hint: str) -> List[List[Tuple[float, float]]]:
        """WKT'den koordinat halkalarını çıkarır."""
        try:
            geom = wkt_loads(wkt)
            rings: List[List[Tuple[float, float]]] = []
            gt = geom.geom_type

            if gt == 'Point':
                x, y = geom.x, geom.y
                rings = [[(x, y)]]
            elif gt == 'LineString':
                rings = [[(float(x), float(y)) for x, y in geom.coords]]
            elif gt == 'MultiLineString':
                rings = [[(float(x), float(y)) for x, y in line.coords] for line in geom.geoms]
            elif gt == 'Polygon':
                rings = [[(float(x), float(y)) for x, y in geom.exterior.coords]]
            elif gt == 'MultiPolygon':
                rings = [[(float(x), float(y)) for x, y in poly.exterior.coords] for poly in geom.geoms]
            elif gt == 'MultiPoint':
                rings = [[(float(p.x), float(p.y))] for p in geom.geoms]
            else:
                # Yedek: dış halkayı almayı dene
                if hasattr(geom, 'exterior') and geom.exterior is not None:
                    rings = [[(float(x), float(y)) for x, y in geom.exterior.coords]]
            return rings
        except Exception as e:
            logger.error(f"WKT'den halkalar çıkarılamadı: {e}")
            return []

    def benchmark_transform(self, geom_type: str, rings: List[List[Tuple[float, float]]], iterations: int = 100) -> Dict[str, Any]:
        """Aynı geometriyi GDAL ve PyProj ile dönüştürerek performans ve doğruluk karşılaştırması yapar."""
        import time

        results: Dict[str, Any] = {
            'iterations': iterations,
            'gdal_available': GDAL_AVAILABLE,
            'pyproj_available': PYPROJ_AVAILABLE,
            'gdal_seconds': None,
            'pyproj_seconds': None,
            'same_geometry': None,
        }

        # Orijinal WKT
        wkt_original = self.create_wkt_from_rings(geom_type, rings)

        # GDAL süresi
        if GDAL_AVAILABLE:
            start = time.perf_counter()
            wkt_gdal = None
            for _ in range(iterations):
                wkt_gdal = self.transform_geometry_gdal(wkt_original)
            results['gdal_seconds'] = time.perf_counter() - start
        else:
            wkt_gdal = None

        # PyProj süresi
        start = time.perf_counter()
        wkt_pyproj = None
        for _ in range(iterations):
            transformed_rings = []
            for ring in rings:
                transformed_ring = self.transform_coordinates(ring)
                transformed_rings.append(transformed_ring)
            wkt_pyproj = self.create_wkt_from_rings(geom_type, transformed_rings)
        results['pyproj_seconds'] = time.perf_counter() - start

        # Doğruluk karşılaştırması (WKT'ler aynı geometrileri temsil ediyor mu?)
        if wkt_gdal and wkt_pyproj:
            try:
                g1 = wkt_loads(wkt_gdal)
                g2 = wkt_loads(wkt_pyproj)
                results['same_geometry'] = g1.equals_exact(g2, 1e-8)
            except Exception:
                results['same_geometry'] = False
        else:
            results['same_geometry'] = None

        logger.info(f"Benchmark - GDAL: {results['gdal_seconds']} sn, PyProj: {results['pyproj_seconds']} sn, same: {results['same_geometry']}")
        return results
    
    def parse_wfs_xml(self, xml_content: str) -> ET.Element:
        """WFS XML içeriğini ayrıştırır - lxml optimize edilmiş."""
        try:
            # lxml bytes'ı doğrudan işleyebilir (xml.etree'den daha hızlı)
            if isinstance(xml_content, str):
                xml_content = xml_content.encode('utf-8')
            
            # lxml.etree.fromstring doğrudan bytes alır
            root = ET.fromstring(xml_content)
            feature_members = root.findall('.//{http://www.opengis.net/gml}featureMember')
            
            logger.info(f"WFS XML başarıyla ayrıştırıldı: {len(feature_members)} feature member bulundu")
            return feature_members
            
        except Exception as e:
            logger.error(f"XML ayrıştırma başarısız: {e}")
            raise Exception(f"XML ayrıştırma başarısız: {e}")
    
    def extract_coordinates_from_string(self, coord_string: str) -> List[Tuple[float, float]]:
        """GML koordinat dizesinden koordinat çiftlerini çıkarır."""
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
            logger.error(f"Koordinat dizesinden koordinat çıkarma başarısız: {e}")
            raise ValueError(f"Geçersiz koordinat dizesi formatı: {coord_string}")
    
    def transform_coordinates(self, coordinates: List[Tuple[float, float]]) -> List[Tuple[float, float]]:
        """Koordinatları kaynak CRS'den hedef CRS'ye dönüştürür (PyProj)."""
        try:
            transformed_coords = []
            for lon, lat in coordinates:
                x, y = self.transformer.transform(lon, lat)
                transformed_coords.append((x, y))
            return transformed_coords
        except Exception as e:
            logger.error(f"Koordinat dönüştürme başarısız: {e}")
            raise Exception(f"Koordinat dönüştürme başarısız: {e}")
     
    def create_wkt_from_rings(self, geom_type: str, rings: List[List[Tuple[float, float]]]) -> str:
        """
        Geometri tipine göre koordinat halkalarından WKT temsili oluşturur.
        
        Args:
            geom_type: GML'den gelen orijinal geometri tipi  
            rings: Koordinat halkaları listesi
            
        Returns:
            WKT dize temsili
        """
        try:
            if not rings:
                raise ValueError("Koordinat halkası sağlanmadı")
            
            # Farklı geometri tiplerini işle
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
                # MultiPolygon için, her halkadan ayrı poligonlar oluştur
                polygons = []
                for ring in rings:
                    if len(ring) >= 4:  # Kapalı poligon için en az 4 nokta
                        polygons.append(Polygon(ring))
                
                if polygons:
                    if len(polygons) == 1:
                        # Sadece bir poligon varsa, POLYGON olarak döndür
                        return wkt_dumps(polygons[0])
                    else:
                        # Birden fazla poligon, MULTIPOLYGON olarak döndür
                        multipolygon = MultiPolygon(polygons)
                        return wkt_dumps(multipolygon)
            
            # Yedek: poligon olarak varsay
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
            logger.error(f"Halkalardan WKT oluşturma başarısız: {e}")
            # Manuel yedek
            if len(rings) == 1:
                coord_pairs = [f"{x} {y}" for x, y in rings[0]]
                return f"POLYGON(({', '.join(coord_pairs)}))"
            else:
                polygon_parts = []
                for ring in rings:
                    coord_pairs = [f"{x} {y}" for x, y in ring]
                    polygon_parts.append(f"(({', '.join(coord_pairs)}))")
                return f"MULTIPOLYGON({', '.join(polygon_parts)})"
        
        raise ValueError(f"{geom_type} geometri tipi için WKT oluşturulamadı")
    
    def process_parcel_wfs_response(self, xml_content: str) -> List[Dict]:
        """Tam WFS yanıtını işler ve dönüştürülmüş geometrileri döndürür."""
        results = []
        
        try:
            # XML'i ayrıştır
            root = self.parse_wfs_xml(xml_content)
            
            # Tüm özellik üyelerini bul
            feature_members = root.findall('.//{http://www.opengis.net/gml}featureMember')
            
            logger.info(f"{len(feature_members)} feature member bulundu")
            
            # Her feature memberi işle
            for i, feature_member in enumerate(feature_members):
                try:
                    # Parseller öğelerini bul
                    parcel_elements = []
                    for child in feature_member:
                        if 'parseller' in child.tag:
                            parcel_elements.append(child)
                    
                    for parcel_elem in parcel_elements:
                        # Parseller öğesinden FID'yi çıkar
                        fid_full = parcel_elem.get('fid', '')
                        fid_value = None
                        if fid_full and '.' in fid_full:
                            fid_value = fid_full.split('.')[-1]
                        
                        # Tüm TKGM alanları ile özellik verilerini başlat
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
                        
                        # Tüm özellik niteliklerini çıkar
                        for child in parcel_elem:
                            tag_name = child.tag.split('}')[-1]  # Ad alanını kaldır
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
                        
                        # Geometri öğelerini bul ve işle (MultiPolygon'u Polygon'dan önceliklendir)
                        geometry_found = False
                        geometry_types = ['MultiPolygon', 'Polygon', 'Point', 'LineString', 'MultiPoint', 'MultiLineString']
                        
                        for geom_type in geometry_types:
                            if geometry_found:
                                break  # Sadece bulunan ilk geometri tipini işle

                            geom_elements = parcel_elem.findall(f'.//{{{self.namespaces["gml"]}}}{geom_type}')
                            
                            for geom_elem in geom_elements:
                                try:
                                    # Geometri öğesini işle
                                    geometry_data = self.process_geometry_element(geom_elem)
                                    
                                    # Sonuç sözlüğü oluştur
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
                                    
                                    logger.info(f"{geometry_data['geometry_type']} işlendi - Özellik {i+1}: Parsel {feature_data['parselno']}, FID: {fid_value}, Halkalar: {geometry_data['rings_count']}")
                                    break  # Bu tipte sadece ilk geometri öğesini işle
                                    
                                except Exception as e:
                                    logger.error(f"{geom_type} geometrisi işlenirken hata oluştu: {e}")
                                    continue
                        
                        if not geometry_found:
                            logger.warning(f"Özellik {i+1} için desteklenen geometri bulunamadı: Parsel {feature_data['parselno']}")
                            
                except Exception as e:
                    logger.error(f"Feature member {i+1} işlenirken hata oluştu: {e}")
                    continue
            
            logger.info(f"Toplam {len(results)} geometri başarıyla işlendi")
            return results
            
        except Exception as e:
            logger.error(f"WFS yanıtı işlenirken hata oluştu: {e}")
            raise
    
    def process_district_wfs_response(self, xml_content: str) -> List[Dict]:
        """Tam WFS yanıtını işler ve dönüştürülmüş geometrileri döndürür."""
        results = []
        
        try:
            # XML'i ayrıştır
            root = self.parse_wfs_xml(xml_content)
            
            # Tüm özellik üyelerini bul
            feature_members = root.findall('.//{http://www.opengis.net/gml}featureMember')
            
            logger.info(f"{len(feature_members)} feature member bulundu")
            
            # Her feature memberi işle
            for i, feature_member in enumerate(feature_members):
                try:
                    # İlçeler öğelerini bul
                    district_elements = []
                    for child in feature_member:
                        if 'ilceler' in child.tag:
                            district_elements.append(child)
                    
                    for district_elem in district_elements:
                        # İlçeler öğesinden FID'yi çıkar
                        fid_full = district_elem.get('fid', '')
                        fid_value = None
                        if fid_full and '.' in fid_full:
                            fid_value = fid_full.split('.')[-1]
                        
                        # Tüm TKGM alanları ile özellik verilerini başlat
                        feature_data = {
                            'fid': fid_value,
                            'tapukimlikno': None,
                            'ilref': None,
                            'ad': None,
                            'durum': None,
                        }
                        
                        # Tüm özellik niteliklerini çıkar
                        for child in district_elem:
                            tag_name = child.tag.split('}')[-1]  # Ad alanını kaldır
                            if tag_name == 'tapukimlikno':
                                feature_data['tapukimlikno'] = child.text
                            elif tag_name == 'ilref':
                                feature_data['ilref'] = child.text
                            elif tag_name == 'ad':
                                feature_data['ad'] = child.text
                            elif tag_name == 'durum':
                                feature_data['durum'] = child.text
                        
                        # Geometri öğelerini bul ve işle (MultiPolygon'u Polygon'dan önceliklendir)
                        geometry_found = False
                        geometry_types = ['MultiPolygon', 'Polygon', 'Point', 'LineString', 'MultiPoint', 'MultiLineString']
                        
                        for geom_type in geometry_types:
                            if geometry_found:
                                break  # Sadece bulunan ilk geometri tipini işle
                                
                            geom_elements = district_elem.findall(f'.//{{{self.namespaces["gml"]}}}{geom_type}')
                            
                            for geom_elem in geom_elements:
                                try:
                                    # Geometri öğesini işle
                                    geometry_data = self.process_geometry_element(geom_elem)
                                    
                                    # Sonuç sözlüğü oluştur
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
                                    
                                    logger.info(f"{geometry_data['geometry_type']} işlendi - Özellik {i+1}: İlçe {feature_data['ad']}, FID: {fid_value}, Halkalar: {geometry_data['rings_count']}")
                                    break  # Bu tipte sadece ilk geometri öğesini işle
                                    
                                except Exception as e:
                                    logger.error(f"{geom_type} geometrisi işlenirken hata oluştu: {e}")
                                    continue
                        
                        if not geometry_found:
                            logger.warning(f"Özellik {i+1} için desteklenen geometri bulunamadı: İlçe {feature_data['ad']}")
                            
                except Exception as e:
                    logger.error(f"Feature member {i+1} işlenirken hata oluştu: {e}")
                    continue
            
            logger.info(f"Toplam {len(results)} geometri başarıyla işlendi")
            return results
            
        except Exception as e:
            logger.error(f"WFS yanıtı işlenirken hata oluştu: {e}")
            raise    

    def process_geometry_element(self, elem) -> Dict[str, Any]:
        """Tek bir geometri öğesini işler ve detaylarını döndürür"""
        # Geometri öğelerini bul ve işle (MultiPolygon'u Polygon'dan önceliklendir)
        geometry_found = False
        geometry_types = ['MultiPolygon', 'Polygon', 'Point', 'LineString', 'MultiPoint', 'MultiLineString']
        
        for geom_type in geometry_types:
            if geometry_found:
                break  # Sadece bulunan ilk geometri tipini işle
                
            geom_elements = elem.findall(f'.//{{{self.namespaces["gml"]}}}{geom_type}')
            
            for geom_elem in geom_elements:
                try:
                    # Öğe etiketinden geometri tipini al
                    geom_type = geom_elem.tag.split('}')[-1]  # Ad alanını kaldır
                    
                    # Bu geometri içindeki tüm koordinat öğelerini bul
                    coord_elements = geom_elem.findall('.//{http://www.opengis.net/gml}coordinates')
                    
                    # Tüm koordinat halkalarını çıkar
                    all_original_rings = []
                    all_transformed_rings = []
                    
                    for coord_elem in coord_elements:
                        if coord_elem.text is not None and coord_elem.text.strip():
                            # Bu halkadan koordinatları çıkar
                            original_coords = self.extract_coordinates_from_string(coord_elem.text)

                            # Poligonlar için orijinal halkanın kapalı olduğundan emin ol
                            if geom_type in ['MultiPolygon', 'Polygon'] and len(original_coords) >= 3:
                                if original_coords[0] != original_coords[-1]:
                                    original_coords.append(original_coords[0])

                            if not GDAL_AVAILABLE:
                                transformed_coords = self.transform_coordinates(original_coords)
                                # Poligonlar için dönüştürülmüş halkanın kapalı olduğundan emin ol
                                if geom_type in ['MultiPolygon', 'Polygon'] and len(transformed_coords) >= 3:
                                    if transformed_coords[0] != transformed_coords[-1]:
                                        transformed_coords.append(transformed_coords[0])
                                all_transformed_rings.append(transformed_coords)
                            
                            all_original_rings.append(original_coords)
                    
                    # GDAL varsa WKT bazlı dönüşüm yap; yoksa PyProj ile koordinatları dönüştür
                    if GDAL_AVAILABLE:
                        # Orijinal halkalardan WKT oluştur ve GDAL ile dönüştür
                        wkt_original = self.create_wkt_from_rings(geom_type, all_original_rings)
                        wkt = self.transform_geometry_gdal(wkt_original)
                        all_transformed_rings = self._rings_from_wkt(wkt, geom_type)
                        logger.debug("WKT dönüşümü GDAL ile yapıldı")
                    else:
                        # PyProj ile koordinatları dönüştür ve WKT üret
                        wkt = self.create_wkt_from_rings(geom_type, all_transformed_rings)
                        logger.debug("Koordinat dönüşümü PyProj ile yapıldı")
                                        
                    # Sonuç sözlüğü oluştur
                    result = {
                        'geometry_type': geom_type,
                        'rings_count': len(all_transformed_rings),
                        'original_rings': all_original_rings,
                        'transformed_rings': all_transformed_rings,
                        'original_crs': self.source_crs,
                        'target_crs': self.target_crs,
                        'wkt': wkt
                    }
                    
                    geometry_found = True
                    logger.debug(f"{geom_type} geometrisi işlendi: {len(all_transformed_rings)} halka")
                    return result  # Bulunan ilk geometriyi döndür
                    
                except Exception as e:
                    logger.error(f"{geom_type} geometrisi işlenirken hata oluştu: {e}")
                    continue
                        
        if not geometry_found:
            logger.warning("Öğede desteklenen geometri bulunamadı")
            return None
