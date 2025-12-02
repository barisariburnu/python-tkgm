#!/usr/bin/env python3
"""
WFS Geometri Ayrıştırıcısı ve Koordinat Dönüştürücüsü

Bu modül WFS FeatureCollection yanıtlarını ayrıştırır, geometri verilerini çıkarır,
koordinatları EPSG:4326'dan EPSG:2320'ye dönüştürür ve WKT formatına çevirir.

Bağımlılıklar:
    pip install pyproj loguru lxml
"""

import xml.etree.ElementTree as ET
from typing import List, Dict, Tuple, Any, Optional
from pyproj import Transformer
from loguru import logger


class WFSGeometryProcessor:
    """
    WFS FeatureCollection yanıtlarını işlemek ve geometrileri dönüştürmek için bir sınıf.
    """
    
    # XML namespace'leri - class-level constant (tüm instance'lar için tek bir kopya)
    NAMESPACES = {
        'gml': 'http://www.opengis.net/gml',
        'wfs': 'http://www.opengis.net/wfs',
        'TKGM': 'http://www.tkgm.gov.tr'
    }
    
    def __init__(self, source_crs: str = "EPSG:4326", target_crs: str = "EPSG:2320") -> None:
        """
        İşlemciyi kaynak ve hedef koordinat sistemleri ile başlatır.
        
        Args:
            source_crs: Kaynak koordinat sistemi (varsayılan: EPSG:4326)
            target_crs: Hedef koordinat sistemi (varsayılan: EPSG:2320)
        """
        self.source_crs = source_crs
        self.target_crs = target_crs
        
        # Geriye uyumluluk için instance-level referans
        self.namespaces = self.NAMESPACES
        
        # Koordinat dönüştürücüyü başlat
        self.transformer = Transformer.from_crs(
            source_crs, 
            target_crs, 
            always_xy=True
        )
        
        logger.info(f"WFSGeometryProcessor başlatıldı: {source_crs} -> {target_crs}")
    
    def extract_text(self, parent: ET.Element, tag: str) -> Optional[str]:
        """
        XML elementinden güvenli metin çıkarımı.
        
        Args:
            parent: Üst XML elementi
            tag: Aranacak tag ismi
            
        Returns:
            Element metni veya None
        """
        elem = parent.find(tag, self.namespaces)
        return elem.text if elem is not None else None
    
    def parse_gml_coordinates(self, coord_text: str) -> List[Tuple[float, float]]:
        """
        GML koordinat metnini (lon,lat) tuple listesine dönüştürür.
        
        Args:
            coord_text: GML koordinat metni
            
        Returns:
            Koordinat tuple listesi [(lon, lat), ...]
        """
        coordinates = []
        try:
            for point in coord_text.strip().split():
                if point and ',' in point:
                    lon, lat = point.split(',')
                    coordinates.append((float(lon.strip()), float(lat.strip())))
            return coordinates
        except Exception as e:
            logger.error(f"Koordinat ayrıştırma hatası: {e}")
            raise ValueError(f"Geçersiz koordinat formatı: {coord_text}")
    
    def transform_to_target_crs(self, coords: List[Tuple[float, float]]) -> List[Tuple[float, float]]:
        """
        Koordinatları kaynak CRS'den hedef CRS'ye dönüştürür.
        
        Args:
            coords: Kaynak CRS'deki koordinatlar
            
        Returns:
            Hedef CRS'deki koordinatlar
        """
        try:
            return [self.transformer.transform(lon, lat) for lon, lat in coords]
        except Exception as e:
            logger.error(f"Koordinat dönüştürme hatası: {e}")
            raise
    
    def coords_to_wkt_polygon(self, coords: List[Tuple[float, float]]) -> Optional[str]:
        """
        Koordinat listesini PostGIS uyumlu WKT Polygon formatına çevirir.
        
        Args:
            coords: Koordinat listesi
            
        Returns:
            WKT Polygon string veya None
        """
        if not coords:
            return None
        
        # Koordinatları "x y" formatında string'e çevir
        coord_strings = [f"{x} {y}" for x, y in coords]
        # WKT Polygon formatı (ilk ve son nokta aynı olmalı - zaten GML'de öyle)
        return f"POLYGON(({', '.join(coord_strings)}))"
    
    def parse_wfs_xml(self, xml_content: str) -> ET.Element:
        """
        WFS XML içeriğini ayrıştırır.
        
        Args:
            xml_content: XML içerik string'i
            
        Returns:
            XML root elementi
        """
        try:
            if isinstance(xml_content, str):
                xml_content = xml_content.encode('utf-8')
            
            parser = ET.XMLParser(encoding="utf-8")
            root = ET.fromstring(xml_content, parser=parser)
            
            feature_members = root.findall('.//{http://www.opengis.net/gml}featureMember')
            logger.info(f"WFS XML başarıyla ayrıştırıldı: {len(feature_members)} feature member bulundu")
            
            return root
            
        except Exception as e:
            logger.error(f"XML ayrıştırma hatası: {e}")
            raise
    
    def process_geometry_element(self, geom_elem: ET.Element) -> Optional[Dict[str, Any]]:
        """
        TKGM:geom öğesinden geometri verilerini çıkarır ve dönüştürür.
        
        Args:
            geom_elem: TKGM:geom XML elementi
            
        Returns:
            Geometri verileri dictionary'si veya None
        """
        try:
            # GML coordinates elementini bul
            coords_elem = geom_elem.find('.//gml:coordinates', self.namespaces)
            
            if coords_elem is None or not coords_elem.text:
                logger.debug("Koordinat elementi bulunamadı veya boş")
                return None
            
            # Koordinatları ayrıştır (EPSG:4326)
            coords_4326 = self.parse_gml_coordinates(coords_elem.text)
            
            if not coords_4326:
                logger.debug("Koordinatlar ayrıştırılamadı")
                return None
            
            # EPSG:2320'ye dönüştür
            coords_2320 = self.transform_to_target_crs(coords_4326)
            
            # WKT formatına çevir
            wkt = self.coords_to_wkt_polygon(coords_2320)
            
            return {
                'geometry_type': 'Polygon',
                'original_coords': coords_4326,
                'transformed_coords': coords_2320,
                'wkt': wkt,
                'original_crs': self.source_crs,
                'target_crs': self.target_crs
            }
            
        except Exception as e:
            logger.error(f"Geometri işleme hatası: {e}")
            return None
    
    def process_parcel_feature(self, feature_member: ET.Element) -> Optional[Dict[str, Any]]:
        """
        Tek bir featureMember elemanını işler ve parsel nesnesi oluşturur.
        
        Args:
            feature_member: gml:featureMember XML elementi
            
        Returns:
            Parsel verileri dictionary'si veya None
        """
        try:
            parcel_elem = feature_member.find('TKGM:parseller', self.namespaces)
            if parcel_elem is None:
                return None
            
            # FID değerini al
            fid_full = parcel_elem.get('fid', '')
            fid = fid_full.split('.')[-1] if fid_full else ''
            
            # Geometri dönüşümü ve WKT formatına çevirme
            geom_elem = parcel_elem.find('TKGM:geom', self.namespaces)
            geometry_data = None
            
            if geom_elem is not None:
                geometry_data = self.process_geometry_element(geom_elem)
            
            # Ana veri yapısını oluştur
            result = {
                'fid': fid,
                'parselno': self.extract_text(parcel_elem, 'TKGM:parselno'),
                'adano': self.extract_text(parcel_elem, 'TKGM:adano'),
                'tapukimlikno': self.extract_text(parcel_elem, 'TKGM:tapukimlikno'),
                'tapucinsaciklama': self.extract_text(parcel_elem, 'TKGM:tapucinsaciklama'),
                'tapuzeminref': self.extract_text(parcel_elem, 'TKGM:tapuzeminref'),
                'tapumahalleref': self.extract_text(parcel_elem, 'TKGM:tapumahalleref'),
                'tapualan': self.extract_text(parcel_elem, 'TKGM:tapualan'),
                'tip': self.extract_text(parcel_elem, 'TKGM:tip'),
                'belirtmetip': self.extract_text(parcel_elem, 'TKGM:belirtmetip'),
                'durum': self.extract_text(parcel_elem, 'TKGM:durum'),
                'sistemkayittarihi': self.extract_text(parcel_elem, 'TKGM:sistemkayittarihi'),
                'onaydurum': self.extract_text(parcel_elem, 'TKGM:onaydurum'),
                'kadastroalan': self.extract_text(parcel_elem, 'TKGM:kadastroalan'),
                'tapucinsid': self.extract_text(parcel_elem, 'TKGM:tapucinsid'),
                'sistemguncellemetarihi': self.extract_text(parcel_elem, 'TKGM:sistemguncellemetarihi'),
                'kmdurum': self.extract_text(parcel_elem, 'TKGM:kmdurum'),
                'hazineparseldurum': self.extract_text(parcel_elem, 'TKGM:hazineparseldurum'),
                'terksebep': self.extract_text(parcel_elem, 'TKGM:terksebep'),
                'detayuretimyontem': self.extract_text(parcel_elem, 'TKGM:detayuretimyontem'),
                'orjinalgeomwkt': self.extract_text(parcel_elem, 'TKGM:orjinalgeomwkt'),
                'orjinalgeomkoordinatsistem': self.extract_text(parcel_elem, 'TKGM:orjinalgeomkoordinatsistem'),
                'orjinalgeomuretimyontem': self.extract_text(parcel_elem, 'TKGM:orjinalgeomuretimyontem'),
                'dom': self.extract_text(parcel_elem, 'TKGM:dom'),
                'epok': self.extract_text(parcel_elem, 'TKGM:epok'),
                'detayverikalite': self.extract_text(parcel_elem, 'TKGM:detayverikalite'),
                'orjinalgeomepok': self.extract_text(parcel_elem, 'TKGM:orjinalgeomepok'),
                'parseltescildurum': self.extract_text(parcel_elem, 'TKGM:parseltescildurum'),
                'olcuyontem': self.extract_text(parcel_elem, 'TKGM:olcuyontem'),
                'detayarsivonaylikoordinat': self.extract_text(parcel_elem, 'TKGM:detayarsivonaylikoordinat'),
                'detaypaftazeminuyumluluk': self.extract_text(parcel_elem, 'TKGM:detaypaftazeminuyumluluk'),
                'tesisislemfenkayitref': self.extract_text(parcel_elem, 'TKGM:tesisislemfenkayitref'),
                'terkinislemfenkayitref': self.extract_text(parcel_elem, 'TKGM:terkinislemfenkayitref'),
                'hesapverikalite': self.extract_text(parcel_elem, 'TKGM:hesapverikalite'),
            }
            
            # Geometri verilerini ekle
            if geometry_data:
                result.update({
                    'geometry_type': geometry_data['geometry_type'],
                    'original_coords': geometry_data['original_coords'],
                    'transformed_coords': geometry_data['transformed_coords'],
                    'wkt': geometry_data['wkt']
                })
            else:
                result.update({
                    'geometry_type': None,
                    'original_coords': [],
                    'transformed_coords': [],
                    'wkt': None
                })
            
            return result
            
        except Exception as e:
            logger.error(f"Parsel feature işleme hatası: {e}")
            return None
    
    def process_district_feature(self, feature_member: ET.Element) -> Optional[Dict[str, Any]]:
        """
        Tek bir featureMember elemanını işler ve ilçe nesnesi oluşturur.
        
        Args:
            feature_member: gml:featureMember XML elementi
            
        Returns:
            İlçe verileri dictionary'si veya None
        """
        try:
            district_elem = feature_member.find('TKGM:ilceler', self.namespaces)
            if district_elem is None:
                return None
            
            # FID değerini al
            fid_full = district_elem.get('fid', '')
            fid = fid_full.split('.')[-1] if fid_full else ''
            
            # Geometri dönüşümü ve WKT formatına çevirme
            geom_elem = district_elem.find('TKGM:geom', self.namespaces)
            geometry_data = None
            
            if geom_elem is not None:
                geometry_data = self.process_geometry_element(geom_elem)
            
            # Ana veri yapısını oluştur
            result = {
                'fid': fid,
                'tapukimlikno': self.extract_text(district_elem, 'TKGM:tapukimlikno'),
                'ilref': self.extract_text(district_elem, 'TKGM:ilref'),
                'ad': self.extract_text(district_elem, 'TKGM:ad'),
                'durum': self.extract_text(district_elem, 'TKGM:durum'),
            }
            
            # Geometri verilerini ekle
            if geometry_data:
                result.update({
                    'geometry_type': geometry_data['geometry_type'],
                    'original_coords': geometry_data['original_coords'],
                    'transformed_coords': geometry_data['transformed_coords'],
                    'wkt': geometry_data['wkt']
                })
            else:
                result.update({
                    'geometry_type': None,
                    'original_coords': [],
                    'transformed_coords': [],
                    'wkt': None
                })
            
            return result
            
        except Exception as e:
            logger.error(f"İlçe feature işleme hatası: {e}")
            return None
    
    def process_parcel_wfs_response(self, xml_content: str) -> List[Dict[str, Any]]:
        """
        TKGM WFS XML yanıtını parse eder ve EPSG:2320'de geometrileriyle birlikte parsel listesi döndürür.
        
        Args:
            xml_content: TKGM WFS XML içeriği
            
        Returns:
            İşlenmiş parsel verileri listesi
        """
        try:
            root = self.parse_wfs_xml(xml_content)
            feature_members = root.findall('.//gml:featureMember', self.namespaces)
            
            parcels = []
            for i, feature_member in enumerate(feature_members):
                parcel = self.process_parcel_feature(feature_member)
                if parcel:
                    parcels.append(parcel)
                    parsel_no = parcel.get('parselno', 'N/A')
                    coord_sys = parcel.get('orjinalgeomkoordinatsistem', 'N/A')
                    logger.info(f"✓ Parsel {i+1} işlendi: {parsel_no} (Koordinat Sistemi: {coord_sys})")
                else:
                    logger.warning(f"✗ Parsel {i+1} işlenemedi")
            
            logger.info(f"Toplam {len(parcels)} parsel başarıyla işlendi")
            return parcels
            
        except Exception as e:
            logger.error(f"WFS yanıtı işleme hatası: {e}")
            raise
    
    def process_district_wfs_response(self, xml_content: str) -> List[Dict[str, Any]]:
        """
        TKGM WFS XML yanıtını parse eder ve EPSG:2320'de geometrileriyle birlikte ilçe listesi döndürür.
        
        Args:
            xml_content: TKGM WFS XML içeriği
            
        Returns:
            İşlenmiş ilçe verileri listesi
        """
        try:
            root = self.parse_wfs_xml(xml_content)
            feature_members = root.findall('.//gml:featureMember', self.namespaces)
            
            districts = []
            for i, feature_member in enumerate(feature_members):
                district = self.process_district_feature(feature_member)
                if district:
                    districts.append(district)
                    district_name = district.get('ad', 'N/A')
                    logger.info(f"✓ İlçe {i+1} işlendi: {district_name}")
                else:
                    logger.warning(f"✗ İlçe {i+1} işlenemedi")
            
            logger.info(f"Toplam {len(districts)} ilçe başarıyla işlendi")
            return districts
            
        except Exception as e:
            logger.error(f"WFS yanıtı işleme hatası: {e}")
            raise
    
    def process_neighbourhood_feature(self, feature_member: ET.Element) -> Optional[Dict[str, Any]]:
        """
        Tek bir featureMember elemanını işler ve mahalle nesnesi oluşturur.
        
        Args:
            feature_member: gml:featureMember XML elementi
            
        Returns:
            Mahalle verileri dictionary'si veya None
        """
        try:
            neighbourhood_elem = feature_member.find('TKGM:mahalleler', self.namespaces)
            if neighbourhood_elem is None:
                return None
            
            # FID değerini al
            fid_full = neighbourhood_elem.get('fid', '')
            fid = fid_full.split('.')[-1] if fid_full else ''
            
            # Geometri dönüşümü ve WKT formatına çevirme
            geom_elem = neighbourhood_elem.find('TKGM:geom', self.namespaces)
            geometry_data = None
            
            if geom_elem is not None:
                geometry_data = self.process_geometry_element(geom_elem)
            
            # Ana veri yapısını oluştur
            result = {
                'fid': fid,
                'ilceref': self.extract_text(neighbourhood_elem, 'TKGM:ilceref'),
                'tapukimlikno': self.extract_text(neighbourhood_elem, 'TKGM:tapukimlikno'),
                'durum': self.extract_text(neighbourhood_elem, 'TKGM:durum'),
                'sistemkayittarihi': self.extract_text(neighbourhood_elem, 'TKGM:sistemkayittarihi'),
                'tip': self.extract_text(neighbourhood_elem, 'TKGM:tip'),
                'tapumahallead': self.extract_text(neighbourhood_elem, 'TKGM:tapumahallead'),
                'kadastromahallead': self.extract_text(neighbourhood_elem, 'TKGM:kadastromahallead'),
            }
            
            # Geometri verilerini ekle
            if geometry_data:
                result.update({
                    'geometry_type': geometry_data['geometry_type'],
                    'original_coords': geometry_data['original_coords'],
                    'transformed_coords': geometry_data['transformed_coords'],
                    'wkt': geometry_data['wkt']
                })
            else:
                result.update({
                    'geometry_type': None,
                    'original_coords': [],
                    'transformed_coords': [],
                    'wkt': None
                })
            
            return result
            
        except Exception as e:
            logger.error(f"Mahalle feature işleme hatası: {e}")
            return None
    
    def process_neighbourhood_wfs_response(self, xml_content: str) -> List[Dict[str, Any]]:
        """
        TKGM WFS XML yanıtını parse eder ve EPSG:2320'de geometrileriyle birlikte mahalle listesi döndürür.
        
        Args:
            xml_content: TKGM WFS XML içeriği
            
        Returns:
            İşlenmiş mahalle verileri listesi
        """
        try:
            root = self.parse_wfs_xml(xml_content)
            feature_members = root.findall('.//gml:featureMember', self.namespaces)
            
            neighbourhoods = []
            for i, feature_member in enumerate(feature_members):
                neighbourhood = self.process_neighbourhood_feature(feature_member)
                if neighbourhood:
                    neighbourhoods.append(neighbourhood)
                    mahalle_name = neighbourhood.get('tapumahallead') or neighbourhood.get('kadastromahallead', 'N/A')
                    logger.info(f"✓ Mahalle {i+1} işlendi: {mahalle_name}")
                else:
                    logger.warning(f"✗ Mahalle {i+1} işlenemedi")
            
            logger.info(f"Toplam {len(neighbourhoods)} mahalle başarıyla işlendi")
            return neighbourhoods
            
        except Exception as e:
            logger.error(f"WFS yanıtı işleme hatası: {e}")
            raise


def generate_sql_insert(items: List[Dict[str, Any]], table_name: str = 'tk_parsel') -> str:
    """
    Parsel/İlçe verilerini PostGIS'e eklemek için SQL INSERT ifadeleri üretir.
    
    Args:
        items: İşlenmiş parsel veya ilçe listesi
        table_name: Hedef PostgreSQL tablosu
        
    Returns:
        SQL INSERT ifadeleri (SRID=2320)
    """
    sql_statements = []
    
    for item in items:
        # 'wkt' dışındaki sütunları al
        columns = [k for k in item.keys() if k not in ['wkt', 'geometry_type', 'original_coords', 'transformed_coords']]
        
        # Değerleri hazırla
        values = []
        for col in columns:
            v = item.get(col)
            if v is None:
                values.append('NULL')
            else:
                # Escape single quotes
                escaped_value = str(v).replace("'", "''")
                values.append(f"'{escaped_value}'")
        
        # Geom için PostGIS fonksiyonu (SRID=2320)
        wkt = item.get('wkt')
        geom_sql = f"ST_GeomFromText('{wkt}', 2320)" if wkt else 'NULL'
        
        # SQL oluştur
        columns_str = ', '.join(columns)
        values_str = ', '.join(values)
        sql = f"INSERT INTO {table_name} ({columns_str}, geom) VALUES ({values_str}, {geom_sql});"
        sql_statements.append(sql)
    
    return '\n'.join(sql_statements)


def parse_tkgm_xml_file(file_path: str, feature_type: str = 'parsel') -> List[Dict[str, Any]]:
    """
    TKGM WFS XML dosyasını parse eder ve EPSG:2320'de geometrileriyle birlikte veri listesi döndürür.
    
    Args:
        file_path: TKGM WFS XML dosyasının yolu
        feature_type: 'parsel' veya 'district'
        
    Returns:
        İşlenmiş veri listesi
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            xml_content = f.read()
        
        processor = WFSGeometryProcessor()
        
        if feature_type == 'parsel':
            return processor.process_parcel_wfs_response(xml_content)
        elif feature_type == 'district':
            return processor.process_district_wfs_response(xml_content)
        else:
            raise ValueError(f"Geçersiz feature_type: {feature_type}. 'parsel' veya 'district' olmalı.")
            
    except Exception as e:
        logger.error(f"XML dosyası işleme hatası ({file_path}): {e}")
        return []
