"""Log Explorer - Log Sorgu ve Analiz Modülü

tk_logs tablosundaki response_xml verilerini parsel bilgilerine göre arar,
eşleşen parselleri XML'den çıkarır ve okunabilir formatta gösterir.
"""

import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Dict, List, Optional
from loguru import logger

from src.database import DatabaseManager


class LogExplorer:
    """Log kayıtlarında parsel verisi arama ve görüntüleme"""

    NAMESPACES = {
        'gml': 'http://www.opengis.net/gml',
        'wfs': 'http://www.opengis.net/wfs',
        'TKGM': 'http://www.tkgm.gov.tr'
    }

    # Tablo görünümünde gösterilecek özet alanlar
    PARCEL_TABLE_FIELDS = [
        'adano', 'parselno', 'tapukimlikno', 'durum',
        'tapualan', 'sistemguncellemetarihi'
    ]

    # featureMember altındaki tüm önemli alanlar (detay kartı için)
    PARCEL_ALL_FIELDS = [
        'fid', 'adano', 'parselno', 'tapukimlikno', 'tapucinsaciklama',
        'tapuzeminref', 'tapumahalleref', 'tapualan', 'tip', 'belirtmetip',
        'durum', 'onaydurum', 'kadastroalan', 'tapucinsid', 'kmdurum',
        'hazineparseldurum', 'terksebep', 'sistemkayittarihi',
        'sistemguncellemetarihi', 'detayuretimyontem', 'parseltescildurum',
        'olcuyontem'
    ]

    def __init__(self, db: DatabaseManager):
        self.db = db

    def extract_matching_parcels(
        self,
        response_xml: str,
        adano: str = None,
        parselno: str = None,
        tapukimlikno: int = None,
        tapumahalleref: int = None,
        durum: str = None
    ) -> List[Dict[str, str]]:
        """response_xml içinden filtreye uyan parselleri çıkar (tüm alanlarıyla)"""
        try:
            root = ET.fromstring(response_xml)
        except ET.ParseError as e:
            logger.warning(f"XML parse hatası, atlanıyor: {e}")
            return []

        feature_members = root.findall('.//gml:featureMember', self.NAMESPACES)
        results = []

        for fm in feature_members:
            parcel_elem = fm.find('TKGM:parseller', self.NAMESPACES)
            if parcel_elem is None:
                continue

            # FID
            fid_full = parcel_elem.get('fid', '')
            fid = fid_full.split('.')[-1] if fid_full else ''

            # Tüm alanları çıkar
            parcel = {'fid': fid}
            for field in self.PARCEL_ALL_FIELDS:
                if field == 'fid':
                    continue
                elem = parcel_elem.find(f'TKGM:{field}', self.NAMESPACES)
                parcel[field] = elem.text if elem is not None else None

            # Filtreleme
            if adano is not None and parcel.get('adano') != str(adano):
                continue
            if parselno is not None and parcel.get('parselno') != str(parselno):
                continue
            if tapukimlikno is not None and parcel.get('tapukimlikno') != str(tapukimlikno):
                continue
            if tapumahalleref is not None and parcel.get('tapumahalleref') != str(tapumahalleref):
                continue
            if durum is not None and parcel.get('durum') != str(durum):
                continue

            results.append(parcel)

        return results

    def _format_duration(self, duration) -> str:
        """INTERVAL veya timedelta formatla"""
        if duration is None:
            return "-"
        total_seconds = duration.total_seconds()
        return f"{total_seconds:.1f}s"

    def _format_size(self, size: int) -> str:
        """Bayt değerini okunabilir formata çevir"""
        if size is None:
            return "-"
        if size >= 1_000_000:
            return f"{size / 1_000_000:.1f} MB"
        if size >= 1_000:
            return f"{size / 1_000:.1f} KB"
        return f"{size} B"

    def _print_parcel_table(self, parcels: List[Dict[str, str]]):
        """Parselleri özet tablo formatında yazdır"""
        if not parcels:
            print("    (Eşleşen parsel bulunamadı)")
            return

        headers = {
            'adano': 'Ada No',
            'parselno': 'Parsel No',
            'tapukimlikno': 'Tapu Kimlik No',
            'durum': 'Durum',
            'tapualan': 'Alan',
            'sistemguncellemetarihi': 'Güncelleme Tarihi'
        }

        # Sütun genişliklerini hesapla
        widths = {}
        for key, header in headers.items():
            max_val = max((len(str(p.get(key) or '-')) for p in parcels), default=0)
            widths[key] = max(len(header), max_val)

        # Header satırı
        header_line = "  ".join(h.ljust(widths[k]) for k, h in headers.items())
        separator = "  ".join("-" * widths[k] for k in headers)
        print(f"    {header_line}")
        print(f"    {separator}")

        # Veri satırları
        for p in parcels:
            row = "  ".join(
                str(p.get(k) or '-').ljust(widths[k])
                for k in headers
            )
            print(f"    {row}")

    def _print_parcel_detail_cards(self, parcels: List[Dict[str, str]]):
        """Her eşleşen parselin tüm alanlarını kart formatında yazdır"""
        if not parcels:
            print("    (Eşleşen parsel bulunamadı)")
            return

        field_labels = {
            'fid': 'FID',
            'adano': 'Ada No',
            'parselno': 'Parsel No',
            'tapukimlikno': 'Tapu Kimlik No',
            'tapucinsaciklama': 'Tapu Cins Açıklama',
            'tapuzeminref': 'Tapu Zemin Ref',
            'tapumahalleref': 'Tapu Mahalle Ref',
            'tapualan': 'Tapu Alan',
            'tip': 'Tip',
            'belirtmetip': 'Belirtme Tip',
            'durum': 'Durum',
            'onaydurum': 'Onay Durum',
            'kadastroalan': 'Kadastro Alan',
            'tapucinsid': 'Tapu Cins ID',
            'kmdurum': 'KM Durum',
            'hazineparseldurum': 'Hazine Parsel Durum',
            'terksebep': 'Terk Sebep',
            'sistemkayittarihi': 'Kayıt Tarihi',
            'sistemguncellemetarihi': 'Güncelleme Tarihi',
            'detayuretimyontem': 'Detay Üretim Yöntem',
            'parseltescildurum': 'Parsel Tescil Durum',
            'olcuyontem': 'Ölçü Yöntem',
        }

        max_label_len = max(len(v) for v in field_labels.values())

        for i, parcel in enumerate(parcels, 1):
            print(f"\n    -- Parsel {i} --")
            for field_key in self.PARCEL_ALL_FIELDS:
                label = field_labels.get(field_key, field_key)
                value = parcel.get(field_key) or '-'
                print(f"    {label.ljust(max_label_len)}  : {value}")

    def search_and_display(
        self,
        adano: str = None,
        parselno: str = None,
        tapukimlikno: int = None,
        tapumahalleref: int = None,
        durum: str = None,
        date_from: str = None,
        date_to: str = None,
        limit: int = 10
    ):
        """Log kayıtlarında parsel ara ve sonuçları göster

        SQL LIKE ile aday loglar çekilir, ardından Python'da XML parse
        edilerek gerçek kombinasyon eşleşmesi yapılır. Eşleşmeyen loglar
        sonuçtan elenir — sadece gerçek eşleşme olan loglar gösterilir.
        """

        # Tarih string'lerini datetime'a çevir
        dt_from = None
        dt_to = None
        if date_from:
            try:
                dt_from = datetime.strptime(date_from, '%Y-%m-%d')
            except ValueError:
                print(f"Hata: Geçersiz başlangıç tarihi formatı: {date_from} (YYYY-MM-DD olmalı)")
                return
        if date_to:
            try:
                dt_to = datetime.strptime(date_to, '%Y-%m-%d').replace(hour=23, minute=59, second=59)
            except ValueError:
                print(f"Hata: Geçersiz bitiş tarihi formatı: {date_to} (YYYY-MM-DD olmalı)")
                return

        # Arama kriterlerini göster
        criteria = []
        if adano is not None:
            criteria.append(f"adano={adano}")
        if parselno is not None:
            criteria.append(f"parselno={parselno}")
        if tapukimlikno is not None:
            criteria.append(f"tapukimlikno={tapukimlikno}")
        if tapumahalleref is not None:
            criteria.append(f"tapumahalleref={tapumahalleref}")
        if durum is not None:
            criteria.append(f"durum={durum}")
        if dt_from:
            criteria.append(f"tarih>={date_from}")
        if dt_to:
            criteria.append(f"tarih<={date_to}")

        print("\n" + "=" * 60)
        print("         LOG SORGU SONUCLARI")
        print("=" * 60)
        print(f"\nArama Kriterleri: {', '.join(criteria)}")

        # SQL LIKE ile aday logları çek (fazla gelebilir)
        # Gerçek eşleşme Python'da XML parse ile yapılacak
        fetch_limit = limit * 5  # Aday havuzunu geniş tut
        candidate_logs = self.db.search_logs_by_parcel(
            adano=adano,
            parselno=parselno,
            tapukimlikno=tapukimlikno,
            tapumahalleref=tapumahalleref,
            durum=durum,
            date_from=dt_from,
            date_to=dt_to,
            limit=fetch_limit
        )

        if not candidate_logs:
            print("Sonuç bulunamadı.")
            print("=" * 60 + "\n")
            return

        # Aday logları filtrele: sadece gerçek eşleşme olanları al
        matched_logs = []
        for log in candidate_logs:
            response_xml = log.get('response_xml')
            if not response_xml:
                continue
            matching = self.extract_matching_parcels(
                response_xml,
                adano=adano,
                parselno=parselno,
                tapukimlikno=tapukimlikno,
                tapumahalleref=tapumahalleref,
                durum=durum
            )
            if matching:
                matched_logs.append((log, matching))
                if len(matched_logs) >= limit:
                    break

        if not matched_logs:
            print("Sonuç bulunamadı.")
            print("=" * 60 + "\n")
            return

        print(f"Bulunan Log Sayısı: {len(matched_logs)}")
        print("=" * 60)

        total_matching = 0

        for idx, (log, matching) in enumerate(matched_logs, 1):
            total_matching += len(matching)
            print(f"\n--- Log #{idx} (ID: {log['id']}) ---")
            print(f"  Tarih          : {log['query_time']}")
            print(f"  Typename       : {log['typename']}")
            print(f"  Feature Count  : {log['feature_count']}")
            print(f"  Response Size  : {self._format_size(log.get('response_size'))}")
            print(f"  Sorgu Suresi   : {self._format_duration(log.get('execution_duration'))}")
            print(f"  Basarili       : {'Evet' if log.get('is_successful') else 'Hayir'}")

            # URL'yi kısalt
            url = log.get('url', '')
            if len(url) > 100:
                print(f"  URL            : {url[:100]}...")
            else:
                print(f"  URL            : {url}")

            print(f"\n  Eslesen Parseller ({len(matching)}):")
            self._print_parcel_table(matching)
            print(f"\n  Detaylar:")
            self._print_parcel_detail_cards(matching)

        print(f"\n{'=' * 60}")
        print(f"Toplam: {len(matched_logs)} log icinde {total_matching} eslesen parsel")
        print("=" * 60 + "\n")

    def show_log_detail(self, log_id: int):
        """Belirli bir log kaydının detayını göster"""

        log = self.db.get_log_by_id(log_id)

        if not log:
            print(f"\nHata: ID={log_id} olan log kaydı bulunamadı.\n")
            return

        print("\n" + "=" * 60)
        print(f"         LOG DETAYI (ID: {log_id})")
        print("=" * 60)
        print(f"  Tarih          : {log['query_time']}")
        print(f"  Typename       : {log['typename']}")
        print(f"  Feature Count  : {log['feature_count']}")
        print(f"  Response Size  : {self._format_size(log.get('response_size'))}")
        print(f"  Sorgu Suresi   : {self._format_duration(log.get('execution_duration'))}")
        print(f"  Basarili       : {'Evet' if log.get('is_successful') else 'Hayir'}")
        print(f"  Bos Yanit      : {'Evet' if log.get('is_empty') else 'Hayir'}")
        print(f"  HTTP Durum     : {log.get('http_status_code', '-')}")
        print(f"  URL            : {log.get('url', '-')}")

        if log.get('error_message'):
            print(f"  Hata Mesaji    : {log['error_message']}")

        if log.get('notes'):
            print(f"  Notlar         : {log['notes']}")

        # XML içindeki tüm parselleri göster
        response_xml = log.get('response_xml')
        if response_xml:
            all_parcels = self.extract_matching_parcels(response_xml)
            print(f"\n  Icerikteki Tum Parseller ({len(all_parcels)}):")
            self._print_parcel_table(all_parcels)
        else:
            print("\n  (response_xml verisi yok)")

        print("=" * 60 + "\n")
