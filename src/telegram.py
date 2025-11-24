"""
Telegram Notification Module

İstatistik sonuçlarını Telegram grubu/kanalına göndermek için yardımcı sınıf.
"""

from typing import Optional, Dict, Any
from datetime import datetime
import requests
from loguru import logger
from .config import settings


class TelegramNotifier:
    """Telegram bildirimlerini yöneten sınıf"""

    def __init__(
        self,
        token: Optional[str] = None,
        chat_id: Optional[str] = None,
        parse_mode: Optional[str] = None,
    ) -> None:
        self.token = token or settings.TELEGRAM_BOT_TOKEN
        self.chat_id = chat_id or settings.TELEGRAM_CHAT_ID
        self.parse_mode = parse_mode or settings.TELEGRAM_PARSE_MODE
        self.base_url = f"https://api.telegram.org/bot{self.token}" if self.token else None

    def is_configured(self) -> bool:
        if not self.token:
            logger.warning("Telegram bot token eksik. .env içinde TELEGRAM_BOT_TOKEN tanımlayın.")
            return False
        if not self.chat_id:
            logger.warning("Telegram chat ID eksik. .env içinde TELEGRAM_CHAT_ID tanımlayın.")
            return False
        return True

    def send_message(self, text: str) -> bool:
        """Telegram'a metin mesajı gönderir"""
        if not self.is_configured():
            return False

        try:
            url = f"{self.base_url}/sendMessage"
            payload = {
                "chat_id": self.chat_id,
                "text": text,
                "disable_web_page_preview": True,
            }
            if self.parse_mode in {"Markdown", "MarkdownV2", "HTML"}:
                payload["parse_mode"] = self.parse_mode

            resp = requests.post(url, json=payload, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            if data.get("ok"):
                logger.info("Telegram mesajı başarıyla gönderildi")
                return True
            else:
                logger.error(f"Telegram API hatası: {data}")
                return False
        except Exception as e:
            logger.error(f"Telegram mesaj gönderimi sırasında hata: {e}")
            return False

    @staticmethod
    def format_stats_message(stats: Dict[str, Any]) -> str:
        """İstatistik sözlüğünü okunabilir bir Telegram mesajına dönüştürür"""
        total_parcels = stats.get("total_parcels", 0)
        parcels_today = stats.get("parcels_today", 0)
        parcels_last_week = stats.get("parcels_last_week", 0)
        total_area = stats.get("total_area", 0.0)

        date_range = stats.get("date_range", {})
        min_date = date_range.get("min_date")
        max_date = date_range.get("max_date")

        total_districts = stats.get("total_districts", 0)
        total_neighbourhoods = stats.get("total_neighbourhoods", 0)

        total_queries = stats.get("total_queries", 0)
        queries_today = stats.get("queries_today", 0)
        avg_features = stats.get("avg_features_per_query", 0.0)

        last_update = stats.get("last_update")

        current_settings = stats.get("current_settings", {})
        cs_query_date = current_settings.get("query_date")
        cs_start_index = current_settings.get("start_index", 0)
        cs_last_updated = current_settings.get("last_updated")

        lines = [
            "📣 TKGM Veritabanı İstatistikleri\n",
            "",
            "📊 Parsel İstatistikleri\n",
            f"• Toplam Parsel: {total_parcels:,}",
            f"• Bugün Eklenen: {parcels_today:,}",
            f"• Son 7 Gün: {parcels_last_week:,}",
            f"• Toplam Alan (m²): {total_area:,.2f}",
        ]

        if min_date and max_date:
            lines.append(f"• Tarih Aralığı: {min_date} - {max_date}")

        lines += [
            "",
            "🏘️ Diğer Veriler\n",
            f"• İlçe Sayısı: {total_districts:,}",
            f"• Mahalle Sayısı: {total_neighbourhoods:,}",
            "\n",
            "🔍 Sorgu İstatistikleri\n",
            f"• Toplam Sorgu: {total_queries:,}",
            f"• Bugün Sorgu: {queries_today:,}",
            f"• Ortalama Sonuç/Sorgu: {avg_features:.1f}",
        ]

        if last_update:
            lines += [
                "",
                "⚙️ Sistem",
                f"• Son Güncelleme: {last_update}",
            ]

        if cs_query_date or cs_last_updated or cs_start_index:
            lines += [
                "",
                "📋 Mevcut Ayarlar\n",
                f"• Sorgu Tarihi: {cs_query_date}" if cs_query_date else "",
                f"• Başlangıç İndeksi: {cs_start_index}",
                f"• Ayar Güncelleme: {cs_last_updated}" if cs_last_updated else "",
            ]

        # Boş satırları filtrele
        lines = [ln for ln in lines if ln]
        return "\n".join(lines)

    def send_stats(self, stats: Dict[str, Any]) -> bool:
        """İstatistik sözlüğünü Telegram'a gönderir"""
        message = self.format_stats_message(stats)
        return self.send_message(message)

    def format_pull_report(
        self,
        date: datetime,
        start_index: int,
        end_index: int,
        found: int,
        saved: int,
        unsaved: int,
    ) -> str:
        """Tek servis çekimi sonrası kayıt raporu mesajını üretir"""
        lines = [
            "📦 Servis Çekim Raporu\n",
            f"• Tarih: {date.strftime('%Y-%m-%d')}",
            f"• Sayfa: {start_index} - {end_index}",
            f"• Bulunan Parsel: {found:,}",
            f"• Kaydedilen Parsel: {saved:,}",
            f"• Kaydedilemeyen Parsel: {unsaved:,}",
        ]
        return "\n".join(lines)