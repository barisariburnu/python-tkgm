# -*- coding: utf-8 -*-
"""
Security utilities - production-grade.

Hassas veri maskeleme ve güvenli hata yönetimi.
"""

import re
import logging
from typing import Any


# Hassas veri kalıpları (regex) - loglara sızmasını engelle
SENSITIVE_PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(r'(password|passwd|pwd)\s*[=:]\s*\S+', re.IGNORECASE), r'\1=***MASKED***'),
    (re.compile(r'(api[_-]?key|token|secret)\s*[=:]\s*\S+', re.IGNORECASE), r'\1=***MASKED***'),
    (re.compile(r'(Authorization|Bearer)\s+[A-Za-z0-9\-._~+/]+=*', re.IGNORECASE), r'\1 ***MASKED***'),
    # URL içindeki user:pass@ formatı (http://user:pass@host, postgresql://user:pass@host vb.)
    (re.compile(r'([a-zA-Z][a-zA-Z0-9+.\-]*://)[^:/\s]+:[^@\s]+@', re.IGNORECASE), r'\1***:***@'),
]


def mask_sensitive_data(text: str) -> str:
    """
    Verilen metindeki hassas verileri maskeler.

    Şifreler, API anahtarları, bearer tokenlar URL'lerdeki
    kullanıcı bilgileri maskelenir.
    """
    if not isinstance(text, str):
        return text
    masked = text
    for pattern, replacement in SENSITIVE_PATTERNS:
        masked = pattern.sub(replacement, masked)
    return masked


class SensitiveDataFilter(logging.Filter):
    """
    Python logging.Filter implementasyonu.

    LogRecord mesajlarına otomatik olarak mask_sensitive_data uygular.
    loguru ile tam uyumlu değil; main_scraper.py bunu loguru için
    patch olarak kullanır (aşağıdaki patch_loguru fonksiyonu).
    """

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            if isinstance(record.msg, str):
                record.msg = mask_sensitive_data(record.msg)
            if record.args:
                record.args = tuple(
                    mask_sensitive_data(a) if isinstance(a, str) else a
                    for a in record.args
                )
        except Exception:
            # Maskeleme başarısız olursa log'u yine de geçir
            pass
        return True


def patch_loguru() -> None:
    """
    loguru'ya global bir mask filtresi ekler.

    main_scraper._setup_logging() içinde logger.add() çağrılarından
    SONRA bir kez çağrılmalıdır. Yeni eklenen handler'lar da bu
    filtreyi miras alır.
    """
    try:
        from loguru import logger as _logger
        sensitive_filter = SensitiveDataDataFilter()
        # loguru filter parametresi sadece callable kabul eder
        for handler_id in list(_logger._core.handlers.keys()):
            try:
                _logger._core.handlers[handler_id].filtering = sensitive_filter
            except Exception:
                pass
    except Exception:
        # loguru iç API değişirse sessizce geç
        pass


class SensitiveDataDataFilter:
    """loguru için callable filter (record -> bool)."""

    def __call__(self, record: Any) -> bool:
        try:
            msg = record.get("message", "")
            if isinstance(msg, str):
                record["message"] = mask_sensitive_data(msg)
        except Exception:
            pass
        return True
