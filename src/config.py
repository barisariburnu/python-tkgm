"""
Centralized Configuration Management with Pydantic Settings

Type-safe, validated configuration with automatic .env loading.
"""

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional


class Settings(BaseSettings):
    """Application settings with type hints and validation"""
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore"
    )
    
    # PostgreSQL Configuration
    POSTGRES_HOST: str = Field(..., description="PostgreSQL host address")
    POSTGRES_PORT: int = Field(default=5432, ge=1, le=65535, description="PostgreSQL port")
    POSTGRES_DB: str = Field(..., description="Database name")
    POSTGRES_USER: str = Field(..., description="Database user")
    POSTGRES_PASS: str = Field(..., description="Database password")
    
    # Oracle Configuration (Optional - for sync script)
    ORACLE_HOST: Optional[str] = Field(default=None, description="Oracle host address")
    ORACLE_PORT: Optional[int] = Field(default=1521, ge=1, le=65535, description="Oracle port")
    ORACLE_SERVICE: Optional[str] = Field(default=None, description="Oracle service name")
    ORACLE_USER: Optional[str] = Field(default=None, description="Oracle user")
    ORACLE_PASS: Optional[str] = Field(default=None, description="Oracle password")
    
    # TKGM Service Configuration
    TKGM_BASE_URL: str = Field(
        default="https://cbsservis.tkgm.gov.tr/tkgm.ows/wfs",
        description="TKGM WFS service URL"
    )
    TKGM_USERNAME: str = Field(..., description="TKGM service username")
    TKGM_PASSWORD: str = Field(..., description="TKGM service password")
    
    # Service Parameters
    MAX_FEATURES: int = Field(default=1000, ge=1, le=10000, description="Max features per request")
    MAX_RETRIES: int = Field(default=10, ge=1, le=20, description="Max retry attempts")
    RETRY_DELAY: int = Field(default=30, ge=1, le=300, description="Retry delay in seconds")
    
    # Logging Configuration
    LOG_LEVEL: str = Field(default="INFO", description="Log level")
    LOG_FILE: str = Field(default="logs/scraper.log", description="Log file path")

    # Telegram Notification (Optional)
    TELEGRAM_BOT_TOKEN: Optional[str] = Field(default=None, description="Telegram bot token")
    TELEGRAM_CHAT_ID: Optional[str] = Field(default=None, description="Telegram chat/group/channel ID")
    TELEGRAM_PARSE_MODE: Optional[str] = Field(default=None, description="Telegram parse mode (Markdown, HTML)")
    
    @field_validator("LOG_LEVEL")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        """Validate log level"""
        valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        v = v.upper()
        if v not in valid_levels:
            raise ValueError(f"Log level must be one of {valid_levels}")
        return v
    
    @field_validator("TKGM_BASE_URL")
    @classmethod
    def validate_url(cls, v: str) -> str:
        """Validate URL format"""
        if not v.startswith(("http://", "https://")):
            raise ValueError("TKGM_BASE_URL must start with http:// or https://")
        return v


# Singleton instance - import this everywhere
settings = Settings()
