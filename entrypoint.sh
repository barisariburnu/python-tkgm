#!/bin/bash

# Cron servisini başlat
echo "Starting cron service..."

# Cron servisini başlat
service cron start

# Cron konfigürasyonunu yükle
crontab /etc/cron.d/tkgm

# Log dosyasını oluştur
touch /app/logs/app.log

# Başlangıç bilgisini logla
echo "[$(date '+%Y-%m-%d %H:%M:%S')] [INFO] TKGM Container started" >> /app/logs/app.log

# Servis durumunu kontrol et
echo "Cron service status:"
service cron status

echo "Cron jobs:"
crontab -l

# Log dosyasını takip et (container'ı çalışır durumda tutar)
tail -f /app/logs/app.log