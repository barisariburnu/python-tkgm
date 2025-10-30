#!/bin/bash

# Cron servisini başlat
echo "Starting cron service..."

# Cron servisini başlat
service cron start

# Cron konfigürasyonunu yükle
crontab /etc/cron.d/tkgm

# Log dosyasını oluştur ve takip et
touch /app/logs/cron.log

# Servis durumunu kontrol et
echo "Cron service status:"
service cron status

echo "Cron jobs:"
crontab -l

# Log dosyasını takip et (container'ı çalışır durumda tutar)
tail -f /app/logs/cron.log