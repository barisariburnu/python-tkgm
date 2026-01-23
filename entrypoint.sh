#!/bin/bash

# Log dizinini oluştur
mkdir -p /app/logs

echo "Starting TKGM Scheduler Service..."
echo "[$(date '+%Y-%m-%d %H:%M:%S')] [INFO] TKGM Container started (Scheduler Mode)"

# Scheduler'ı başlat
# exec kullanarak python process'ini ana process yapıyoruz (PID 1)
exec python3 /app/run_scheduler.py