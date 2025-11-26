# TKGM Veritabanı Senkronizasyonu

Bu dizin TKGM verilerinin farklı veritabanlarına senkronize edilmesi için gerekli scriptleri içerir.

## 📄 Dosyalar

- **sync-oracle.sh**: PostgreSQL'den Oracle'a TKGM verisi aktarımı
- **sync-postgresql.sh**: PostgreSQL'den PostgreSQL'e TKGM verisi aktarımı
- **crontab**: Otomatik çalışma zamanları (cron jobs)

## 🚀 Kullanım

### Manuel Çalıştırma

```bash
# Oracle sync
./scripts/sync-oracle.sh

# PostgreSQL sync
./scripts/sync-postgresql.sh

# Bağlantı testi
./scripts/sync-oracle.sh --test
./scripts/sync-postgresql.sh --test

# SQL sorgusunu görüntüleme
./scripts/sync-oracle.sh --dry-run
./scripts/sync-postgresql.sh --dry-run
```

### Cron ile Otomatik Çalıştırma

Crontab dosyası her iki sync işlemini de **günde 1 kez saat 20:00'de** çalıştıracak şekilde yapılandırılmıştır.

#### Docker Container İçinde

Docker container'ı başlatırken cron otomatik olarak yapılandırılır. `entrypoint.sh` veya `Dockerfile` içinde şu komutları ekleyin:

```bash
# Crontab'ı yükle
crontab /app/scripts/crontab

# Cron servisini başlat
service cron start
```

#### Manuel Kurulum

```bash
# Crontab'ı yükle
crontab scripts/crontab

# Mevcut crontab'ı görüntüle
crontab -l

# Cron loglarını kontrol et
tail -f /app/logs/cron-oracle.log
tail -f /app/logs/cron-postgresql.log
```

## ⚙️ Konfigürasyon

Tüm ayarlar `.env` dosyasından okunur. `.env.example` dosyasını `.env` olarak kopyalayıp düzenleyin:

```bash
cp .env.example .env
```

### Oracle Ayarları

```bash
ORACLE_TARGET_HOST=your_oracle_host
ORACLE_TARGET_PORT=1521
ORACLE_TARGET_SERVICE_NAME=ORCL
ORACLE_TARGET_USER=cadastral
ORACLE_TARGET_PASS=your_password
ORACLE_TARGET_TABLE=TK_PARSEL
```

### PostgreSQL Source (Kaynak) Ayarları

```bash
POSTGRES_SOURCE_HOST=localhost
POSTGRES_SOURCE_PORT=5432
POSTGRES_SOURCE_DB=cadastral_db
POSTGRES_SOURCE_USER=postgres
POSTGRES_SOURCE_PASS=password
```

### PostgreSQL Target (Hedef) Ayarları

```bash
POSTGRES_TARGET_HOST=target_host
POSTGRES_TARGET_PORT=5433
POSTGRES_TARGET_DB=cadastral_target_db
POSTGRES_TARGET_USER=postgres
POSTGRES_TARGET_PASS=target_password
POSTGRES_TARGET_TABLE=tk_parsel
```

## 📋 Özellikler

### sync-oracle.sh
- PostgreSQL'den Oracle'a veri aktarımı
- Tablo varsa TRUNCATE, yoksa CREATE
- Spatial index otomatiği
- İstatistik güncelleme (DBMS_STATS)
- Detaylı loglama

### sync-postgresql.sh
- PostgreSQL'den PostgreSQL'e veri aktarımı
- Tablo varsa TRUNCATE, yoksa CREATE
- GIST spatial index otomatiği
- VACUUM ANALYZE ile istatistik güncelleme
- Detaylı loglama

### Ortak Özellikler
- Bağlantı testleri
- Veri doğrulama
- Progress tracking
- Hata yönetimi
- Timestamp'li log dosyaları

## 📊 Log Yönetimi
### Merkezi Log Dosyaları

Yeni loglama sisteminde her işlem tipi için **tek bir merkezi log dosyası** kullanılır:

```bash
# Ana log dosyaları
/app/logs/cron_oracle.log          # Oracle senkronizasyon logları
/app/logs/cron_postgresql.log      # PostgreSQL senkronizasyon logları
/app/logs/scraper.log               # Python scraper logları
```

### Self-Cleanup (Kendi Kendini Temizleme)

Her script (`sync-oracle.sh`, `sync-postgresql.sh`) kendi log dosyasını yönetir:

- **Kontrol Zamanı**: Script çalışması bittiğinde
- **Maksimum Boyut**: 100MB
- **Tutulacak Boyut**: 50MB
- **Mantık**: Boyut aşıldığında dosyanın son 50MB'lık kısmı tutulur, gerisi silinir.

### Log Görüntüleme

```bash
# Son 100 satırı görüntüle
tail -n 100 /app/logs/cron_oracle.log

# Canlı takip et
tail -f /app/logs/cron_oracle.log

# Belirli bir tarih aralığı
grep "2025-11-26" /app/logs/cron_oracle.log

# Sadece hataları göster
grep "ERROR" /app/logs/cron_oracle.log
```

## 🔧 Bağımlılıklar

- `ogr2ogr` (GDAL)
- `psql` (PostgreSQL client)
- `sqlplus` (Oracle Instant Client) - sadece Oracle sync için
- `bash`

## ⏰ Cron Zamanlaması

```bash
# Oracle senkronizasyonu - Her gün 20:00
0 20 * * * /app/scripts/sync-oracle.sh

# PostgreSQL senkronizasyonu - Her gün 20:00
0 20 * * * /app/scripts/sync-postgresql.sh
```

### Farklı Saatler İçin

Cron zamanlamasını değiştirmek için `scripts/crontab` dosyasını düzenleyin:

```bash
# Dakika Saat Gün Ay Haftanın_Günü Komut
0 20 * * * /app/scripts/sync-oracle.sh        # Her gün 20:00
0 8 * * * /app/scripts/sync-oracle.sh         # Her gün 08:00
0 */6 * * * /app/scripts/sync-oracle.sh       # Her 6 saatte bir
0 0 * * 1 /app/scripts/sync-oracle.sh         # Her Pazartesi 00:00
```

## 🐛 Sorun Giderme

### Bağlantı Hatası

```bash
# Test komutu ile kontrol edin
./scripts/sync-oracle.sh --test
./scripts/sync-postgresql.sh --test
```

### Cron Çalışmıyor

```bash
# Cron servisini kontrol edin
service cron status

# Crontab yüklü mü kontrol edin
crontab -l

# Cron loglarını kontrol edin
grep CRON /var/log/syslog
```

### OGR2OGR Hatası

```bash
# GDAL sürümünü kontrol edin
ogr2ogr --version

# Dry-run ile SQL'i kontrol edin
./scripts/sync-oracle.sh --dry-run
```

## 📝 Notlar

- Her iki sync de aynı anda çalışabilir
- Loglar otomatik olarak rotate edilir
- Spatial index'ler otomatik oluşturulur
- SRID 2320 (ED50 / TM30) kullanılır
- Batch size: 65536 kayıt
