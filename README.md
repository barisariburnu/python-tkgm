# TKGM WFS Veri Tarayıcısı (python-tkgm)

TKGM WFS servisinden parsel, mahalle ve ilçe verilerini otomatik olarak çeker, geometrileri EPSG:4326’dan EPSG:2320’ye dönüştürür ve PostgreSQL/PostGIS’e kaydeder. Docker ile çalışır, Oracle’a senkronizasyon için `scripts/sync.sh` içerir.

## Özellikler
- TKGM WFS 1.1.2 uyumlu istemci ile veri çekme ve sayfalandırma
- Geometri dönüştürme: EPSG:4326 → EPSG:2320 ve WKT üretimi
- PostgreSQL’e kayıt ve benzersiz anahtarlarla upsert mantığı
- Günlük/tam veri senkronizasyon akışları ve istatistik çıktıları
- Docker imajında Oracle Instant Client ve Oracle destekli GDAL derleme
- PostgreSQL → Oracle veri aktarımı için optimize `scripts/sync.sh`

## Mimarî ve Bileşenler
- `main.py`: CLI komutları ve senkronizasyon akışları
- `src/client.py`: WFS istekleri, kimlik doğrulama ve retry
- `src/geometry.py`: XML ayrıştırma, koordinat dönüşümü, WKT üretimi
- `src/database.py`: PostgreSQL bağlantısı, tablo yönetimi ve upsert işlemleri
- `scripts/sync.sh`: Postgres’ten Oracle’a veri aktarım (ogr2ogr, sqlplus)
- `Dockerfile` ve `docker-compose.yml`: Çalışma ortamı ve cron görevleri

## Gereksinimler
- Python 3.10+
- PostgreSQL (PostGIS uzantısı etkin olmalı)
- Docker ve Docker Compose (opsiyonel)
- Oracle Instant Client (Docker imajında dahil, harici kullanım için gerekebilir)

## Kurulum
1. Depoyu klonlayın:
   - `git clone <repo-url>`
   - `cd python-tkgm`
2. Ortam değişkenlerini ayarlayın:
   - `.env.example` dosyasını `.env` olarak kopyalayın ve değerleri doldurun.
   - Önemli değişkenler:
     - `TKGM_BASE_URL`, `TKGM_USERNAME`, `TKGM_PASSWORD`
     - `MAXFEATURES`, `STARTINDEX`
     - `POSTGRES_HOST`, `POSTGRES_DB`, `POSTGRES_PORT`, `POSTGRES_USER`, `POSTGRES_PASS`
     - `PARSELLER`, `MAHALLELER`, `ILCELER`, `TARGET_SRID`
     - (Opsiyonel Oracle) `ORACLE_HOST`, `ORACLE_PORT`, `ORACLE_SERVICE_NAME`, `ORACLE_USER`, `ORACLE_PASS`
3. Python ortamı (Docker kullanmadan):
   - `python -m venv venv && venv\Scripts\activate` (Windows)
   - `pip install -r requirements.txt`
4. Docker ile çalıştırma (önerilir):
   - `docker compose up -d --build`
   - Container içinde cron görevleriyle planlı çalışır.

## Kullanım
- CLI Komutları (lokalde veya container içinde):
  - `python main.py --daily` günlük parsel senkronizasyonu
  - `python main.py --fully` tam parsel senkronizasyonu (sayfalandırmalı)
  - `python main.py --neighbourhoods` mahalle senkronizasyonu
  - `python main.py --districts` ilçe senkronizasyonu
  - `python main.py --stats` istatistikleri gösterir
- Loglama:
  - `LOG_FILE` ve `LOG_LEVEL` `.env` ile ayarlanır; varsayılan `logs/scraper.log`.
- Performans Ayarları:
  - `MAXFEATURES` (varsayılan 1000), `STARTINDEX` sayfalandırmayı kontrol eder.

## Veri Dönüşümü ve Depolama
- Geometri işleme EPSG:4326 kaynak CRS’den EPSG:2320 hedef CRS’ye `pyproj` ile yapılır.
- WKT üretimi `shapely` ile; PostgreSQL’e `ST_GeomFromText(wkt, 2320)` kullanılarak yazılır.
- Upsert anahtarı: `(tapukimlikno, tapuzeminref)` benzersiz kayıt kontrolü.

## Oracle Senkronizasyonu
- `scripts/sync.sh` Postgres’ten Oracle’a veri aktarımı yapar:
  - Bağımlılıklar: `ogr2ogr`, `sqlplus`, `psql`
  - Bağlantı testleri ve tablo hazırlığı (truncate veya oluşturma)
  - Ortam değişkenleri `.env` üzerinden okunur.

## Docker ve Cron
- `Dockerfile` içinde cron job’lar tanımlı:
  - `0 2 * * *` günlük parsel senkronizasyonu (`/app/main.py --daily`)
  - `0 3 * * *` Oracle senkronizasyonu (`/app/scripts/sync.sh`)
- Not: `Dockerfile` cron dosyasını `/etc/cron.d/tkgm` olarak yazar.

## Lisans
- MIT Lisansı altında dağıtılır. Detaylar için `LICENSE` dosyasına bakınız.