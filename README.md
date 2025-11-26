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
     - `POSTGRES_SOURCE_HOST`, `POSTGRES_SOURCE_DB`, `POSTGRES_SOURCE_PORT`, `POSTGRES_SOURCE_USER`, `POSTGRES_SOURCE_PASS`
     - `PARSELLER`, `MAHALLELER`, `ILCELER`, `TARGET_SRID`
     - (Opsiyonel Oracle) `ORACLE_TARGET_HOST`, `ORACLE_TARGET_PORT`, `ORACLE_TARGET_SERVICE_NAME`, `ORACLE_TARGET_USER`, `ORACLE_TARGET_PASS`
3. Python ortamı (Docker kullanmadan):
   - `python -m venv venv && venv\Scripts\activate` (Windows)
   - `pip install -r requirements.txt`
   - Alternatif (önerilen): `uv pip install -r requirements.txt`
4. Docker ile çalıştırma (önerilir):
   - `docker compose up -d --build`
   - Container içinde cron görevleriyle planlı çalışır.

- WKT üretimi `shapely` ile; PostgreSQL’e `ST_GeomFromText(wkt, 2320)` kullanılarak yazılır.
- Upsert anahtarı: `(tapukimlikno, tapuzeminref)` benzersiz kayıt kontrolü.

## GDAL Binding ve Fallback Davranışı
- Proje, geometri dönüşümünde öncelikle GDAL Python binding’i kullanır. GDAL mevcut değilse otomatik olarak Shapely+PyProj fallback’i devreye girer.
- `src/geometry.py` içinde `GDAL_AVAILABLE` bayrağı ile durum kontrol edilir. Başlatma sırasında loglara aşağıdaki mesajlardan biri yazılır:
  - GDAL mevcutsa: `GDAL dönüştürücü başlatıldı: EPSG:4326 -> EPSG:2320`
  - GDAL yoksa: `GDAL bulunamadı, Shapely+PyProj kullanılacak (daha yavaş)`
- Docker imajı bağımlılık kurulumu için `uv pip install --system -r requirements.txt` kullanır ve derlenen GDAL (v3.8.0) ile Python binding’in eşleştiğini build aşamasında doğrular.

### Kullanım (Programatik)
- Dönüştürücü otomatik seçilir; `WFSGeometryProcessor` sınıfı iç mantıkta GDAL varsa `transform_geometry_gdal` kullanır, değilse PyProj ile dönüştürür.
- Örnek:

```python
from src.geometry import WFSGeometryProcessor, GDAL_AVAILABLE

# Kaynak ve hedef CRS tanımla
processor = WFSGeometryProcessor(source_crs="EPSG:4326", target_crs="EPSG:2320")

print("GDAL kullanılabilir mi?", GDAL_AVAILABLE)

# WKT örneği (Polygon)
wkt = "POLYGON((30.0 40.0, 30.1 40.0, 30.1 40.1, 30.0 40.1, 30.0 40.0))"

# Dönüşüm (içeride GDAL varsa onu, yoksa PyProj’u kullanır)
# Not: Normal akışta WKT dönüşümü process_geometry_element içinde otomatik gerçekleşir.
try:
    transformed_wkt = processor.transform_geometry_gdal(wkt) if GDAL_AVAILABLE else None
except Exception:
    transformed_wkt = None

print("Transformed WKT:", transformed_wkt)
```

## Benchmark Örneği
- GDAL ve PyProj arasında performans ve doğruluk karşılaştırması yapmak için `benchmark_transform` fonksiyonunu kullanabilirsiniz.

```python
from src.geometry import WFSGeometryProcessor

processor = WFSGeometryProcessor(source_crs="EPSG:4326", target_crs="EPSG:2320")

wkt = "POLYGON((30.0 40.0, 30.1 40.0, 30.1 40.1, 30.0 40.1, 30.0 40.0))"

result = processor.benchmark_transform(wkt)
print(result)
# Örnek çıktı:
# {
#   'gdal_time_ms': 2.1,
#   'pyproj_time_ms': 5.8,
#   'mean_distance_m': 0.003,
#   'method_used': 'gdal'
# }
```

- `mean_distance_m` değeri dönüşümden sonra GDAL ve PyProj sonuçları arasındaki ortalama nokta mesafesini (metre cinsinden yaklaşık) raporlar; farklılık çok düşük olmalıdır.
- Büyük veri setlerinde GDAL genellikle daha hızlıdır; loglar hangi yöntemin kullanıldığını bildirir.

## Oracle Senkronizasyonu
- `scripts/sync.sh` Postgres’ten Oracle’a veri aktarımı yapar:
  - Bağımlılıklar: `ogr2ogr`, `sqlplus`, `psql`
  - Bağlantı testleri ve tablo hazırlığı (truncate veya oluşturma)
  - Ortam değişkenleri `.env` üzerinden okunur.

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
     - `POSTGRES_SOURCE_HOST`, `POSTGRES_SOURCE_DB`, `POSTGRES_SOURCE_PORT`, `POSTGRES_SOURCE_USER`, `POSTGRES_SOURCE_PASS`
     - `PARSELLER`, `MAHALLELER`, `ILCELER`, `TARGET_SRID`
     - (Opsiyonel Oracle) `ORACLE_TARGET_HOST`, `ORACLE_TARGET_PORT`, `ORACLE_TARGET_SERVICE_NAME`, `ORACLE_TARGET_USER`, `ORACLE_TARGET_PASS`
3. Python ortamı (Docker kullanmadan):
   - `python -m venv venv && venv\Scripts\activate` (Windows)
   - `pip install -r requirements.txt`
   - Alternatif (önerilen): `uv pip install -r requirements.txt`
4. Docker ile çalıştırma (önerilir):
   - `docker compose up -d --build`
   - Container içinde cron görevleriyle planlı çalışır.

- WKT üretimi `shapely` ile; PostgreSQL’e `ST_GeomFromText(wkt, 2320)` kullanılarak yazılır.
- Upsert anahtarı: `(tapukimlikno, tapuzeminref)` benzersiz kayıt kontrolü.

## GDAL Binding ve Fallback Davranışı
- Proje, geometri dönüşümünde öncelikle GDAL Python binding’i kullanır. GDAL mevcut değilse otomatik olarak Shapely+PyProj fallback’i devreye girer.
- `src/geometry.py` içinde `GDAL_AVAILABLE` bayrağı ile durum kontrol edilir. Başlatma sırasında loglara aşağıdaki mesajlardan biri yazılır:
  - GDAL mevcutsa: `GDAL dönüştürücü başlatıldı: EPSG:4326 -> EPSG:2320`
  - GDAL yoksa: `GDAL bulunamadı, Shapely+PyProj kullanılacak (daha yavaş)`
- Docker imajı bağımlılık kurulumu için `uv pip install --system -r requirements.txt` kullanır ve derlenen GDAL (v3.8.0) ile Python binding’in eşleştiğini build aşamasında doğrular.

### Kullanım (Programatik)
- Dönüştürücü otomatik seçilir; `WFSGeometryProcessor` sınıfı iç mantıkta GDAL varsa `transform_geometry_gdal` kullanır, değilse PyProj ile dönüştürür.
- Örnek:

```python
from src.geometry import WFSGeometryProcessor, GDAL_AVAILABLE

# Kaynak ve hedef CRS tanımla
processor = WFSGeometryProcessor(source_crs="EPSG:4326", target_crs="EPSG:2320")

print("GDAL kullanılabilir mi?", GDAL_AVAILABLE)

# WKT örneği (Polygon)
wkt = "POLYGON((30.0 40.0, 30.1 40.0, 30.1 40.1, 30.0 40.1, 30.0 40.0))"

# Dönüşüm (içeride GDAL varsa onu, yoksa PyProj’u kullanır)
# Not: Normal akışta WKT dönüşümü process_geometry_element içinde otomatik gerçekleşir.
try:
    transformed_wkt = processor.transform_geometry_gdal(wkt) if GDAL_AVAILABLE else None
except Exception:
    transformed_wkt = None

print("Transformed WKT:", transformed_wkt)
```

## Benchmark Örneği
- GDAL ve PyProj arasında performans ve doğruluk karşılaştırması yapmak için `benchmark_transform` fonksiyonunu kullanabilirsiniz.

```python
from src.geometry import WFSGeometryProcessor

processor = WFSGeometryProcessor(source_crs="EPSG:4326", target_crs="EPSG:2320")

wkt = "POLYGON((30.0 40.0, 30.1 40.0, 30.1 40.1, 30.0 40.1, 30.0 40.0))"

result = processor.benchmark_transform(wkt)
print(result)
# Örnek çıktı:
# {
#   'gdal_time_ms': 2.1,
#   'pyproj_time_ms': 5.8,
#   'mean_distance_m': 0.003,
#   'method_used': 'gdal'
# }
```

- `mean_distance_m` değeri dönüşümden sonra GDAL ve PyProj sonuçları arasındaki ortalama nokta mesafesini (metre cinsinden yaklaşık) raporlar; farklılık çok düşük olmalıdır.
- Büyük veri setlerinde GDAL genellikle daha hızlıdır; loglar hangi yöntemin kullanıldığını bildirir.

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

## Telegram Entegrasyonu
- Ortam değişkenlerini `.env` içinde tanımlayın:
  - `TELEGRAM_BOT_TOKEN` (BotFather ile oluşturulan bot token'ı)
  - `TELEGRAM_CHAT_ID` (grup/kanal/chat kimliği; negatif değerli olabilir)
  - `TELEGRAM_PARSE_MODE` (opsiyonel: `Markdown`, `MarkdownV2`, `HTML`)
- İstatistik gönderimi:
  - `python main.py --stats-telegram`
 - Günlük senkronizasyon özeti ve hata bildirimleri:
   - `python main.py --daily` çalışırken Telegram yapılandırması varsa, akış sonunda günlük özet otomatik gönderilir.
   - TKGM yanıt alınamazsa, veritabanı kayıt hatası olursa veya işleme sırasında hata oluşursa anında uyarı mesajı gönderilir.
- Notlar:
  - Chat ID'yi `@userinfobot` ile veya Telegram API üzerinden edinebilirsiniz.
  - Gönderim sonucu loglara yazılır.

## Log Yönetimi (Self-Cleanup FIFO)
Proje, **Self-Cleanup (Kendi Kendini Temizleme)** stratejisi ile log yönetimi yapar:

### Merkezi Log Dosyaları
- `/app/logs/cron_oracle.log` - Oracle senkronizasyon logları
- `/app/logs/cron_postgresql.log` - PostgreSQL senkronizasyon logları
- `/app/logs/scraper.log` - Python scraper logları

### FIFO Temizleme Mekanizması
- Her script/uygulama kendi log dosyasını yönetir
- **Maksimum boyut**: 100MB
- **Temizlik sonrası**: Son 50MB tutulur
- **Çalışma zamanı**: Her script çalıştığında/bittiğinde otomatik kontrol

## Lisans
- MIT Lisansı altında dağıtılır. Detaylar için `LICENSE` dosyasına bakınız.