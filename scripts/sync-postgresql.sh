#!/bin/bash

# =============================================================================
# TKGM PostgreSQL Senkronizasyonu - Optimize Edilmiş Script
# Kaynak PostgreSQL'den Hedef PostgreSQL'e TKGM verisi aktarımı
# =============================================================================

set -euo pipefail

# =============================================================================
# GLOBAL VARIABLES
# =============================================================================

LOG_FILE=""
START_TIME=$(date +%s)

# =============================================================================
# CONFIGURATION
# =============================================================================

load_config() {
    for env_file in ".env" "../.env"; do
        [ -f "$env_file" ] && source "$env_file" && break
    done
    
    local log_dir
    for dir in "/app/logs" "./logs"; do
        if mkdir -p "$dir" 2>/dev/null; then
            log_dir="$dir"
            break
        fi
    done
    LOG_FILE="${log_dir:-./}/cron_postgresql.log"
    
    # Source Database configuration (PostgreSQL)
    POSTGRES_SOURCE_HOST="${POSTGRES_SOURCE_HOST:-localhost}"
    POSTGRES_SOURCE_PORT="${POSTGRES_SOURCE_PORT:-5432}"
    POSTGRES_SOURCE_DB="${POSTGRES_SOURCE_DB:-cadastral_db}"
    POSTGRES_SOURCE_USER="${POSTGRES_SOURCE_USER:-postgres}"
    POSTGRES_SOURCE_PASS="${POSTGRES_SOURCE_PASS:-password}"
    
    # Target Database configuration (PostgreSQL)
    POSTGRES_TARGET_HOST="${POSTGRES_TARGET_HOST:-localhost}"
    POSTGRES_TARGET_PORT="${POSTGRES_TARGET_PORT:-5433}"
    POSTGRES_TARGET_DB="${POSTGRES_TARGET_DB:-cadastral_target_db}"
    POSTGRES_TARGET_USER="${POSTGRES_TARGET_USER:-postgres}"
    POSTGRES_TARGET_PASS="${POSTGRES_TARGET_PASS:-password}"
    POSTGRES_TARGET_TABLE="${POSTGRES_TARGET_TABLE:-tk_parsel}"
    
    # Sync mode: truncate or recreate
    SYNC_MODE="${SYNC_MODE:-truncate}"
    
    # PostgreSQL environment
    export GDAL_CACHEMAX=2048
}

# =============================================================================
# LOGGING FUNCTIONS
# =============================================================================

log() {
    local level="${2:-INFO}"
    local message="[$(date '+%Y-%m-%d %H:%M:%S')] [$level] $1"
    echo "$message" | tee -a "$LOG_FILE"
}

log_error() { log "$1" "ERROR" >&2; }
log_warn() { log "$1" "WARN"; }
log_success() { log "$1" "SUCCESS"; }

exit_with_error() {
    log_error "$1"
    exit 1
}

# =============================================================================
# VALIDATION FUNCTIONS
# =============================================================================

check_dependencies() {
    log "Checking dependencies..."
    
    local missing_deps=()
    for cmd in ogr2ogr psql; do
        command -v "$cmd" >/dev/null || missing_deps+=("$cmd")
    done
    
    if [ ${#missing_deps[@]} -gt 0 ]; then
        exit_with_error "Missing dependencies: ${missing_deps[*]}"
    fi
    
    log_success "All dependencies found"
}

test_connections() {
    log "Testing database connections..."
    
    # Test Source PostgreSQL
    if ! PGPASSWORD="$POSTGRES_SOURCE_PASS" psql -h "$POSTGRES_SOURCE_HOST" -p "$POSTGRES_SOURCE_PORT" \
        -U "$POSTGRES_SOURCE_USER" -d "$POSTGRES_SOURCE_DB" -c "SELECT 1;" >/dev/null 2>&1; then
        exit_with_error "Source PostgreSQL connection failed"
    fi
    
    # Test Target PostgreSQL
    if ! PGPASSWORD="$POSTGRES_TARGET_PASS" psql -h "$POSTGRES_TARGET_HOST" -p "$POSTGRES_TARGET_PORT" \
        -U "$POSTGRES_TARGET_USER" -d "$POSTGRES_TARGET_DB" -c "SELECT 1;" >/dev/null 2>&1; then
        exit_with_error "Target PostgreSQL connection failed"
    fi
    
    log_success "Database connections successful"
}

# =============================================================================
# DATA FUNCTIONS
# =============================================================================

get_sql_query() {
    cat <<'EOF'
SELECT * FROM public.tk_parsel p
EOF
}

get_record_count() {
    local host="$1"
    local port="$2"
    local user="$3"
    local pass="$4"
    local db="$5"
    local table="$6"
    
    PGPASSWORD="$pass" psql -h "$host" -p "$port" \
        -U "$user" -d "$db" \
        -t -c "SELECT COUNT(*) FROM $table;" 2>/dev/null | \
        grep -E '[0-9]+' | head -1 | tr -d ' ' | xargs
}

# =============================================================================
# MAIN OPERATIONS
# =============================================================================

check_and_prepare_table() {
    log "Checking PostgreSQL target table status..."
    
    local table_exists
    table_exists=$(PGPASSWORD="$POSTGRES_TARGET_PASS" psql -h "$POSTGRES_TARGET_HOST" \
        -p "$POSTGRES_TARGET_PORT" -U "$POSTGRES_TARGET_USER" -d "$POSTGRES_TARGET_DB" \
        -t -c "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '$POSTGRES_TARGET_TABLE');" 2>/dev/null | \
        tr -d ' ')
    
    if [ "$table_exists" = "t" ]; then
        log "Table exists, truncating..."
        truncate_postgresql_table
        return 0
    else
        log "Table does not exist, will be created by OGR2OGR..."
        return 1
    fi
}

truncate_postgresql_table() {
    log "Truncating PostgreSQL table: $POSTGRES_TARGET_TABLE"
    
    PGPASSWORD="$POSTGRES_TARGET_PASS" psql -h "$POSTGRES_TARGET_HOST" \
        -p "$POSTGRES_TARGET_PORT" -U "$POSTGRES_TARGET_USER" -d "$POSTGRES_TARGET_DB" \
        -c "TRUNCATE TABLE $POSTGRES_TARGET_TABLE RESTART IDENTITY CASCADE;" 2>&1 | tee -a "$LOG_FILE"
    
    local exit_code=${PIPESTATUS[0]}
    
    if [ $exit_code -ne 0 ]; then
        log_error "Failed to truncate PostgreSQL table"
        exit 1
    fi
    
    log_success "PostgreSQL table truncated successfully"
}

sync_data() {
    log "Starting data synchronization..."
    
    # Tablo varsa truncate, yoksa OGR2OGR oluşturacak
    local table_exists=false
    if check_and_prepare_table; then
        table_exists=true
    fi
    
    local sql_query
    sql_query=$(get_sql_query)

    log "Executing OGR2OGR data transfer..."
    
    if [ "$table_exists" = true ]; then
        # Tablo var - APPEND modu
        log "Using APPEND mode (table exists)..."
        ogr2ogr -f "PostgreSQL" \
            "PG:host=${POSTGRES_TARGET_HOST} port=${POSTGRES_TARGET_PORT} dbname=${POSTGRES_TARGET_DB} user=${POSTGRES_TARGET_USER} password=${POSTGRES_TARGET_PASS}" \
            "PG:host=${POSTGRES_SOURCE_HOST} port=${POSTGRES_SOURCE_PORT} dbname=${POSTGRES_SOURCE_DB} user=${POSTGRES_SOURCE_USER} password=${POSTGRES_SOURCE_PASS}" \
            -sql "$sql_query" \
            -nln "$POSTGRES_TARGET_TABLE" \
            -append \
            -lco LAUNDER=NO \
            -lco GEOMETRY_NAME=geometry \
            -lco PRECISION=YES \
            -lco FID=ogr_fid \
            -skipfailures \
            -a_srs "EPSG:2320" \
            -gt 65536 \
            -progress \
            --config PG_USE_COPY YES \
            2>&1 | tee -a "$LOG_FILE"
    else
        # Tablo yok - CREATE modu
        log "Using CREATE mode (table will be created)..."
        ogr2ogr -f "PostgreSQL" \
            "PG:host=${POSTGRES_TARGET_HOST} port=${POSTGRES_TARGET_PORT} dbname=${POSTGRES_TARGET_DB} user=${POSTGRES_TARGET_USER} password=${POSTGRES_TARGET_PASS}" \
            "PG:host=${POSTGRES_SOURCE_HOST} port=${POSTGRES_SOURCE_PORT} dbname=${POSTGRES_SOURCE_DB} user=${POSTGRES_SOURCE_USER} password=${POSTGRES_SOURCE_PASS}" \
            -sql "$sql_query" \
            -nln "$POSTGRES_TARGET_TABLE" \
            -nlt MULTIPOLYGON \
            -lco LAUNDER=NO \
            -lco GEOMETRY_NAME=geometry \
            -lco DIM=2 \
            -lco FID=ogr_fid \
            -lco PRECISION=YES \
            -a_srs "EPSG:2320" \
            -gt 65536 \
            -progress \
            --config PG_USE_COPY YES \
            2>&1 | tee -a "$LOG_FILE"
        
        # İlk oluşturmada spatial index ekle
        create_spatial_index
    fi
    
    local exit_code=${PIPESTATUS[0]}
    
    if [ $exit_code -ne 0 ]; then
        log_error "OGR2OGR failed with exit code: $exit_code"
        exit 1
    fi
    
    # Veri kontrolü yap
    log "Verifying data transfer..."
    local target_count
    target_count=$(PGPASSWORD="$POSTGRES_TARGET_PASS" psql -h "$POSTGRES_TARGET_HOST" \
        -p "$POSTGRES_TARGET_PORT" -U "$POSTGRES_TARGET_USER" -d "$POSTGRES_TARGET_DB" \
        -t -c "SELECT COUNT(*) FROM $POSTGRES_TARGET_TABLE;" 2>/dev/null | \
        grep -E '[0-9]+' | head -1 | tr -d ' ' | xargs)
    
    if [ -n "$target_count" ] && [ "$target_count" -gt 0 ]; then
        log_success "Data transfer completed. Transferred records: $target_count"
    else
        log_error "No data transferred to target PostgreSQL"
        exit 1
    fi
}

create_spatial_index() {
    log "Creating spatial index..."
    
    PGPASSWORD="$POSTGRES_TARGET_PASS" psql -h "$POSTGRES_TARGET_HOST" \
        -p "$POSTGRES_TARGET_PORT" -U "$POSTGRES_TARGET_USER" -d "$POSTGRES_TARGET_DB" \
        -c "CREATE INDEX IF NOT EXISTS ${POSTGRES_TARGET_TABLE}_geometry_idx ON ${POSTGRES_TARGET_TABLE} USING GIST (geometry);" \
        2>&1 | tee -a "$LOG_FILE"
    
    log_success "Spatial index created"
}

update_statistics() {
    log "Updating PostgreSQL table statistics..."
    
    PGPASSWORD="$POSTGRES_TARGET_PASS" psql -h "$POSTGRES_TARGET_HOST" \
        -p "$POSTGRES_TARGET_PORT" -U "$POSTGRES_TARGET_USER" -d "$POSTGRES_TARGET_DB" \
        -c "VACUUM ANALYZE ${POSTGRES_TARGET_TABLE};" 2>&1 | tee -a "$LOG_FILE"
    
    local exit_code=${PIPESTATUS[0]}
    
    if [ $exit_code -eq 0 ]; then
        log_success "Statistics updated successfully"
    else
        log_warn "Failed to update statistics"
    fi
}

# =============================================================================
# MAIN FUNCTION
# =============================================================================

main() {
    load_config
    log "=== TKGM PostgreSQL Sync Started ==="
    
    check_dependencies
    test_connections
    
    sync_data
    update_statistics
    
    local duration=$(($(date +%s) - START_TIME))
    log_success "Sync completed in ${duration} seconds"
    log "Log file: $LOG_FILE"
    
    # Log temizliği yap (Self-Cleanup)
    cleanup_log_file
}

cleanup_log_file() {
    local max_size=$((100 * 1024 * 1024)) # 100MB
    local keep_size=$((50 * 1024 * 1024)) # 50MB
    
    if [ -f "$LOG_FILE" ]; then
        local current_size=$(stat -c%s "$LOG_FILE" 2>/dev/null || echo 0)
        
        if [ "$current_size" -gt "$max_size" ]; then
            log "Log file size ($((current_size/1024/1024))MB) exceeds limit ($((max_size/1024/1024))MB). Truncating..."
            
            # Son N byte'ı al (tail -c) - Binary güvenli ve hızlı
            tail -c "$keep_size" "$LOG_FILE" > "${LOG_FILE}.tmp"
            mv "${LOG_FILE}.tmp" "$LOG_FILE"
            
            # Temizlik bilgisini loga ekle
            echo "[$(date '+%Y-%m-%d %H:%M:%S')] [INFO] Log file truncated. Kept last $((keep_size/1024/1024))MB." >> "$LOG_FILE"
        fi
    fi
}

# =============================================================================
# SCRIPT ENTRY POINT
# =============================================================================

case "${1:-}" in
    "--help"|"-h")
        echo "Usage: $0 [--help|--test|--dry-run]"
        echo "  --help      Show this help"
        echo "  --test      Test connections only"
        echo "  --dry-run   Show SQL query only"
        exit 0
        ;;
    "--test")
        load_config
        check_dependencies
        test_connections
        log_success "Connection tests passed"
        exit 0
        ;;
    "--dry-run")
        echo "SQL Query:"
        get_sql_query
        exit 0
        ;;
    *)
        main
        ;;
esac
