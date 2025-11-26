#!/bin/bash

# =============================================================================
# TKGM Oracle Senkronizasyonu - Optimize Edilmiş Script
# PostgreSQL'den Oracle'a TKGM verisi aktarımı
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
    LOG_FILE="${log_dir:-./}/cron_oracle.log"
    
    # Database configuration
    POSTGRES_SOURCE_HOST="${POSTGRES_SOURCE_HOST:-localhost}"
    POSTGRES_SOURCE_PORT="${POSTGRES_SOURCE_PORT:-5432}"
    POSTGRES_SOURCE_DB="${POSTGRES_SOURCE_DB:-cadastral_db}"
    POSTGRES_SOURCE_USER="${POSTGRES_SOURCE_USER:-postgres}"
    POSTGRES_SOURCE_PASS="${POSTGRES_SOURCE_PASS:-password}"
    
    ORACLE_TARGET_HOST="${ORACLE_TARGET_HOST:-localhost}"
    ORACLE_TARGET_PORT="${ORACLE_TARGET_PORT:-1521}"
    ORACLE_TARGET_SERVICE_NAME="${ORACLE_TARGET_SERVICE_NAME:-ORCL}"
    ORACLE_TARGET_USER="${ORACLE_TARGET_USER:-cadastral}"
    ORACLE_TARGET_PASS="${ORACLE_TARGET_PASS:-password}"
    ORACLE_TARGET_TABLE="${ORACLE_TARGET_TABLE:-TK_PARSEL}"
    
    # Sync mode: truncate or recreate
    SYNC_MODE="${SYNC_MODE:-truncate}"
    
    # Oracle environment
    export TNS_ADMIN="${TNS_ADMIN:-/usr/lib/oracle/instantclient}"
    export NLS_LANG="AMERICAN_AMERICA.UTF8"
    export NLS_DATE_FORMAT="YYYY-MM-DD HH24:MI:SS"
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
    for cmd in ogr2ogr sqlplus psql; do
        command -v "$cmd" >/dev/null || missing_deps+=("$cmd")
    done
    
    if [ ${#missing_deps[@]} -gt 0 ]; then
        exit_with_error "Missing dependencies: ${missing_deps[*]}"
    fi
    
    log_success "All dependencies found"
}

test_connections() {
    log "Testing database connections..."
    
    # Test PostgreSQL
    if ! PGPASSWORD="$POSTGRES_SOURCE_PASS" psql -h "$POSTGRES_SOURCE_HOST" -p "$POSTGRES_SOURCE_PORT" \
        -U "$POSTGRES_SOURCE_USER" -d "$POSTGRES_SOURCE_DB" -c "SELECT 1;" >/dev/null 2>&1; then
        exit_with_error "PostgreSQL connection failed"
    fi
    
    # Test Oracle
    if ! echo "SELECT 1 FROM DUAL;" | sqlplus -s "${ORACLE_TARGET_USER}/${ORACLE_TARGET_PASS}@${ORACLE_TARGET_HOST}:${ORACLE_TARGET_PORT}/${ORACLE_TARGET_SERVICE_NAME}" >/dev/null 2>&1; then
        exit_with_error "Oracle connection failed"
    fi
    
    log_success "Database connections successful"
}

# =============================================================================
# DATA FUNCTIONS
# =============================================================================

get_sql_query() {
    cat << 'EOF'
SELECT 
    CAST(p.id AS NUMERIC(38,0)) AS "OBJECT_ID",
    CAST(p.fid AS NUMERIC(20,0)) AS "FID",
    CAST(p.tapukimlikno AS NUMERIC(20,0)) AS "TAPU_KIMLIK_NO",
    CAST(m.ilceref AS NUMERIC(38,0)) AS "ILCE_ID",   
    CAST(i.ad AS VARCHAR(100)) AS "ILCE_ADI", 
    CAST(m.fid AS NUMERIC(38,0)) AS "TAPU_MAHALLE_ID",
    CAST(p.tapumahalleref AS NUMERIC(20,0)) AS "TAPU_MAHALLE_REF",
    CAST(m.tapumahallead AS VARCHAR(100)) AS "TAPU_MAHALLE_ADI",
    CAST(p.tapuzeminref AS NUMERIC(20,0)) AS "TAPU_ZEMIN_REF",
    CAST(p.tapuzeminref AS NUMERIC(20,0)) AS "ZEMIN_ID",
    CAST(p.adano AS VARCHAR(255)) AS "ADA",
    CAST(p.parselno AS VARCHAR(255)) AS "PARSEL",
    CAST(p.tip AS NUMERIC(38,0)) AS "TIP",
    CAST(p.tapualan AS FLOAT) AS "ALAN", 
    CAST(p.kadastroalan AS FLOAT) AS "KADASTRO_ALAN",  
    CAST(p.tapucinsid AS NUMERIC(20,0)) AS "TAPU_CINS_ID",  
    CAST(p.tapucinsaciklama AS VARCHAR(1000)) AS "TAPU_CINS_ACIKLAMA", 
    CAST(p.durum AS NUMERIC(38,0)) AS "DURUM",
    CAST(d.adi AS VARCHAR(255)) AS "DURUM_ACIKLAMA", 
    CAST(p.hazineparseldurum AS NUMERIC(38,0)) AS "HAZINE_PARSEL_DURUM",
    CAST(hpd.adi AS VARCHAR(255)) AS "HAZINE_PARSEL_DURUM_ACIKLAMA",
    CAST(p.kmdurum AS NUMERIC(38,0)) AS "KM_DURUM",
    CAST(kmd.adi AS VARCHAR(255)) AS "KM_DURUM_ACIKLAMASI", 
    CAST(p.onaydurum AS NUMERIC(38,0)) AS "ONAY_DURUM",
    CAST(od.adi AS VARCHAR(255)) AS "ONAY_DURUM_ACIKLAMA", 
    CAST(p.sistemkayittarihi AS timestamp(6)) AS "SISTEM_KAYIT_TARIHI",
    CAST(TO_CHAR(p.sistemguncellemetarihi, 'YYYY-MM-DD HH24:MI:SS') AS VARCHAR(4000)) AS "M_DATE",
    CAST(COALESCE(p.adano::text, '') || '-' || COALESCE(p.parselno::text, '') || '-' || 
         COALESCE(i.ad, '') || '-' || COALESCE(m.tapumahallead, '') AS VARCHAR(4000)) AS "APIM",
    p.geom AS "GEOMETRY"
FROM public.tk_parsel p
LEFT JOIN public.tk_kat_mulkiyet_durum_tip kmd ON kmd.kod = p.kmdurum 
LEFT JOIN public.tk_hazine_parsel_durum_tip hpd ON hpd.kod = p.hazineparseldurum 
LEFT JOIN public.tk_durum d ON d.kod = p.durum 
LEFT JOIN public.tk_onay_durum od ON od.kod = p.onaydurum 
LEFT JOIN public.tk_mahalle m ON m.tapukimlikno = p.tapumahalleref
LEFT JOIN public.tk_ilce i ON i.fid = m.ilceref
WHERE p.durum <> '2'
EOF
}

get_record_count() {
    local db_type="$1"
    local table="$2"
    
    if [ "$db_type" = "postgres" ]; then
        PGPASSWORD="$POSTGRES_SOURCE_PASS" psql -h "$POSTGRES_SOURCE_HOST" -p "$POSTGRES_SOURCE_PORT" \
            -U "$POSTGRES_SOURCE_USER" -d "$POSTGRES_SOURCE_DB" \
            -t -c "SELECT COUNT(*) FROM $table WHERE durum <> '2';" 2>/dev/null | \
            grep -E '[0-9]+' | head -1 | tr -d ' ' | xargs
    elif [ "$db_type" = "oracle" ]; then
        echo "SELECT COUNT(*) FROM $table;" | \
            sqlplus -s "${ORACLE_TARGET_USER}/${ORACLE_TARGET_PASS}@${ORACLE_TARGET_HOST}:${ORACLE_TARGET_PORT}/${ORACLE_TARGET_SERVICE_NAME}" 2>/dev/null | \
            grep -E '^[[:space:]]*[0-9]+' | head -1 | tr -d ' ' | xargs
    else
        echo "Error: Unknown database type" >&2
        return 1
    fi
}

# =============================================================================
# MAIN OPERATIONS
# =============================================================================

check_and_prepare_table() {
    log "Checking Oracle table status..."
    
    local check_sql
    check_sql=$(cat << EOF
SET SERVEROUTPUT ON
DECLARE
    table_exists NUMBER;
BEGIN
    SELECT COUNT(*) INTO table_exists 
    FROM user_tables 
    WHERE table_name = '$ORACLE_TARGET_TABLE';
    
    IF table_exists > 0 THEN
        DBMS_OUTPUT.PUT_LINE('EXISTS');
    ELSE
        DBMS_OUTPUT.PUT_LINE('NOT_EXISTS');
    END IF;
END;
/
EOF
)
    
    local result
    result=$(echo "$check_sql" | sqlplus -s "${ORACLE_TARGET_USER}/${ORACLE_TARGET_PASS}@${ORACLE_TARGET_HOST}:${ORACLE_TARGET_PORT}/${ORACLE_TARGET_SERVICE_NAME}" 2>&1 | grep -E "EXISTS|NOT_EXISTS" | tail -1)
    
    if [[ "$result" == *"EXISTS"* ]] && [[ "$result" != *"NOT_EXISTS"* ]]; then
        log "Table exists, truncating..."
        truncate_oracle_table
        return 0
    else
        log "Table does not exist, will be created by OGR2OGR..."
        return 1
    fi
}

truncate_oracle_table() {
    log "Truncating Oracle table: $ORACLE_TARGET_TABLE"
    
    local truncate_sql
    truncate_sql=$(cat << EOF
DECLARE
    v_sequence_name VARCHAR2(100);
BEGIN
    -- Tabloyu NOLOGGING moduna al (archivelog azaltmak için)
    EXECUTE IMMEDIATE 'ALTER TABLE $ORACLE_TARGET_TABLE NOLOGGING';
    DBMS_OUTPUT.PUT_LINE('Table set to NOLOGGING mode');
    
    -- Tabloyu truncate et
    EXECUTE IMMEDIATE 'TRUNCATE TABLE $ORACLE_TARGET_TABLE';
    DBMS_OUTPUT.PUT_LINE('Table truncated successfully');
    
    -- OGR_FID ile ilişkili sequence'i bul ve sıfırla
    BEGIN
        SELECT sequence_name INTO v_sequence_name
        FROM user_sequences
        WHERE sequence_name LIKE '%' || '$ORACLE_TARGET_TABLE' || '%'
        AND ROWNUM = 1;
        
        -- Sequence'i drop et
        EXECUTE IMMEDIATE 'DROP SEQUENCE ' || v_sequence_name;
        DBMS_OUTPUT.PUT_LINE('Sequence dropped: ' || v_sequence_name);
        
        -- Sequence'i yeniden oluştur
        EXECUTE IMMEDIATE 'CREATE SEQUENCE ' || v_sequence_name || ' START WITH 1 INCREMENT BY 1';
        DBMS_OUTPUT.PUT_LINE('Sequence recreated: ' || v_sequence_name);
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            DBMS_OUTPUT.PUT_LINE('No sequence found for table');
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('Sequence reset error: ' || SQLERRM);
    END;
    
    COMMIT;
END;
/
EOF
)
    
    local truncate_output
    truncate_output=$(echo "$truncate_sql" | sqlplus -s "${ORACLE_TARGET_USER}/${ORACLE_TARGET_PASS}@${ORACLE_TARGET_HOST}:${ORACLE_TARGET_PORT}/${ORACLE_TARGET_SERVICE_NAME}" 2>&1)
    
    local exit_code=$?
    echo "$truncate_output" >> "$LOG_FILE"
    
    if [ $exit_code -ne 0 ]; then
        log_error "Failed to truncate Oracle table"
        echo "$truncate_output" | tail -10
        exit 1
    fi
    
    log_success "Oracle table truncated and sequence reset successfully"
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
        ogr2ogr -f "OCI" \
            "OCI:${ORACLE_TARGET_USER}/${ORACLE_TARGET_PASS}@${ORACLE_TARGET_HOST}:${ORACLE_TARGET_PORT}/${ORACLE_TARGET_SERVICE_NAME}:${ORACLE_TARGET_TABLE}" \
            "PG:host=${POSTGRES_SOURCE_HOST} port=${POSTGRES_SOURCE_PORT} dbname=${POSTGRES_SOURCE_DB} user=${POSTGRES_SOURCE_USER} password=${POSTGRES_SOURCE_PASS}" \
            -sql "$sql_query" \
            -nln "$ORACLE_TARGET_TABLE" \
            -append \
            -lco LAUNDER=NO \
            -lco GEOMETRY_NAME=GEOMETRY \
            -lco PRECISION=YES \
	    -lco SRID=2320 \
            -skipfailures \
            -a_srs "EPSG:2320" \
            -gt 65536 \
            -progress \
            --config PG_USE_COPY YES \
            --config OCI_VARCHAR2_SIZE 4000 \
            2>&1 | tee -a "$LOG_FILE"
    else
        # Tablo yok - CREATE modu
        log "Using CREATE mode (table will be created)..."
        ogr2ogr -f "OCI" \
            "OCI:${ORACLE_TARGET_USER}/${ORACLE_TARGET_PASS}@${ORACLE_TARGET_HOST}:${ORACLE_TARGET_PORT}/${ORACLE_TARGET_SERVICE_NAME}:${ORACLE_TARGET_TABLE}" \
            "PG:host=${POSTGRES_SOURCE_HOST} port=${POSTGRES_SOURCE_PORT} dbname=${POSTGRES_SOURCE_DB} user=${POSTGRES_SOURCE_USER} password=${POSTGRES_SOURCE_PASS}" \
            -sql "$sql_query" \
            -nln "$ORACLE_TARGET_TABLE" \
            -nlt MULTIPOLYGON \
            -lco LAUNDER=NO \
            -lco GEOMETRY_NAME=GEOMETRY \
            -lco DIM=2 \
            -lco SRID=2320 \
            -lco INDEX=NO \
            -lco SPATIAL_INDEX=NO \
            -lco PRECISION=YES \
            -a_srs "EPSG:2320" \
            -gt 65536 \
            -progress \
            --config PG_USE_COPY YES \
            --config OCI_VARCHAR2_SIZE 4000 \
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
    target_count=$(get_record_count "oracle" "$ORACLE_TARGET_TABLE")
    
    if [ -n "$target_count" ] && [ "$target_count" -gt 0 ]; then
        log_success "Data transfer completed. Transferred records: $target_count"
    else
        log_error "No data transferred to Oracle"
        exit 1
    fi
    
    # Tabloyu LOGGING moduna geri al
    set_table_logging "LOGGING"
}

set_table_logging() {
    log "Setting Oracle table to $1 mode..."
    
    local mode="$1"
    local logging_sql
    logging_sql=$(cat <<EOF
BEGIN
    EXECUTE IMMEDIATE 'ALTER TABLE $ORACLE_TARGET_TABLE $mode';
    DBMS_OUTPUT.PUT_LINE('Table set to $mode mode');
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error setting $mode: ' || SQLERRM);
        RAISE;
END;
/
EOF
)
    
    echo "$logging_sql" | sqlplus -s "${ORACLE_TARGET_USER}/${ORACLE_TARGET_PASS}@${ORACLE_TARGET_HOST}:${ORACLE_TARGET_PORT}/${ORACLE_TARGET_SERVICE_NAME}" 2>&1 | tee -a "$LOG_FILE"
    
    log_success "Table $mode mode set successfully"
}

create_spatial_index() {
    log "Creating spatial index..."
    
    local index_sql
    index_sql=$(cat << EOF
BEGIN
    -- Spatial index oluştur
    EXECUTE IMMEDIATE 
    'CREATE INDEX ${ORACLE_TARGET_TABLE}_GEOMETRY_IDX ON ${ORACLE_TARGET_TABLE}(GEOMETRY) 
     INDEXTYPE IS MDSYS.SPATIAL_INDEX 
     PARAMETERS(''SDO_INDX_DIMS=2'')';
    
    DBMS_OUTPUT.PUT_LINE('Spatial index created');
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE = -955 THEN
            DBMS_OUTPUT.PUT_LINE('Index already exists');
        ELSE
            RAISE;
        END IF;
END;
/
EOF
)
    
    echo "$index_sql" | sqlplus -s "${ORACLE_TARGET_USER}/${ORACLE_TARGET_PASS}@${ORACLE_TARGET_HOST}:${ORACLE_TARGET_PORT}/${ORACLE_TARGET_SERVICE_NAME}" 2>&1 | tee -a "$LOG_FILE"
    
    log_success "Spatial index created"
}

update_statistics() {
    log "Updating Oracle table statistics..."
    
    local stats_output
    stats_output=$(echo "BEGIN DBMS_STATS.GATHER_TABLE_STATS('${ORACLE_TARGET_USER}', '${ORACLE_TARGET_TABLE}'); END;" | \
        sqlplus -s "${ORACLE_TARGET_USER}/${ORACLE_TARGET_PASS}@${ORACLE_TARGET_HOST}:${ORACLE_TARGET_PORT}/${ORACLE_TARGET_SERVICE_NAME}" 2>&1)
    
    local exit_code=$?
    echo "$stats_output" >> "$LOG_FILE"
    
    if [ $exit_code -eq 0 ]; then
        log_success "Statistics updated successfully"
    else
        log_warn "Failed to update statistics"
        echo "$stats_output" | tail -5
    fi
}

# =============================================================================
# MAIN FUNCTION
# =============================================================================

main() {
    load_config
    log "=== TKGM Oracle Sync Started ==="
    
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
