#!/bin/bash
# ============================================================
# MEP Oracle Gatherer — Recolector Automatizado de Metadata
# ============================================================
#
# PROPOSITO:
#   Recolectar TODA la metadata de una instancia Oracle
#   en una sola ejecucion. Genera CSVs organizados por categoria.
#
# REQUISITOS:
#   - sqlplus instalado y en PATH
#   - Usuario con: SELECT ANY DICTIONARY, SELECT_CATALOG_ROLE
#     (o DBA role para acceso completo)
#   - Conexion TNS configurada al instance target
#
# USO INTERACTIVO:
#   chmod +x gather_oracle.sh
#   ./gather_oracle.sh
#
# USO CLI:
#   ./gather_oracle.sh <TNS_ALIAS> <USERNAME> [OUTPUT_DIR]
#
# EJEMPLO:
#   ./gather_oracle.sh SIMPLE_PROD mep_reader /tmp/mep_oracle_prod
#
# COMPATIBLE: Oracle 12c+ (optimizado para 19c)
# ============================================================

VERSION="1.0"

# ---- No abortar en primer error; manejar por query ----
set -uo pipefail

# ---- Cleanup on exit ----
cleanup() {
    # Limpiar password de memoria
    ORA_PASS=""
    unset ORA_PASS
}
trap cleanup EXIT

# ---- Colors (solo si terminal soporta) ----
if [ -t 1 ]; then
    RED='\033[0;31m'
    GREEN='\033[0;32m'
    YELLOW='\033[0;33m'
    CYAN='\033[0;36m'
    BOLD='\033[1m'
    NC='\033[0m'
else
    RED='' GREEN='' YELLOW='' CYAN='' BOLD='' NC=''
fi

# ---- Validate sqlplus ----
check_sqlplus() {
    if ! command -v sqlplus &>/dev/null; then
        echo -e "${RED}[ERROR]${NC} sqlplus no encontrado en PATH."
        echo ""
        echo "  Opciones de instalacion:"
        echo "    1) Oracle Instant Client: https://www.oracle.com/database/technologies/instant-client/downloads.html"
        echo "       Descargar: instantclient-basic + instantclient-sqlplus"
        echo "    2) Oracle Client completo (si ya esta instalado, agregar al PATH):"
        echo "       export PATH=\$ORACLE_HOME/bin:\$PATH"
        echo ""
        echo "  Verificar con: sqlplus -V"
        exit 1
    fi
    SQLPLUS_VERSION=$(sqlplus -V 2>/dev/null | head -1)
    echo -e "  ${GREEN}[OK]${NC} $SQLPLUS_VERSION"
}

# ---- Test connection ----
test_connection() {
    local TNS="$1"
    local ORA_USER="$2"
    local ORA_PASS="$3"

    echo -n "  Probando conexion a ${ORA_USER}@${TNS}... "

    local RESULT
    RESULT=$(sqlplus -s "${ORA_USER}/${ORA_PASS}@${TNS}" <<'EOSQL' 2>&1
SET PAGESIZE 0
SET FEEDBACK OFF
SET HEADING OFF
SELECT 'CONNECTION_OK' FROM dual;
EXIT;
EOSQL
    )

    if echo "$RESULT" | grep -q "CONNECTION_OK"; then
        echo -e "${GREEN}OK${NC}"
        return 0
    else
        echo -e "${RED}FALLO${NC}"
        echo ""
        echo "  Error de conexion:"
        echo "$RESULT" | grep -iE "ORA-|SP2-|ERROR" | head -5 | sed 's/^/    /'
        echo ""
        echo "  Verifique:"
        echo "    - TNS alias correcto (tnsping $TNS)"
        echo "    - Usuario y password"
        echo "    - Permisos: SELECT ANY DICTIONARY, SELECT_CATALOG_ROLE"
        return 1
    fi
}

# ---- Interactive mode ----
interactive_mode() {
    echo -e "${BOLD}============================================================${NC}"
    echo -e "${BOLD}  MEP Oracle Gatherer v${VERSION} -- Stefanini Group${NC}"
    echo -e "${BOLD}  Recolector automatizado de metadata Oracle${NC}"
    echo -e "${BOLD}============================================================${NC}"
    echo ""

    check_sqlplus
    echo ""

    # TNS
    echo -e "${CYAN}  Conexion Oracle${NC}"
    echo -n "  TNS Alias (ej: PROD, orcl, //host:1521/service): "
    read -r TNS
    if [ -z "$TNS" ]; then
        echo -e "${RED}[ERROR]${NC} TNS alias requerido."
        exit 1
    fi

    # Username
    echo -n "  Usuario (ej: mep_reader): "
    read -r ORA_USER
    if [ -z "$ORA_USER" ]; then
        echo -e "${RED}[ERROR]${NC} Usuario requerido."
        exit 1
    fi

    # Password
    echo -n "  Password para ${ORA_USER}@${TNS}: "
    read -s -r ORA_PASS
    echo ""
    if [ -z "$ORA_PASS" ]; then
        echo -e "${RED}[ERROR]${NC} Password requerido."
        exit 1
    fi

    echo ""

    # Test connection
    if ! test_connection "$TNS" "$ORA_USER" "$ORA_PASS"; then
        exit 1
    fi

    echo ""

    # Output dir
    OUTDIR="./mep_oracle_$(date +%Y%m%d_%H%M%S)"
    echo -e "  Output: ${BOLD}${OUTDIR}${NC}"
    echo ""
    echo -e "  ${CYAN}Presione ENTER para iniciar o Ctrl+C para cancelar${NC}"
    read -r
}

# ---- Exclude list for system schemas ----
SYS_EXCLUDE="'SYS','SYSTEM','DBSNMP','OUTLN','XDB','WMSYS','CTXSYS','MDSYS','ORDDATA','ORDSYS','OLAPSYS','EXFSYS','DVSYS','LBACSYS','APEX_040200','APEX_PUBLIC_USER','FLOWS_FILES','ANONYMOUS','APPQOSSYS','GSMADMIN_INTERNAL','XS\$NULL','OJVMSYS','DMSYS','GGSYS','GSMUSER','DIP','REMOTE_SCHEDULER_AGENT','SYSBACKUP','SYSDG','SYSKM','SYSRAC','AUDSYS','DBSFWUSER','DVF'"

# ---- Counters ----
TOTAL_OK=0
TOTAL_ERR=0

# ---- Helper: run SQL and save CSV ----
run_sql() {
    local LABEL="$1"
    local OUTFILE="$2"
    local SQL="$3"

    echo -n "  [$LABEL] ... " | tee -a "$LOG"
    START=$(date +%s)

    # Run query, capture output and exit code
    local OUTPUT
    OUTPUT=$(sqlplus -s "${ORA_USER}/${ORA_PASS}@${TNS}" <<EOSQL 2>&1
SET MARKUP CSV ON QUOTE ON
SET PAGESIZE 0
SET LINESIZE 32767
SET LONG 1000000
SET LONGCHUNKSIZE 200000
SET TRIMSPOOL ON
SET FEEDBACK OFF
SET HEADING ON
SET TERMOUT OFF

${SQL}

EXIT;
EOSQL
    )
    local EXIT_CODE=$?

    END=$(date +%s)
    ELAPSED=$((END-START))

    # Check for ORA- or SP2- errors in output
    local HAS_ERRORS=0
    if [ $EXIT_CODE -ne 0 ]; then
        HAS_ERRORS=1
    elif echo "$OUTPUT" | grep -qiE "^(ORA-|SP2-)"; then
        HAS_ERRORS=1
    fi

    if [ $HAS_ERRORS -eq 1 ]; then
        # Write output anyway (may have partial data)
        echo "$OUTPUT" > "$OUTDIR/$OUTFILE"
        local ERR_MSG
        ERR_MSG=$(echo "$OUTPUT" | grep -iE "^(ORA-|SP2-)" | head -3)
        echo -e "${RED}ERROR${NC} (${ELAPSED}s)" | tee -a "$LOG"
        echo "    $ERR_MSG" | tee -a "$LOG"
        TOTAL_ERR=$((TOTAL_ERR + 1))
    else
        echo "$OUTPUT" > "$OUTDIR/$OUTFILE"
        ROWS=$(echo "$OUTPUT" | wc -l | tr -d ' ')
        echo -e "${GREEN}OK${NC} (${ROWS} filas, ${ELAPSED}s)" | tee -a "$LOG"
        TOTAL_OK=$((TOTAL_OK + 1))
    fi
}


# ============================================================
# MAIN
# ============================================================

if [ $# -ge 2 ]; then
    # ---- CLI mode ----
    TNS="${1:?USO: $0 <TNS_ALIAS> <USERNAME> [OUTPUT_DIR]}"
    ORA_USER="${2:?USO: $0 <TNS_ALIAS> <USERNAME> [OUTPUT_DIR]}"
    OUTDIR="${3:-./mep_oracle_$(date +%Y%m%d_%H%M%S)}"

    check_sqlplus

    # Password
    echo -n "Password para ${ORA_USER}@${TNS}: "
    read -s -r ORA_PASS
    echo ""

    if ! test_connection "$TNS" "$ORA_USER" "$ORA_PASS"; then
        exit 1
    fi
elif [ $# -eq 0 ]; then
    # ---- Interactive mode ----
    interactive_mode
else
    echo "USO:"
    echo "  Interactivo:  $0"
    echo "  CLI:          $0 <TNS_ALIAS> <USERNAME> [OUTPUT_DIR]"
    exit 1
fi

# ---- Setup ----
mkdir -p "$OUTDIR"
LOG="$OUTDIR/gather_oracle.log"
echo "============================================" | tee "$LOG"
echo "MEP Oracle Gatherer v${VERSION} -- $(date)" | tee -a "$LOG"
echo "Instance: ${ORA_USER}@${TNS}" | tee -a "$LOG"
echo "Output:   $OUTDIR" | tee -a "$LOG"
echo "============================================" | tee -a "$LOG"
echo "" | tee -a "$LOG"


# ============================================================
# 00: INSTANCE INFO
# ============================================================
run_sql "00 Instance Info" "00_instance_info.csv" "
SELECT
    d.name              AS db_name,
    d.db_unique_name,
    d.platform_name,
    d.created,
    i.version_full      AS oracle_version,
    i.instance_name,
    i.host_name,
    i.startup_time,
    i.status,
    d.log_mode,
    d.force_logging,
    d.guard_status,
    d.supplemental_log_data_min,
    d.supplemental_log_data_pk,
    d.supplemental_log_data_ui,
    d.supplemental_log_data_fk,
    d.supplemental_log_data_all,
    d.open_mode,
    d.database_role,
    d.dataguard_broker
FROM v\$database d, v\$instance i;
"

# ============================================================
# 01: SCHEMAS
# ============================================================
run_sql "01 Schemas" "01_schemas.csv" "
SELECT
    username, account_status, default_tablespace,
    temporary_tablespace, created, profile,
    authentication_type,
    ROUND((SELECT SUM(bytes)/1024/1024 FROM dba_segments WHERE owner = u.username), 2) AS size_mb
FROM dba_users u
WHERE username NOT IN (${SYS_EXCLUDE})
ORDER BY username;
"

# ============================================================
# 02: OBJECTS SUMMARY
# ============================================================
run_sql "02 Objects Summary" "02_objects_summary.csv" "
SELECT
    owner, object_type,
    COUNT(*)                                                AS cantidad,
    MAX(last_ddl_time)                                      AS ultimo_ddl,
    SUM(CASE WHEN status = 'VALID'   THEN 1 ELSE 0 END)    AS validos,
    SUM(CASE WHEN status = 'INVALID' THEN 1 ELSE 0 END)    AS invalidos
FROM dba_objects
WHERE owner NOT IN (${SYS_EXCLUDE})
GROUP BY owner, object_type
ORDER BY owner, object_type;
"

# ============================================================
# 03: FULL DATA DICTIONARY (tables + columns + comments)
# ============================================================
run_sql "03 Data Dictionary" "03_tables_columns.csv" "
SELECT
    t.owner, t.table_name, t.num_rows, t.avg_row_len,
    t.last_analyzed, t.tablespace_name, t.partitioned,
    t.temporary, t.iot_type,
    NVL(tc.comments, '')    AS table_comment,
    c.column_name, c.column_id, c.data_type, c.data_length,
    c.data_precision, c.data_scale, c.nullable, c.data_default,
    NVL(cc.comments, '')    AS column_comment,
    CASE
        WHEN t.table_name LIKE 'BL_%'  THEN 'BILLING'
        WHEN t.table_name LIKE 'CM_%'  THEN 'CUSTOMER_MGMT'
        WHEN t.table_name LIKE 'PM_%'  THEN 'PRODUCT_MGMT'
        WHEN t.table_name LIKE 'OM_%'  THEN 'ORDER_MGMT'
        WHEN t.table_name LIKE 'RM_%'  THEN 'RESOURCE_MGMT'
        WHEN t.table_name LIKE 'PR_%'  THEN 'PROVISIONING'
        WHEN t.table_name LIKE 'AR_%'  THEN 'ACCOUNTS_RECV'
        WHEN t.table_name LIKE 'GL_%'  THEN 'GENERAL_LEDGER'
        WHEN t.table_name LIKE 'INV_%' THEN 'INVOICE'
        WHEN t.table_name LIKE 'PAY_%' THEN 'PAYMENTS'
        WHEN t.table_name LIKE 'ADJ_%' THEN 'ADJUSTMENTS'
        WHEN t.table_name LIKE 'RT_%'  THEN 'RATING'
        WHEN t.table_name LIKE 'CH_%'  THEN 'CHARGING'
        WHEN t.table_name LIKE 'MED_%' THEN 'MEDIATION'
        WHEN t.table_name LIKE 'CDR_%' THEN 'CDR_EDR'
        WHEN t.table_name LIKE 'REF_%' OR t.table_name LIKE 'LU_%' THEN 'REFERENCE_DATA'
        WHEN t.table_name LIKE 'CFG_%' OR t.table_name LIKE 'CONF_%' THEN 'CONFIG'
        WHEN t.table_name LIKE 'AUD_%' OR t.table_name LIKE 'LOG_%'  THEN 'AUDIT_LOG'
        ELSE 'OTHER'
    END AS domain_hint
FROM dba_tables t
JOIN dba_tab_columns c ON t.owner = c.owner AND t.table_name = c.table_name
LEFT JOIN dba_tab_comments tc ON t.owner = tc.owner AND t.table_name = tc.table_name
LEFT JOIN dba_col_comments cc ON c.owner = cc.owner
    AND c.table_name = cc.table_name AND c.column_name = cc.column_name
WHERE t.owner NOT IN (${SYS_EXCLUDE})
ORDER BY t.owner, t.table_name, c.column_id;
"

# ============================================================
# 04: PRIMARY KEYS
# ============================================================
run_sql "04 Primary Keys" "04_primary_keys.csv" "
SELECT
    c.owner, c.table_name, c.constraint_name,
    cc.column_name, cc.position
FROM dba_constraints c
JOIN dba_cons_columns cc ON c.owner = cc.owner
    AND c.constraint_name = cc.constraint_name
WHERE c.constraint_type = 'P'
AND c.owner NOT IN (${SYS_EXCLUDE})
ORDER BY c.owner, c.table_name, cc.position;
"

# ============================================================
# 05: TABLES WITHOUT PK (CDC risk)
# ============================================================
run_sql "05 Tables No PK" "05_tables_no_pk.csv" "
SELECT
    t.owner, t.table_name, t.num_rows, t.avg_row_len,
    t.last_analyzed, 'SIN PRIMARY KEY -- RIESGO CDC' AS alerta
FROM dba_tables t
WHERE NOT EXISTS (
    SELECT 1 FROM dba_constraints c
    WHERE c.owner = t.owner AND c.table_name = t.table_name
    AND c.constraint_type = 'P'
)
AND t.owner NOT IN (${SYS_EXCLUDE})
AND (t.num_rows > 0 OR t.num_rows IS NULL)
ORDER BY t.num_rows DESC NULLS LAST;
"

# ============================================================
# 06: FOREIGN KEYS
# ============================================================
run_sql "06 Foreign Keys" "06_foreign_keys.csv" "
SELECT
    c.owner, c.table_name AS child_table, c.constraint_name,
    cc.column_name AS fk_column, cc.position,
    r.owner AS parent_owner, r.table_name AS parent_table,
    rc.column_name AS parent_column,
    c.delete_rule, c.status
FROM dba_constraints c
JOIN dba_cons_columns cc ON c.owner = cc.owner AND c.constraint_name = cc.constraint_name
JOIN dba_constraints r ON c.r_owner = r.owner AND c.r_constraint_name = r.constraint_name
JOIN dba_cons_columns rc ON r.owner = rc.owner AND r.constraint_name = rc.constraint_name AND cc.position = rc.position
WHERE c.constraint_type = 'R'
AND c.owner NOT IN (${SYS_EXCLUDE})
ORDER BY c.owner, c.table_name, c.constraint_name, cc.position;
"

# ============================================================
# 07: INDEXES
# ============================================================
run_sql "07 Indexes" "07_indexes.csv" "
SELECT
    i.owner, i.table_name, i.index_name, i.index_type,
    i.uniqueness, ic.column_name, ic.column_position,
    i.tablespace_name, i.status, i.last_analyzed
FROM dba_indexes i
JOIN dba_ind_columns ic ON i.owner = ic.index_owner AND i.index_name = ic.index_name
WHERE i.owner NOT IN (${SYS_EXCLUDE})
AND i.table_name NOT LIKE 'BIN\$%' ESCAPE '\\'
ORDER BY i.owner, i.table_name, i.index_name, ic.column_position;
"

# ============================================================
# 08: CHECK CONSTRAINTS
# ============================================================
run_sql "08 Check Constraints" "08_constraints_check.csv" "
SELECT
    owner, table_name, constraint_name,
    search_condition AS constraint_expression,
    status, validated
FROM dba_constraints
WHERE constraint_type = 'C'
AND owner NOT IN (${SYS_EXCLUDE})
AND constraint_name NOT LIKE 'SYS_%'
ORDER BY owner, table_name;
"

# ============================================================
# 09: TRIGGERS — FULL CODE
# ============================================================
run_sql "09 Triggers Code" "09_triggers_code.csv" "
SELECT
    t.owner, t.trigger_name, t.trigger_type,
    t.triggering_event, t.table_owner, t.table_name,
    t.base_object_type, t.status, t.action_type,
    t.trigger_body AS codigo_completo
FROM dba_triggers t
WHERE t.owner NOT IN (${SYS_EXCLUDE})
ORDER BY t.owner, t.table_name, t.trigger_name;
"

# ============================================================
# 10: VIEWS — FULL CODE
# ============================================================
run_sql "10 Views Code" "10_views_code.csv" "
SELECT
    owner, view_name, text_length,
    text AS view_code_completo, read_only
FROM dba_views
WHERE owner NOT IN (${SYS_EXCLUDE})
ORDER BY owner, view_name;
"

# ============================================================
# 11: MATERIALIZED VIEWS
# ============================================================
run_sql "11 Materialized Views" "11_mviews.csv" "
SELECT
    owner, mview_name, container_name,
    query AS mview_query_completo,
    refresh_mode, refresh_method, build_mode,
    fast_refreshable, last_refresh_type,
    last_refresh_date, staleness, compile_state
FROM dba_mviews
WHERE owner NOT IN (${SYS_EXCLUDE})
ORDER BY owner, mview_name;
"

# ============================================================
# 12: PL/SQL SOURCE CODE (Procedures, Functions, Packages)
# ============================================================
run_sql "12 PL/SQL Code" "12_sp_code.csv" "
SELECT
    owner, name, type, line,
    text AS code_line
FROM dba_source
WHERE owner NOT IN (${SYS_EXCLUDE})
AND type IN ('PROCEDURE','FUNCTION','PACKAGE','PACKAGE BODY',
             'TYPE','TYPE BODY')
ORDER BY owner, name, type, line;
"

# ============================================================
# 13: SYNONYMS
# ============================================================
run_sql "13 Synonyms" "13_synonyms.csv" "
SELECT
    owner, synonym_name, table_owner, table_name, db_link
FROM dba_synonyms
WHERE (owner NOT IN (${SYS_EXCLUDE})
   OR (owner = 'PUBLIC'
       AND table_owner NOT IN (${SYS_EXCLUDE})))
ORDER BY owner, synonym_name;
"

# ============================================================
# 14: SEQUENCES
# ============================================================
run_sql "14 Sequences" "14_sequences.csv" "
SELECT
    sequence_owner, sequence_name,
    min_value, max_value, increment_by,
    last_number, cache_size, cycle_flag, order_flag
FROM dba_sequences
WHERE sequence_owner NOT IN (${SYS_EXCLUDE})
ORDER BY sequence_owner, sequence_name;
"

# ============================================================
# 15: DB LINKS
# ============================================================
run_sql "15 DB Links" "15_db_links.csv" "
SELECT owner, db_link, username, host, created
FROM dba_db_links
ORDER BY owner, db_link;
"

# ============================================================
# 16: OBJECT DEPENDENCIES
# ============================================================
run_sql "16 Dependencies" "16_dependencies.csv" "
SELECT
    owner, name, type,
    referenced_owner, referenced_name, referenced_type,
    referenced_link_name AS via_db_link
FROM dba_dependencies
WHERE owner NOT IN (${SYS_EXCLUDE})
ORDER BY owner, name, referenced_owner, referenced_name;
"

# ============================================================
# 17: ORACLE SCHEDULER JOBS
# ============================================================
run_sql "17 Scheduler Jobs" "17_jobs_scheduler.csv" "
SELECT
    owner, job_name, job_type, job_action,
    schedule_type, repeat_interval, enabled, state,
    last_start_date, last_run_duration, next_run_date,
    run_count, failure_count, comments
FROM dba_scheduler_jobs
WHERE owner NOT IN (${SYS_EXCLUDE})
ORDER BY owner, job_name;
"

# ============================================================
# 18: LEGACY DBMS_JOBS
# ============================================================
run_sql "18 DBMS Jobs" "18_jobs_dbms.csv" "
SELECT
    job, log_user, schema_user,
    what AS job_code, interval,
    last_date, next_date, broken
FROM dba_jobs
ORDER BY job;
"

# ============================================================
# 19: GRANTS (object privileges)
# ============================================================
run_sql "19 Grants" "19_grants.csv" "
SELECT
    grantee, owner, table_name AS object_name,
    privilege, grantable, grantor
FROM dba_tab_privs
WHERE owner NOT IN (${SYS_EXCLUDE})
AND grantee NOT IN (${SYS_EXCLUDE})
ORDER BY grantee, owner, table_name;
"

# ============================================================
# 20: ROLE MEMBERS
# ============================================================
run_sql "20 Role Members" "20_role_members.csv" "
SELECT
    granted_role, grantee, admin_option, default_role
FROM dba_role_privs
WHERE grantee NOT IN (${SYS_EXCLUDE})
ORDER BY grantee, granted_role;
"

# ============================================================
# 21: COMMENTS (semantic definitions)
# ============================================================
run_sql "21 Comments" "21_comments.csv" "
SELECT owner, table_name, table_type, comments
FROM dba_tab_comments
WHERE owner NOT IN (${SYS_EXCLUDE})
AND comments IS NOT NULL
ORDER BY owner, table_name;
"

# ============================================================
# 22: SEGMENT SIZES (volumetrics)
# ============================================================
run_sql "22 Segment Sizes" "22_segments_sizes.csv" "
SELECT
    owner, segment_name, segment_type, tablespace_name,
    ROUND(bytes / 1024 / 1024, 2)        AS size_mb,
    ROUND(bytes / 1024 / 1024 / 1024, 4) AS size_gb
FROM dba_segments
WHERE owner NOT IN (${SYS_EXCLUDE})
AND bytes > 1048576
ORDER BY bytes DESC;
"

# ============================================================
# 23: PARTITIONS
# ============================================================
run_sql "23 Partitions" "23_partitions.csv" "
SELECT
    table_owner, table_name, partition_name,
    high_value, partition_position, tablespace_name,
    num_rows, avg_row_len, last_analyzed
FROM dba_tab_partitions
WHERE table_owner NOT IN (${SYS_EXCLUDE})
ORDER BY table_owner, table_name, partition_position;
"

# ============================================================
# 24: CDC READINESS — Supplemental Logging
# ============================================================
run_sql "24 CDC Readiness" "24_cdc_readiness.csv" "
SELECT
    'DATABASE_LEVEL' AS scope,
    supplemental_log_data_min  AS min_supplemental,
    supplemental_log_data_pk   AS pk_supplemental,
    supplemental_log_data_ui   AS unique_supplemental,
    supplemental_log_data_fk   AS fk_supplemental,
    supplemental_log_data_all  AS all_supplemental,
    force_logging,
    log_mode,
    open_mode,
    database_role
FROM v\$database;
"

# ============================================================
# 25: REDO LOG SIZING
# ============================================================
run_sql "25 Redo Log Sizing" "25_redo_log_sizing.csv" "
SELECT
    group#, bytes / 1024 / 1024 AS size_mb,
    members, status, archived, first_time
FROM v\$log
ORDER BY group#;
"

# ============================================================
# 26: ARCHIVE LOG GENERATION RATE (last 48h)
# ============================================================
run_sql "26 Archive Rate" "26_archive_rate.csv" "
SELECT
    TRUNC(completion_time, 'HH24') AS hora,
    COUNT(*)                       AS archives_generados,
    ROUND(SUM(blocks * block_size) / 1024 / 1024, 2) AS total_mb
FROM v\$archived_log
WHERE completion_time > SYSDATE - 2
GROUP BY TRUNC(completion_time, 'HH24')
ORDER BY hora;
"

# ============================================================
# 27: DATA GUARD STATUS (if applicable)
# ============================================================
run_sql "27 Data Guard Status" "27_dataguard_status.csv" "
SELECT
    dest_id, status, type, database_mode,
    recovery_mode, protection_mode, standby_logfile_count,
    gap_status
FROM v\$archive_dest_status
WHERE status != 'INACTIVE';
"

# ============================================================
# 28: TABLESPACES
# ============================================================
run_sql "28 Tablespaces" "28_tablespaces.csv" "
SELECT
    t.tablespace_name, t.status, t.contents,
    t.logging, t.bigfile, t.encrypted,
    ROUND(d.total_mb, 2)             AS total_mb,
    ROUND(d.total_mb - f.free_mb, 2) AS used_mb,
    ROUND(f.free_mb, 2)              AS free_mb,
    ROUND((d.total_mb - f.free_mb) / d.total_mb * 100, 1) AS pct_used
FROM dba_tablespaces t
JOIN (SELECT tablespace_name, SUM(bytes)/1024/1024 AS total_mb
      FROM dba_data_files GROUP BY tablespace_name) d
    ON t.tablespace_name = d.tablespace_name
LEFT JOIN (SELECT tablespace_name, SUM(bytes)/1024/1024 AS free_mb
           FROM dba_free_space GROUP BY tablespace_name) f
    ON t.tablespace_name = f.tablespace_name
ORDER BY t.tablespace_name;
"

# ============================================================
# 29: INVALID OBJECTS
# ============================================================
run_sql "29 Invalid Objects" "29_invalid_objects.csv" "
SELECT
    owner, object_name, object_type,
    created, last_ddl_time
FROM dba_objects
WHERE status = 'INVALID'
AND owner NOT IN (${SYS_EXCLUDE})
ORDER BY owner, object_type, object_name;
"

# ============================================================
# 30: DATABASE PARAMETERS (non-default)
# ============================================================
run_sql "30 DB Parameters" "30_db_parameters.csv" "
SELECT
    name, value, isdefault, ismodified,
    description
FROM v\$parameter
WHERE isdefault = 'FALSE'
   OR name IN (
       'db_name','db_unique_name','compatible',
       'log_archive_dest%','archive_lag_target',
       'enable_goldengate_replication',
       'streams_pool_size','sga_target','pga_aggregate_target',
       'undo_retention','db_recovery_file_dest_size'
   )
ORDER BY name;
"


# ============================================================
# SUMMARY
# ============================================================
echo "" | tee -a "$LOG"
echo "============================================" | tee -a "$LOG"
echo "RECOLECCION COMPLETADA -- $(date)" | tee -a "$LOG"
echo "============================================" | tee -a "$LOG"
echo "" | tee -a "$LOG"
echo -e "  Queries exitosas: ${GREEN}${TOTAL_OK}${NC}" | tee -a "$LOG"
if [ "$TOTAL_ERR" -gt 0 ]; then
    echo -e "  Queries con error: ${RED}${TOTAL_ERR}${NC} (ver detalles arriba)" | tee -a "$LOG"
fi
echo "" | tee -a "$LOG"
echo "Archivos generados:" | tee -a "$LOG"
ls -lhS "$OUTDIR"/*.csv 2>/dev/null | awk '{print "  "$5" "$NF}' | tee -a "$LOG"
echo "" | tee -a "$LOG"
TOTAL=$(du -sh "$OUTDIR" | cut -f1)
echo "Tamano total: $TOTAL" | tee -a "$LOG"
echo "Log: $LOG" | tee -a "$LOG"
echo "" | tee -a "$LOG"
echo "SIGUIENTE PASO: Comprimir y entregar" | tee -a "$LOG"
echo "  tar czf mep_oracle_\$(basename $OUTDIR).tar.gz $OUTDIR/" | tee -a "$LOG"
