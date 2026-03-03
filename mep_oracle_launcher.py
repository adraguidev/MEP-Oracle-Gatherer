"""
MEP Oracle Gatherer - Recolector automatizado de metadata Oracle
Integratel Peru - Stefanini Group

Ejecutable portable cross-platform (Windows + Linux).
Usa sqlplus para extraer metadata de instancias Oracle 12c+.
"""

import os
import subprocess
import sys
import time
import shutil
import getpass
from datetime import datetime

VERSION = "1.0"

# ---------------------------------------------------------------------------
# SQL Queries — cada tupla: (label, filename, sql)
# ---------------------------------------------------------------------------

# Schemas de sistema a excluir
SYS_EXCLUDE = (
    "'SYS','SYSTEM','DBSNMP','OUTLN','XDB','WMSYS','CTXSYS','MDSYS',"
    "'ORDDATA','ORDSYS','OLAPSYS','EXFSYS','DVSYS','LBACSYS',"
    "'APEX_040200','APEX_PUBLIC_USER','FLOWS_FILES','ANONYMOUS',"
    "'APPQOSSYS','GSMADMIN_INTERNAL','XS$NULL','OJVMSYS','DMSYS',"
    "'GGSYS','GSMUSER','DIP','REMOTE_SCHEDULER_AGENT','SYSBACKUP',"
    "'SYSDG','SYSKM','SYSRAC','AUDSYS','DBSFWUSER','DVF'"
)

QUERIES = [
    ("00 Instance Info", "00_instance_info.csv", f"""
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
FROM v$database d, v$instance i;
"""),

    ("01 Schemas", "01_schemas.csv", f"""
SELECT
    username, account_status, default_tablespace,
    temporary_tablespace, created, profile,
    authentication_type,
    ROUND((SELECT SUM(bytes)/1024/1024 FROM dba_segments WHERE owner = u.username), 2) AS size_mb
FROM dba_users u
WHERE username NOT IN ({SYS_EXCLUDE})
ORDER BY username;
"""),

    ("02 Objects Summary", "02_objects_summary.csv", f"""
SELECT
    owner, object_type,
    COUNT(*)                                                AS cantidad,
    MAX(last_ddl_time)                                      AS ultimo_ddl,
    SUM(CASE WHEN status = 'VALID'   THEN 1 ELSE 0 END)    AS validos,
    SUM(CASE WHEN status = 'INVALID' THEN 1 ELSE 0 END)    AS invalidos
FROM dba_objects
WHERE owner NOT IN ({SYS_EXCLUDE})
GROUP BY owner, object_type
ORDER BY owner, object_type;
"""),

    ("03 Data Dictionary", "03_tables_columns.csv", f"""
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
WHERE t.owner NOT IN ({SYS_EXCLUDE})
ORDER BY t.owner, t.table_name, c.column_id;
"""),

    ("04 Primary Keys", "04_primary_keys.csv", f"""
SELECT
    c.owner, c.table_name, c.constraint_name,
    cc.column_name, cc.position
FROM dba_constraints c
JOIN dba_cons_columns cc ON c.owner = cc.owner
    AND c.constraint_name = cc.constraint_name
WHERE c.constraint_type = 'P'
AND c.owner NOT IN ({SYS_EXCLUDE})
ORDER BY c.owner, c.table_name, cc.position;
"""),

    ("05 Tables No PK", "05_tables_no_pk.csv", f"""
SELECT
    t.owner, t.table_name, t.num_rows, t.avg_row_len,
    t.last_analyzed, 'SIN PRIMARY KEY -- RIESGO CDC' AS alerta
FROM dba_tables t
WHERE NOT EXISTS (
    SELECT 1 FROM dba_constraints c
    WHERE c.owner = t.owner AND c.table_name = t.table_name
    AND c.constraint_type = 'P'
)
AND t.owner NOT IN ({SYS_EXCLUDE})
AND (t.num_rows > 0 OR t.num_rows IS NULL)
ORDER BY t.num_rows DESC NULLS LAST;
"""),

    ("06 Foreign Keys", "06_foreign_keys.csv", f"""
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
AND c.owner NOT IN ({SYS_EXCLUDE})
ORDER BY c.owner, c.table_name, c.constraint_name, cc.position;
"""),

    ("07 Indexes", "07_indexes.csv", f"""
SELECT
    i.owner, i.table_name, i.index_name, i.index_type,
    i.uniqueness, ic.column_name, ic.column_position,
    i.tablespace_name, i.status, i.last_analyzed
FROM dba_indexes i
JOIN dba_ind_columns ic ON i.owner = ic.index_owner AND i.index_name = ic.index_name
WHERE i.owner NOT IN ({SYS_EXCLUDE})
AND i.table_name NOT LIKE 'BIN$%'
ORDER BY i.owner, i.table_name, i.index_name, ic.column_position;
"""),

    ("08 Check Constraints", "08_constraints_check.csv", f"""
SELECT
    owner, table_name, constraint_name,
    search_condition AS constraint_expression,
    status, validated
FROM dba_constraints
WHERE constraint_type = 'C'
AND owner NOT IN ({SYS_EXCLUDE})
AND constraint_name NOT LIKE 'SYS_%'
ORDER BY owner, table_name;
"""),

    ("09 Triggers Code", "09_triggers_code.csv", f"""
SELECT
    t.owner, t.trigger_name, t.trigger_type,
    t.triggering_event, t.table_owner, t.table_name,
    t.base_object_type, t.status, t.action_type,
    t.trigger_body AS codigo_completo
FROM dba_triggers t
WHERE t.owner NOT IN ({SYS_EXCLUDE})
ORDER BY t.owner, t.table_name, t.trigger_name;
"""),

    ("10 Views Code", "10_views_code.csv", f"""
SELECT
    owner, view_name, text_length,
    text AS view_code_completo, read_only
FROM dba_views
WHERE owner NOT IN ({SYS_EXCLUDE})
ORDER BY owner, view_name;
"""),

    ("11 Materialized Views", "11_mviews.csv", f"""
SELECT
    owner, mview_name, container_name,
    query AS mview_query_completo,
    refresh_mode, refresh_method, build_mode,
    fast_refreshable, last_refresh_type,
    last_refresh_date, staleness, compile_state
FROM dba_mviews
WHERE owner NOT IN ({SYS_EXCLUDE})
ORDER BY owner, mview_name;
"""),

    ("12 PL/SQL Code", "12_sp_code.csv", f"""
SELECT
    owner, name, type, line,
    text AS code_line
FROM dba_source
WHERE owner NOT IN ({SYS_EXCLUDE})
AND type IN ('PROCEDURE','FUNCTION','PACKAGE','PACKAGE BODY',
             'TYPE','TYPE BODY')
ORDER BY owner, name, type, line;
"""),

    ("13 Synonyms", "13_synonyms.csv", f"""
SELECT
    owner, synonym_name, table_owner, table_name, db_link
FROM dba_synonyms
WHERE (owner NOT IN ({SYS_EXCLUDE})
   OR (owner = 'PUBLIC'
       AND table_owner NOT IN ({SYS_EXCLUDE})))
ORDER BY owner, synonym_name;
"""),

    ("14 Sequences", "14_sequences.csv", f"""
SELECT
    sequence_owner, sequence_name,
    min_value, max_value, increment_by,
    last_number, cache_size, cycle_flag, order_flag
FROM dba_sequences
WHERE sequence_owner NOT IN ({SYS_EXCLUDE})
ORDER BY sequence_owner, sequence_name;
"""),

    ("15 DB Links", "15_db_links.csv", """
SELECT owner, db_link, username, host, created
FROM dba_db_links
ORDER BY owner, db_link;
"""),

    ("16 Dependencies", "16_dependencies.csv", f"""
SELECT
    owner, name, type,
    referenced_owner, referenced_name, referenced_type,
    referenced_link_name AS via_db_link
FROM dba_dependencies
WHERE owner NOT IN ({SYS_EXCLUDE})
ORDER BY owner, name, referenced_owner, referenced_name;
"""),

    ("17 Scheduler Jobs", "17_jobs_scheduler.csv", f"""
SELECT
    owner, job_name, job_type, job_action,
    schedule_type, repeat_interval, enabled, state,
    last_start_date, last_run_duration, next_run_date,
    run_count, failure_count, comments
FROM dba_scheduler_jobs
WHERE owner NOT IN ({SYS_EXCLUDE})
ORDER BY owner, job_name;
"""),

    ("18 DBMS Jobs", "18_jobs_dbms.csv", """
SELECT
    job, log_user, schema_user,
    what AS job_code, interval,
    last_date, next_date, broken
FROM dba_jobs
ORDER BY job;
"""),

    ("19 Grants", "19_grants.csv", f"""
SELECT
    grantee, owner, table_name AS object_name,
    privilege, grantable, grantor
FROM dba_tab_privs
WHERE owner NOT IN ({SYS_EXCLUDE})
AND grantee NOT IN ({SYS_EXCLUDE})
ORDER BY grantee, owner, table_name;
"""),

    ("20 Role Members", "20_role_members.csv", f"""
SELECT
    granted_role, grantee, admin_option, default_role
FROM dba_role_privs
WHERE grantee NOT IN ({SYS_EXCLUDE})
ORDER BY grantee, granted_role;
"""),

    ("21 Comments", "21_comments.csv", f"""
SELECT owner, table_name, table_type, comments
FROM dba_tab_comments
WHERE owner NOT IN ({SYS_EXCLUDE})
AND comments IS NOT NULL
ORDER BY owner, table_name;
"""),

    ("22 Segment Sizes", "22_segments_sizes.csv", f"""
SELECT
    owner, segment_name, segment_type, tablespace_name,
    ROUND(bytes / 1024 / 1024, 2)        AS size_mb,
    ROUND(bytes / 1024 / 1024 / 1024, 4) AS size_gb
FROM dba_segments
WHERE owner NOT IN ({SYS_EXCLUDE})
AND bytes > 1048576
ORDER BY bytes DESC;
"""),

    ("23 Partitions", "23_partitions.csv", f"""
SELECT
    table_owner, table_name, partition_name,
    high_value, partition_position, tablespace_name,
    num_rows, avg_row_len, last_analyzed
FROM dba_tab_partitions
WHERE table_owner NOT IN ({SYS_EXCLUDE})
ORDER BY table_owner, table_name, partition_position;
"""),

    ("24 CDC Readiness", "24_cdc_readiness.csv", """
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
FROM v$database;
"""),

    ("25 Redo Log Sizing", "25_redo_log_sizing.csv", """
SELECT
    group#, bytes / 1024 / 1024 AS size_mb,
    members, status, archived, first_time
FROM v$log
ORDER BY group#;
"""),

    ("26 Archive Rate", "26_archive_rate.csv", """
SELECT
    TRUNC(completion_time, 'HH24') AS hora,
    COUNT(*)                       AS archives_generados,
    ROUND(SUM(blocks * block_size) / 1024 / 1024, 2) AS total_mb
FROM v$archived_log
WHERE completion_time > SYSDATE - 2
GROUP BY TRUNC(completion_time, 'HH24')
ORDER BY hora;
"""),

    ("27 Data Guard Status", "27_dataguard_status.csv", """
SELECT
    dest_id, status, type, database_mode,
    recovery_mode, protection_mode, standby_logfile_count,
    gap_status
FROM v$archive_dest_status
WHERE status != 'INACTIVE';
"""),

    ("28 Tablespaces", "28_tablespaces.csv", """
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
"""),

    ("29 Invalid Objects", "29_invalid_objects.csv", f"""
SELECT
    owner, object_name, object_type,
    created, last_ddl_time
FROM dba_objects
WHERE status = 'INVALID'
AND owner NOT IN ({SYS_EXCLUDE})
ORDER BY owner, object_type, object_name;
"""),

    ("30 DB Parameters", "30_db_parameters.csv", """
SELECT
    name, value, isdefault, ismodified,
    description
FROM v$parameter
WHERE isdefault = 'FALSE'
   OR name IN (
       'db_name','db_unique_name','compatible',
       'log_archive_dest%','archive_lag_target',
       'enable_goldengate_replication',
       'streams_pool_size','sga_target','pga_aggregate_target',
       'undo_retention','db_recovery_file_dest_size'
   )
ORDER BY name;
"""),
]


# ---------------------------------------------------------------------------
# Console helpers
# ---------------------------------------------------------------------------

def clear_screen():
    os.system("cls" if os.name == "nt" else "clear")


def find_sqlplus() -> str:
    """Find sqlplus executable. Returns path or empty string."""
    path = shutil.which("sqlplus")
    if path:
        return path

    # Windows: check common Oracle install paths
    if os.name == "nt":
        for drive in ["C", "D"]:
            for base in [
                f"{drive}:\\oracle",
                f"{drive}:\\app\\oracle",
                f"{drive}:\\oraclexe",
            ]:
                if os.path.isdir(base):
                    for root, dirs, files in os.walk(base):
                        if "sqlplus.exe" in files:
                            return os.path.join(root, "sqlplus.exe")

    # Linux: check ORACLE_HOME
    oracle_home = os.environ.get("ORACLE_HOME", "")
    if oracle_home:
        candidate = os.path.join(oracle_home, "bin", "sqlplus")
        if os.path.isfile(candidate):
            return candidate

    return ""


def check_sqlplus() -> str:
    """Validate sqlplus is available. Returns path or exits."""
    sqlplus = find_sqlplus()
    if not sqlplus:
        print()
        print("  [ERROR] sqlplus no encontrado en PATH.")
        print()
        print("  Opciones de instalacion:")
        if os.name == "nt":
            print("    1) Oracle Instant Client para Windows:")
            print("       https://www.oracle.com/database/technologies/instant-client/winx64-64-downloads.html")
            print("       Descargar: instantclient-basic + instantclient-sqlplus")
            print("       Agregar la carpeta al PATH del sistema")
        else:
            print("    1) Oracle Instant Client:")
            print("       https://www.oracle.com/database/technologies/instant-client/downloads.html")
            print("       Descargar: instantclient-basic + instantclient-sqlplus")
            print("       export PATH=$ORACLE_HOME/bin:$PATH")
        print()
        print("    2) Si Oracle Client ya esta instalado, verificar que")
        print("       el directorio bin/ este en el PATH.")
        print()
        print("  Verificar con: sqlplus -V")
        input("\n  Presione ENTER para salir...")
        sys.exit(1)

    # Get version
    try:
        result = subprocess.run(
            [sqlplus, "-V"],
            capture_output=True, text=True, timeout=10
        )
        version_line = result.stdout.strip().split("\n")[0] if result.stdout else "sqlplus encontrado"
    except Exception:
        version_line = "sqlplus encontrado"

    print(f"  [OK] {version_line}")
    return sqlplus


def test_connection(sqlplus: str, tns: str, ora_user: str, ora_pass: str) -> bool:
    """Test Oracle connectivity. Returns True if successful."""
    print(f"  Probando conexion a {ora_user}@{tns}... ", end="", flush=True)

    sql_input = (
        "SET PAGESIZE 0\n"
        "SET FEEDBACK OFF\n"
        "SET HEADING OFF\n"
        "SELECT 'CONNECTION_OK' FROM dual;\n"
        "EXIT;\n"
    )

    try:
        result = subprocess.run(
            [sqlplus, "-s", f"{ora_user}/{ora_pass}@{tns}"],
            input=sql_input,
            capture_output=True, text=True, timeout=30
        )

        if "CONNECTION_OK" in result.stdout:
            print("OK")
            return True
        else:
            print("FALLO")
            print()
            # Show error details
            for line in result.stdout.split("\n"):
                line = line.strip()
                if line.upper().startswith(("ORA-", "SP2-", "ERROR")):
                    print(f"    {line}")
            print()
            print("  Verifique:")
            print(f"    - TNS alias correcto (tnsping {tns})")
            print("    - Usuario y password")
            print("    - Permisos: SELECT ANY DICTIONARY, SELECT_CATALOG_ROLE")
            return False

    except subprocess.TimeoutExpired:
        print("TIMEOUT")
        print(f"    No se pudo conectar a {tns} en 30 segundos.")
        print("    Verifique la conectividad de red y el TNS alias.")
        return False
    except Exception as e:
        print(f"ERROR: {e}")
        return False


# ---------------------------------------------------------------------------
# Query execution
# ---------------------------------------------------------------------------

def run_sql(sqlplus: str, tns: str, ora_user: str, ora_pass: str,
            outdir: str, label: str, filename: str, sql: str,
            log_file) -> bool:
    """Execute a SQL query via sqlplus and save as CSV. Returns True on success."""
    print(f"  [{label}] ... ", end="", flush=True)
    log_file.write(f"  [{label}] ... ")
    start = time.time()

    sql_input = (
        "SET MARKUP CSV ON QUOTE ON\n"
        "SET PAGESIZE 0\n"
        "SET LINESIZE 32767\n"
        "SET LONG 1000000\n"
        "SET LONGCHUNKSIZE 200000\n"
        "SET TRIMSPOOL ON\n"
        "SET FEEDBACK OFF\n"
        "SET HEADING ON\n"
        "SET TERMOUT OFF\n"
        f"\n{sql}\n"
        "EXIT;\n"
    )

    try:
        result = subprocess.run(
            [sqlplus, "-s", f"{ora_user}/{ora_pass}@{tns}"],
            input=sql_input,
            capture_output=True, text=True, timeout=600
        )
        elapsed = int(time.time() - start)
        output = result.stdout

        # Check for Oracle errors
        has_errors = False
        error_lines = []
        for line in output.split("\n"):
            stripped = line.strip()
            if stripped.upper().startswith(("ORA-", "SP2-")):
                has_errors = True
                error_lines.append(stripped)

        # Write output to file regardless
        outpath = os.path.join(outdir, filename)
        with open(outpath, "w", encoding="utf-8") as f:
            f.write(output)

        if has_errors:
            msg = f"ERROR ({elapsed}s)\n"
            print(msg, end="")
            log_file.write(msg)
            for err in error_lines[:3]:
                err_detail = f"    {err}\n"
                print(err_detail, end="")
                log_file.write(err_detail)
            return False
        else:
            rows = len([l for l in output.split("\n") if l.strip()])
            msg = f"OK ({rows} filas, {elapsed}s)\n"
            print(msg, end="")
            log_file.write(msg)
            return True

    except subprocess.TimeoutExpired:
        elapsed = int(time.time() - start)
        msg = f"TIMEOUT ({elapsed}s) - query excedio 10 minutos\n"
        print(msg, end="")
        log_file.write(msg)
        return False
    except Exception as e:
        elapsed = int(time.time() - start)
        msg = f"ERROR ({elapsed}s) - {e}\n"
        print(msg, end="")
        log_file.write(msg)
        return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    clear_screen()
    print("=" * 62)
    print(f"  MEP Oracle Gatherer v{VERSION} -- Stefanini Group")
    print("  Recolector automatizado de metadata Oracle")
    print("=" * 62)
    print()

    # Step 1: Validate sqlplus
    sqlplus = check_sqlplus()
    print()

    # Step 2: Connection details (args or interactive)
    if len(sys.argv) >= 3:
        tns = sys.argv[1]
        ora_user = sys.argv[2]
        outdir = sys.argv[3] if len(sys.argv) >= 4 else None
    else:
        print("  CONEXION ORACLE")
        print("  " + "-" * 40)
        tns = input("  TNS Alias (ej: PROD, orcl, //host:1521/service): ").strip()
        if not tns:
            print("\n  [ERROR] TNS alias requerido.")
            input("\n  Presione ENTER para salir...")
            sys.exit(1)

        ora_user = input("  Usuario (ej: mep_reader): ").strip()
        if not ora_user:
            print("\n  [ERROR] Usuario requerido.")
            input("\n  Presione ENTER para salir...")
            sys.exit(1)

        outdir = None

    # Password (always interactive for security)
    ora_pass = getpass.getpass(f"  Password para {ora_user}@{tns}: ")
    if not ora_pass:
        print("\n  [ERROR] Password requerido.")
        input("\n  Presione ENTER para salir...")
        sys.exit(1)

    print()

    # Step 3: Test connection
    if not test_connection(sqlplus, tns, ora_user, ora_pass):
        input("\n  Presione ENTER para salir...")
        sys.exit(1)

    # Step 4: Output directory
    if not outdir:
        outdir = os.path.join(".", f"mep_oracle_{datetime.now().strftime('%Y%m%d_%H%M%S')}")

    os.makedirs(outdir, exist_ok=True)
    print()
    print(f"  Output: {os.path.abspath(outdir)}")
    print()

    # Confirm
    if len(sys.argv) < 3:
        input("  Presione ENTER para iniciar la recoleccion (Ctrl+C para cancelar)...")
        print()

    # Step 5: Execute queries
    log_path = os.path.join(outdir, "gather_oracle.log")
    total_ok = 0
    total_err = 0

    with open(log_path, "w", encoding="utf-8") as log_file:
        header = (
            f"{'=' * 50}\n"
            f"MEP Oracle Gatherer v{VERSION} -- {datetime.now()}\n"
            f"Instance: {ora_user}@{tns}\n"
            f"Output:   {os.path.abspath(outdir)}\n"
            f"{'=' * 50}\n\n"
        )
        print(header, end="")
        log_file.write(header)

        for label, filename, sql in QUERIES:
            success = run_sql(
                sqlplus, tns, ora_user, ora_pass,
                outdir, label, filename, sql, log_file
            )
            if success:
                total_ok += 1
            else:
                total_err += 1

        # Summary
        print()
        log_file.write("\n")

        summary = f"{'=' * 50}\n"
        summary += f"RECOLECCION COMPLETADA -- {datetime.now()}\n"
        summary += f"{'=' * 50}\n\n"
        summary += f"  Queries exitosas: {total_ok}\n"
        if total_err > 0:
            summary += f"  Queries con error: {total_err} (ver detalles arriba)\n"
        summary += "\n"

        print(summary, end="")
        log_file.write(summary)

        # List generated files
        files_header = "Archivos generados:\n"
        print(files_header, end="")
        log_file.write(files_header)

        csv_files = sorted([
            f for f in os.listdir(outdir)
            if f.endswith(".csv")
        ])
        for fname in csv_files:
            fpath = os.path.join(outdir, fname)
            size = os.path.getsize(fpath)
            if size > 1024 * 1024:
                size_str = f"{size / 1024 / 1024:.1f} MB"
            elif size > 1024:
                size_str = f"{size / 1024:.1f} KB"
            else:
                size_str = f"{size} B"
            line = f"  {size_str:>10}  {fname}\n"
            print(line, end="")
            log_file.write(line)

        # Total size
        total_size = sum(
            os.path.getsize(os.path.join(outdir, f))
            for f in os.listdir(outdir)
        )
        if total_size > 1024 * 1024:
            total_str = f"{total_size / 1024 / 1024:.1f} MB"
        else:
            total_str = f"{total_size / 1024:.1f} KB"

        footer = (
            f"\nTamano total: {total_str}\n"
            f"Log: {log_path}\n\n"
            f"SIGUIENTE PASO: Comprimir y entregar\n"
        )
        if os.name == "nt":
            footer += f'  powershell Compress-Archive -Path "{outdir}" -DestinationPath "mep_oracle.zip"\n'
        else:
            footer += f"  tar czf mep_oracle_$(basename {outdir}).tar.gz {outdir}/\n"

        print(footer)
        log_file.write(footer)

    # Clean password from memory
    ora_pass = ""

    # Wait for user on interactive mode
    print()
    input("  Presione ENTER para salir...")


if __name__ == "__main__":
    main()
