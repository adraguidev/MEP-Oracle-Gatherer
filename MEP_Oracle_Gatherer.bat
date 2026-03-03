@echo off
setlocal EnableDelayedExpansion
REM ============================================================
REM MEP Oracle Gatherer — Recolector Automatizado de Metadata
REM Integratel Peru - Stefanini Group
REM
REM Script standalone para Windows. Solo requiere sqlplus en PATH.
REM No necesita Python ni el .exe compilado.
REM
REM USO INTERACTIVO:
REM   MEP_Oracle_Gatherer.bat
REM
REM USO CLI:
REM   MEP_Oracle_Gatherer.bat <TNS_ALIAS> <USERNAME> [OUTPUT_DIR]
REM
REM COMPATIBLE: Oracle 12c+ (optimizado para 19c)
REM ============================================================

title MEP Oracle Gatherer -- Stefanini Group

set "VERSION=1.0"
set "TOTAL_OK=0"
set "TOTAL_ERR=0"
set "SYS_EXCLUDE='SYS','SYSTEM','DBSNMP','OUTLN','XDB','WMSYS','CTXSYS','MDSYS','ORDDATA','ORDSYS','OLAPSYS','EXFSYS','DVSYS','LBACSYS','APEX_040200','APEX_PUBLIC_USER','FLOWS_FILES','ANONYMOUS','APPQOSSYS','GSMADMIN_INTERNAL','XS$NULL','OJVMSYS','DMSYS','GGSYS','GSMUSER','DIP','REMOTE_SCHEDULER_AGENT','SYSBACKUP','SYSDG','SYSKM','SYSRAC','AUDSYS','DBSFWUSER','DVF'"

REM ---- Check sqlplus ----
set "SQLPLUS_CMD="
where sqlplus >nul 2>&1
if %errorlevel% equ 0 (
    set "SQLPLUS_CMD=sqlplus"
    goto :sqlplus_found
)

REM Check common Oracle paths
for %%D in (C D) do (
    for %%P in ("%%D:\oracle" "%%D:\app\oracle" "%%D:\oraclexe") do (
        if exist "%%~P\*" (
            for /f "delims=" %%F in ('dir /s /b "%%~P\sqlplus.exe" 2^>nul') do (
                set "SQLPLUS_CMD=%%F"
                goto :sqlplus_found
            )
        )
    )
)

if defined ORACLE_HOME (
    if exist "%ORACLE_HOME%\bin\sqlplus.exe" (
        set "SQLPLUS_CMD=%ORACLE_HOME%\bin\sqlplus.exe"
        goto :sqlplus_found
    )
)

echo.
echo   [ERROR] sqlplus no encontrado en PATH.
echo.
echo   Opciones:
echo     1^) Oracle Instant Client para Windows:
echo        https://www.oracle.com/database/technologies/instant-client/winx64-64-downloads.html
echo        Descargar: instantclient-basic + instantclient-sqlplus
echo        Agregar la carpeta al PATH del sistema
echo     2^) Si Oracle Client ya esta instalado, agregar bin\ al PATH
echo.
echo   Verificar con: sqlplus -V
echo.
pause
exit /b 1

:sqlplus_found
for /f "delims=" %%V in ('"%SQLPLUS_CMD%" -V 2^>nul') do (
    set "SQLPLUS_VER=%%V"
    goto :show_version
)
set "SQLPLUS_VER=sqlplus encontrado"
:show_version
echo   [OK] %SQLPLUS_VER%

REM ---- Parse arguments or interactive mode ----
if "%~1"=="" goto :interactive
if "%~2"=="" (
    echo USO:
    echo   Interactivo:  %~nx0
    echo   CLI:          %~nx0 ^<TNS_ALIAS^> ^<USERNAME^> [OUTPUT_DIR]
    pause
    exit /b 1
)

REM CLI mode
set "TNS=%~1"
set "ORA_USER=%~2"
if "%~3"=="" (
    for /f "tokens=1-5 delims=/:. " %%a in ("%date% %time%") do (
        set "OUTDIR=.\mep_oracle_%%c%%b%%a_%%d%%e"
    )
) else (
    set "OUTDIR=%~3"
)
goto :get_password

:interactive
cls
echo ==============================================================
echo   MEP Oracle Gatherer v%VERSION% -- Stefanini Group
echo   Recolector automatizado de metadata Oracle
echo ==============================================================
echo.
echo   %SQLPLUS_VER%
echo.
echo   CONEXION ORACLE
echo   ----------------------------------------
set /p "TNS=  TNS Alias (ej: PROD, orcl, //host:1521/service): "
if "!TNS!"=="" (
    echo.
    echo   [ERROR] TNS alias requerido.
    pause
    exit /b 1
)
set /p "ORA_USER=  Usuario (ej: mep_reader): "
if "!ORA_USER!"=="" (
    echo.
    echo   [ERROR] Usuario requerido.
    pause
    exit /b 1
)
for /f "tokens=1-5 delims=/:. " %%a in ("%date% %time%") do (
    set "OUTDIR=.\mep_oracle_%%c%%b%%a_%%d%%e"
)

:get_password
REM Password (always interactive)
set /p "ORA_PASS=  Password para !ORA_USER!@!TNS!: "
if "!ORA_PASS!"=="" (
    echo.
    echo   [ERROR] Password requerido.
    pause
    exit /b 1
)
echo.

REM ---- Test connection ----
echo   Probando conexion a !ORA_USER!@!TNS!...
set "TMPSQL=%TEMP%\mep_test_%RANDOM%.sql"
(
    echo SET PAGESIZE 0
    echo SET FEEDBACK OFF
    echo SET HEADING OFF
    echo SELECT 'CONNECTION_OK' FROM dual;
    echo EXIT;
) > "!TMPSQL!"

for /f "delims=" %%R in ('"!SQLPLUS_CMD!" -s "!ORA_USER!/!ORA_PASS!@!TNS!" @"!TMPSQL!" 2^>^&1') do (
    set "CONN_RESULT=%%R"
    echo %%R | findstr /i "CONNECTION_OK" >nul 2>&1
    if !errorlevel! equ 0 (
        echo   OK
        del "!TMPSQL!" 2>nul
        goto :connection_ok
    )
)

echo   FALLO
echo.
echo   Verifique:
echo     - TNS alias correcto
echo     - Usuario y password
echo     - Permisos: SELECT ANY DICTIONARY, SELECT_CATALOG_ROLE
del "!TMPSQL!" 2>nul
pause
exit /b 1

:connection_ok
echo.

REM ---- Setup output directory ----
if not exist "!OUTDIR!" mkdir "!OUTDIR!"
set "LOG=!OUTDIR!\gather_oracle.log"

echo ============================================ > "!LOG!"
echo MEP Oracle Gatherer v%VERSION% -- %date% %time% >> "!LOG!"
echo Instance: !ORA_USER!@!TNS! >> "!LOG!"
echo Output:   !OUTDIR! >> "!LOG!"
echo ============================================ >> "!LOG!"
echo. >> "!LOG!"

echo ============================================
echo MEP Oracle Gatherer v%VERSION% -- %date% %time%
echo Instance: !ORA_USER!@!TNS!
echo Output:   !OUTDIR!
echo ============================================
echo.

if "%~1"=="" (
    echo   Presione ENTER para iniciar la recoleccion ^(Ctrl+C para cancelar^)...
    pause >nul
    echo.
)

REM ============================================================
REM QUERIES — 31 recolecciones
REM ============================================================

call :run_query "00 Instance Info" "00_instance_info.csv" "SELECT d.name AS db_name, d.db_unique_name, d.platform_name, d.created, i.version_full AS oracle_version, i.instance_name, i.host_name, i.startup_time, i.status, d.log_mode, d.force_logging, d.guard_status, d.supplemental_log_data_min, d.supplemental_log_data_pk, d.supplemental_log_data_ui, d.supplemental_log_data_fk, d.supplemental_log_data_all, d.open_mode, d.database_role, d.dataguard_broker FROM v$database d, v$instance i;"

call :run_query "01 Schemas" "01_schemas.csv" "SELECT username, account_status, default_tablespace, temporary_tablespace, created, profile, authentication_type, ROUND((SELECT SUM(bytes)/1024/1024 FROM dba_segments WHERE owner = u.username), 2) AS size_mb FROM dba_users u WHERE username NOT IN (%SYS_EXCLUDE%) ORDER BY username;"

call :run_query "02 Objects Summary" "02_objects_summary.csv" "SELECT owner, object_type, COUNT(*) AS cantidad, MAX(last_ddl_time) AS ultimo_ddl, SUM(CASE WHEN status = 'VALID' THEN 1 ELSE 0 END) AS validos, SUM(CASE WHEN status = 'INVALID' THEN 1 ELSE 0 END) AS invalidos FROM dba_objects WHERE owner NOT IN (%SYS_EXCLUDE%) GROUP BY owner, object_type ORDER BY owner, object_type;"

REM Query 03 is large — use temp file approach
set "TMPSQL03=%TEMP%\mep_q03_%RANDOM%.sql"
(
    echo SET MARKUP CSV ON QUOTE ON
    echo SET PAGESIZE 0
    echo SET LINESIZE 32767
    echo SET LONG 1000000
    echo SET LONGCHUNKSIZE 200000
    echo SET TRIMSPOOL ON
    echo SET FEEDBACK OFF
    echo SET HEADING ON
    echo SET TERMOUT OFF
    echo SELECT
    echo     t.owner, t.table_name, t.num_rows, t.avg_row_len,
    echo     t.last_analyzed, t.tablespace_name, t.partitioned,
    echo     t.temporary, t.iot_type,
    echo     NVL(tc.comments, ''^) AS table_comment,
    echo     c.column_name, c.column_id, c.data_type, c.data_length,
    echo     c.data_precision, c.data_scale, c.nullable, c.data_default,
    echo     NVL(cc.comments, ''^) AS column_comment,
    echo     CASE
    echo         WHEN t.table_name LIKE 'BL_%%'  THEN 'BILLING'
    echo         WHEN t.table_name LIKE 'CM_%%'  THEN 'CUSTOMER_MGMT'
    echo         WHEN t.table_name LIKE 'PM_%%'  THEN 'PRODUCT_MGMT'
    echo         WHEN t.table_name LIKE 'OM_%%'  THEN 'ORDER_MGMT'
    echo         WHEN t.table_name LIKE 'RM_%%'  THEN 'RESOURCE_MGMT'
    echo         WHEN t.table_name LIKE 'PR_%%'  THEN 'PROVISIONING'
    echo         WHEN t.table_name LIKE 'AR_%%'  THEN 'ACCOUNTS_RECV'
    echo         WHEN t.table_name LIKE 'GL_%%'  THEN 'GENERAL_LEDGER'
    echo         WHEN t.table_name LIKE 'INV_%%' THEN 'INVOICE'
    echo         WHEN t.table_name LIKE 'PAY_%%' THEN 'PAYMENTS'
    echo         WHEN t.table_name LIKE 'ADJ_%%' THEN 'ADJUSTMENTS'
    echo         WHEN t.table_name LIKE 'RT_%%'  THEN 'RATING'
    echo         WHEN t.table_name LIKE 'CH_%%'  THEN 'CHARGING'
    echo         WHEN t.table_name LIKE 'MED_%%' THEN 'MEDIATION'
    echo         WHEN t.table_name LIKE 'CDR_%%' THEN 'CDR_EDR'
    echo         WHEN t.table_name LIKE 'REF_%%' OR t.table_name LIKE 'LU_%%' THEN 'REFERENCE_DATA'
    echo         WHEN t.table_name LIKE 'CFG_%%' OR t.table_name LIKE 'CONF_%%' THEN 'CONFIG'
    echo         WHEN t.table_name LIKE 'AUD_%%' OR t.table_name LIKE 'LOG_%%'  THEN 'AUDIT_LOG'
    echo         ELSE 'OTHER'
    echo     END AS domain_hint
    echo FROM dba_tables t
    echo JOIN dba_tab_columns c ON t.owner = c.owner AND t.table_name = c.table_name
    echo LEFT JOIN dba_tab_comments tc ON t.owner = tc.owner AND t.table_name = tc.table_name
    echo LEFT JOIN dba_col_comments cc ON c.owner = cc.owner
    echo     AND c.table_name = cc.table_name AND c.column_name = cc.column_name
    echo WHERE t.owner NOT IN (%SYS_EXCLUDE%^)
    echo ORDER BY t.owner, t.table_name, c.column_id;
    echo EXIT;
) > "!TMPSQL03!"
call :run_query_file "03 Data Dictionary" "03_tables_columns.csv" "!TMPSQL03!"

call :run_query "04 Primary Keys" "04_primary_keys.csv" "SELECT c.owner, c.table_name, c.constraint_name, cc.column_name, cc.position FROM dba_constraints c JOIN dba_cons_columns cc ON c.owner = cc.owner AND c.constraint_name = cc.constraint_name WHERE c.constraint_type = 'P' AND c.owner NOT IN (%SYS_EXCLUDE%) ORDER BY c.owner, c.table_name, cc.position;"

call :run_query "05 Tables No PK" "05_tables_no_pk.csv" "SELECT t.owner, t.table_name, t.num_rows, t.avg_row_len, t.last_analyzed, 'SIN PRIMARY KEY -- RIESGO CDC' AS alerta FROM dba_tables t WHERE NOT EXISTS (SELECT 1 FROM dba_constraints c WHERE c.owner = t.owner AND c.table_name = t.table_name AND c.constraint_type = 'P') AND t.owner NOT IN (%SYS_EXCLUDE%) AND (t.num_rows > 0 OR t.num_rows IS NULL) ORDER BY t.num_rows DESC NULLS LAST;"

REM Query 06 — Foreign Keys (complex JOIN)
set "TMPSQL06=%TEMP%\mep_q06_%RANDOM%.sql"
(
    echo SET MARKUP CSV ON QUOTE ON
    echo SET PAGESIZE 0
    echo SET LINESIZE 32767
    echo SET LONG 1000000
    echo SET LONGCHUNKSIZE 200000
    echo SET TRIMSPOOL ON
    echo SET FEEDBACK OFF
    echo SET HEADING ON
    echo SET TERMOUT OFF
    echo SELECT
    echo     c.owner, c.table_name AS child_table, c.constraint_name,
    echo     cc.column_name AS fk_column, cc.position,
    echo     r.owner AS parent_owner, r.table_name AS parent_table,
    echo     rc.column_name AS parent_column,
    echo     c.delete_rule, c.status
    echo FROM dba_constraints c
    echo JOIN dba_cons_columns cc ON c.owner = cc.owner AND c.constraint_name = cc.constraint_name
    echo JOIN dba_constraints r ON c.r_owner = r.owner AND c.r_constraint_name = r.constraint_name
    echo JOIN dba_cons_columns rc ON r.owner = rc.owner AND r.constraint_name = rc.constraint_name AND cc.position = rc.position
    echo WHERE c.constraint_type = 'R'
    echo AND c.owner NOT IN (%SYS_EXCLUDE%^)
    echo ORDER BY c.owner, c.table_name, c.constraint_name, cc.position;
    echo EXIT;
) > "!TMPSQL06!"
call :run_query_file "06 Foreign Keys" "06_foreign_keys.csv" "!TMPSQL06!"

call :run_query "07 Indexes" "07_indexes.csv" "SELECT i.owner, i.table_name, i.index_name, i.index_type, i.uniqueness, ic.column_name, ic.column_position, i.tablespace_name, i.status, i.last_analyzed FROM dba_indexes i JOIN dba_ind_columns ic ON i.owner = ic.index_owner AND i.index_name = ic.index_name WHERE i.owner NOT IN (%SYS_EXCLUDE%) AND i.table_name NOT LIKE 'BIN$%%' ORDER BY i.owner, i.table_name, i.index_name, ic.column_position;"

call :run_query "08 Check Constraints" "08_constraints_check.csv" "SELECT owner, table_name, constraint_name, search_condition AS constraint_expression, status, validated FROM dba_constraints WHERE constraint_type = 'C' AND owner NOT IN (%SYS_EXCLUDE%) AND constraint_name NOT LIKE 'SYS_%%' ORDER BY owner, table_name;"

call :run_query "09 Triggers Code" "09_triggers_code.csv" "SELECT t.owner, t.trigger_name, t.trigger_type, t.triggering_event, t.table_owner, t.table_name, t.base_object_type, t.status, t.action_type, t.trigger_body AS codigo_completo FROM dba_triggers t WHERE t.owner NOT IN (%SYS_EXCLUDE%) ORDER BY t.owner, t.table_name, t.trigger_name;"

call :run_query "10 Views Code" "10_views_code.csv" "SELECT owner, view_name, text_length, text AS view_code_completo, read_only FROM dba_views WHERE owner NOT IN (%SYS_EXCLUDE%) ORDER BY owner, view_name;"

call :run_query "11 Materialized Views" "11_mviews.csv" "SELECT owner, mview_name, container_name, query AS mview_query_completo, refresh_mode, refresh_method, build_mode, fast_refreshable, last_refresh_type, last_refresh_date, staleness, compile_state FROM dba_mviews WHERE owner NOT IN (%SYS_EXCLUDE%) ORDER BY owner, mview_name;"

call :run_query "12 PL/SQL Code" "12_sp_code.csv" "SELECT owner, name, type, line, text AS code_line FROM dba_source WHERE owner NOT IN (%SYS_EXCLUDE%) AND type IN ('PROCEDURE','FUNCTION','PACKAGE','PACKAGE BODY','TYPE','TYPE BODY') ORDER BY owner, name, type, line;"

REM Query 13 — Synonyms (complex WHERE with OR)
set "TMPSQL13=%TEMP%\mep_q13_%RANDOM%.sql"
(
    echo SET MARKUP CSV ON QUOTE ON
    echo SET PAGESIZE 0
    echo SET LINESIZE 32767
    echo SET LONG 1000000
    echo SET LONGCHUNKSIZE 200000
    echo SET TRIMSPOOL ON
    echo SET FEEDBACK OFF
    echo SET HEADING ON
    echo SET TERMOUT OFF
    echo SELECT
    echo     owner, synonym_name, table_owner, table_name, db_link
    echo FROM dba_synonyms
    echo WHERE (owner NOT IN (%SYS_EXCLUDE%^)
    echo    OR (owner = 'PUBLIC'
    echo        AND table_owner NOT IN (%SYS_EXCLUDE%^)^)^)
    echo ORDER BY owner, synonym_name;
    echo EXIT;
) > "!TMPSQL13!"
call :run_query_file "13 Synonyms" "13_synonyms.csv" "!TMPSQL13!"

call :run_query "14 Sequences" "14_sequences.csv" "SELECT sequence_owner, sequence_name, min_value, max_value, increment_by, last_number, cache_size, cycle_flag, order_flag FROM dba_sequences WHERE sequence_owner NOT IN (%SYS_EXCLUDE%) ORDER BY sequence_owner, sequence_name;"

call :run_query "15 DB Links" "15_db_links.csv" "SELECT owner, db_link, username, host, created FROM dba_db_links ORDER BY owner, db_link;"

call :run_query "16 Dependencies" "16_dependencies.csv" "SELECT owner, name, type, referenced_owner, referenced_name, referenced_type, referenced_link_name AS via_db_link FROM dba_dependencies WHERE owner NOT IN (%SYS_EXCLUDE%) ORDER BY owner, name, referenced_owner, referenced_name;"

call :run_query "17 Scheduler Jobs" "17_jobs_scheduler.csv" "SELECT owner, job_name, job_type, job_action, schedule_type, repeat_interval, enabled, state, last_start_date, last_run_duration, next_run_date, run_count, failure_count, comments FROM dba_scheduler_jobs WHERE owner NOT IN (%SYS_EXCLUDE%) ORDER BY owner, job_name;"

call :run_query "18 DBMS Jobs" "18_jobs_dbms.csv" "SELECT job, log_user, schema_user, what AS job_code, interval, last_date, next_date, broken FROM dba_jobs ORDER BY job;"

call :run_query "19 Grants" "19_grants.csv" "SELECT grantee, owner, table_name AS object_name, privilege, grantable, grantor FROM dba_tab_privs WHERE owner NOT IN (%SYS_EXCLUDE%) AND grantee NOT IN (%SYS_EXCLUDE%) ORDER BY grantee, owner, table_name;"

call :run_query "20 Role Members" "20_role_members.csv" "SELECT granted_role, grantee, admin_option, default_role FROM dba_role_privs WHERE grantee NOT IN (%SYS_EXCLUDE%) ORDER BY grantee, granted_role;"

call :run_query "21 Comments" "21_comments.csv" "SELECT owner, table_name, table_type, comments FROM dba_tab_comments WHERE owner NOT IN (%SYS_EXCLUDE%) AND comments IS NOT NULL ORDER BY owner, table_name;"

call :run_query "22 Segment Sizes" "22_segments_sizes.csv" "SELECT owner, segment_name, segment_type, tablespace_name, ROUND(bytes / 1024 / 1024, 2) AS size_mb, ROUND(bytes / 1024 / 1024 / 1024, 4) AS size_gb FROM dba_segments WHERE owner NOT IN (%SYS_EXCLUDE%) AND bytes > 1048576 ORDER BY bytes DESC;"

call :run_query "23 Partitions" "23_partitions.csv" "SELECT table_owner, table_name, partition_name, high_value, partition_position, tablespace_name, num_rows, avg_row_len, last_analyzed FROM dba_tab_partitions WHERE table_owner NOT IN (%SYS_EXCLUDE%) ORDER BY table_owner, table_name, partition_position;"

call :run_query "24 CDC Readiness" "24_cdc_readiness.csv" "SELECT 'DATABASE_LEVEL' AS scope, supplemental_log_data_min AS min_supplemental, supplemental_log_data_pk AS pk_supplemental, supplemental_log_data_ui AS unique_supplemental, supplemental_log_data_fk AS fk_supplemental, supplemental_log_data_all AS all_supplemental, force_logging, log_mode, open_mode, database_role FROM v$database;"

call :run_query "25 Redo Log Sizing" "25_redo_log_sizing.csv" "SELECT group#, bytes / 1024 / 1024 AS size_mb, members, status, archived, first_time FROM v$log ORDER BY group#;"

call :run_query "26 Archive Rate" "26_archive_rate.csv" "SELECT TRUNC(completion_time, 'HH24') AS hora, COUNT(*) AS archives_generados, ROUND(SUM(blocks * block_size) / 1024 / 1024, 2) AS total_mb FROM v$archived_log WHERE completion_time > SYSDATE - 2 GROUP BY TRUNC(completion_time, 'HH24') ORDER BY hora;"

call :run_query "27 Data Guard Status" "27_dataguard_status.csv" "SELECT dest_id, status, type, database_mode, recovery_mode, protection_mode, standby_logfile_count, gap_status FROM v$archive_dest_status WHERE status != 'INACTIVE';"

REM Query 28 — Tablespaces (complex subqueries)
set "TMPSQL28=%TEMP%\mep_q28_%RANDOM%.sql"
(
    echo SET MARKUP CSV ON QUOTE ON
    echo SET PAGESIZE 0
    echo SET LINESIZE 32767
    echo SET LONG 1000000
    echo SET LONGCHUNKSIZE 200000
    echo SET TRIMSPOOL ON
    echo SET FEEDBACK OFF
    echo SET HEADING ON
    echo SET TERMOUT OFF
    echo SELECT
    echo     t.tablespace_name, t.status, t.contents,
    echo     t.logging, t.bigfile, t.encrypted,
    echo     ROUND(d.total_mb, 2^) AS total_mb,
    echo     ROUND(d.total_mb - f.free_mb, 2^) AS used_mb,
    echo     ROUND(f.free_mb, 2^) AS free_mb,
    echo     ROUND((d.total_mb - f.free_mb^) / d.total_mb * 100, 1^) AS pct_used
    echo FROM dba_tablespaces t
    echo JOIN (SELECT tablespace_name, SUM(bytes^)/1024/1024 AS total_mb
    echo       FROM dba_data_files GROUP BY tablespace_name^) d
    echo     ON t.tablespace_name = d.tablespace_name
    echo LEFT JOIN (SELECT tablespace_name, SUM(bytes^)/1024/1024 AS free_mb
    echo            FROM dba_free_space GROUP BY tablespace_name^) f
    echo     ON t.tablespace_name = f.tablespace_name
    echo ORDER BY t.tablespace_name;
    echo EXIT;
) > "!TMPSQL28!"
call :run_query_file "28 Tablespaces" "28_tablespaces.csv" "!TMPSQL28!"

call :run_query "29 Invalid Objects" "29_invalid_objects.csv" "SELECT owner, object_name, object_type, created, last_ddl_time FROM dba_objects WHERE status = 'INVALID' AND owner NOT IN (%SYS_EXCLUDE%) ORDER BY owner, object_type, object_name;"

REM Query 30 — DB Parameters (IN list with quotes)
set "TMPSQL30=%TEMP%\mep_q30_%RANDOM%.sql"
(
    echo SET MARKUP CSV ON QUOTE ON
    echo SET PAGESIZE 0
    echo SET LINESIZE 32767
    echo SET LONG 1000000
    echo SET LONGCHUNKSIZE 200000
    echo SET TRIMSPOOL ON
    echo SET FEEDBACK OFF
    echo SET HEADING ON
    echo SET TERMOUT OFF
    echo SELECT
    echo     name, value, isdefault, ismodified,
    echo     description
    echo FROM v$parameter
    echo WHERE isdefault = 'FALSE'
    echo    OR name IN (
    echo        'db_name','db_unique_name','compatible',
    echo        'log_archive_dest%%','archive_lag_target',
    echo        'enable_goldengate_replication',
    echo        'streams_pool_size','sga_target','pga_aggregate_target',
    echo        'undo_retention','db_recovery_file_dest_size'
    echo    ^)
    echo ORDER BY name;
    echo EXIT;
) > "!TMPSQL30!"
call :run_query_file "30 DB Parameters" "30_db_parameters.csv" "!TMPSQL30!"


REM ============================================================
REM SUMMARY
REM ============================================================
echo.
echo ============================================
echo RECOLECCION COMPLETADA -- %date% %time%
echo ============================================
echo.
echo   Queries exitosas: !TOTAL_OK!
if !TOTAL_ERR! gtr 0 (
    echo   Queries con error: !TOTAL_ERR! ^(ver detalles arriba^)
)
echo.

echo ============================================ >> "!LOG!"
echo RECOLECCION COMPLETADA -- %date% %time% >> "!LOG!"
echo ============================================ >> "!LOG!"
echo. >> "!LOG!"
echo   Queries exitosas: !TOTAL_OK! >> "!LOG!"
if !TOTAL_ERR! gtr 0 (
    echo   Queries con error: !TOTAL_ERR! >> "!LOG!"
)
echo. >> "!LOG!"

echo Archivos generados:
echo Archivos generados: >> "!LOG!"
for %%F in ("!OUTDIR!\*.csv") do (
    echo   %%~zF bytes  %%~nxF
    echo   %%~zF bytes  %%~nxF >> "!LOG!"
)
echo.
echo Log: !LOG!
echo.
echo SIGUIENTE PASO: Comprimir y entregar
echo   powershell Compress-Archive -Path "!OUTDIR!" -DestinationPath "mep_oracle.zip"
echo.

REM Cleanup temp files
del "%TEMP%\mep_q*_%RANDOM%.sql" 2>nul
del "%TEMP%\mep_tmp_%RANDOM%.sql" 2>nul

REM Clear password
set "ORA_PASS="
pause
exit /b 0


REM ============================================================
REM SUBROUTINE: run_query — for simple inline queries
REM Usage: call :run_query "label" "filename" "SQL"
REM ============================================================
:run_query
set "Q_LABEL=%~1"
set "Q_FILE=%~2"
set "Q_SQL=%~3"
set "TMPSQL_=%TEMP%\mep_tmp_%RANDOM%.sql"

(
    echo SET MARKUP CSV ON QUOTE ON
    echo SET PAGESIZE 0
    echo SET LINESIZE 32767
    echo SET LONG 1000000
    echo SET LONGCHUNKSIZE 200000
    echo SET TRIMSPOOL ON
    echo SET FEEDBACK OFF
    echo SET HEADING ON
    echo SET TERMOUT OFF
    echo %Q_SQL%
    echo EXIT;
) > "!TMPSQL_!"

set "T_START=%time%"
"!SQLPLUS_CMD!" -s "!ORA_USER!/!ORA_PASS!@!TNS!" @"!TMPSQL_!" > "!OUTDIR!\%Q_FILE%" 2>&1

REM Check for errors in output
findstr /i /r "^ORA- ^SP2-" "!OUTDIR!\%Q_FILE%" >nul 2>&1
if !errorlevel! equ 0 (
    echo   [%Q_LABEL%] ... ERROR
    echo   [%Q_LABEL%] ... ERROR >> "!LOG!"
    for /f "delims=" %%E in ('findstr /i /r "^ORA- ^SP2-" "!OUTDIR!\%Q_FILE%"') do (
        echo     %%E
        echo     %%E >> "!LOG!"
    )
    set /a TOTAL_ERR+=1
) else (
    for %%S in ("!OUTDIR!\%Q_FILE%") do set "FSIZE=%%~zS"
    echo   [%Q_LABEL%] ... OK ^(!FSIZE! bytes^)
    echo   [%Q_LABEL%] ... OK ^(!FSIZE! bytes^) >> "!LOG!"
    set /a TOTAL_OK+=1
)

del "!TMPSQL_!" 2>nul
goto :eof


REM ============================================================
REM SUBROUTINE: run_query_file — for pre-built .sql files
REM Usage: call :run_query_file "label" "filename" "sqlfile"
REM ============================================================
:run_query_file
set "Q_LABEL=%~1"
set "Q_FILE=%~2"
set "Q_SQLFILE=%~3"

"!SQLPLUS_CMD!" -s "!ORA_USER!/!ORA_PASS!@!TNS!" @"!Q_SQLFILE!" > "!OUTDIR!\%Q_FILE%" 2>&1

REM Check for errors in output
findstr /i /r "^ORA- ^SP2-" "!OUTDIR!\%Q_FILE%" >nul 2>&1
if !errorlevel! equ 0 (
    echo   [%Q_LABEL%] ... ERROR
    echo   [%Q_LABEL%] ... ERROR >> "!LOG!"
    for /f "delims=" %%E in ('findstr /i /r "^ORA- ^SP2-" "!OUTDIR!\%Q_FILE%"') do (
        echo     %%E
        echo     %%E >> "!LOG!"
    )
    set /a TOTAL_ERR+=1
) else (
    for %%S in ("!OUTDIR!\%Q_FILE%") do set "FSIZE=%%~zS"
    echo   [%Q_LABEL%] ... OK ^(!FSIZE! bytes^)
    echo   [%Q_LABEL%] ... OK ^(!FSIZE! bytes^) >> "!LOG!"
    set /a TOTAL_OK+=1
)

del "!Q_SQLFILE!" 2>nul
goto :eof
