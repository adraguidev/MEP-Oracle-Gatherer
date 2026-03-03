# MEP Oracle Gatherer

Herramienta automatizada para recolectar metadata de instancias Oracle, empaquetada como ejecutable portable que no requiere instalacion.

Desarrollado por **Integratel Peru - Stefanini Group**.

## Caracteristicas

- Recoleccion completa de metadata Oracle (schemas, tablas, columnas, PKs, FKs, indexes, triggers, views, PL/SQL, jobs, grants, etc.)
- Analisis de CDC readiness (supplemental logging, redo logs, archive rate)
- Deteccion de Data Guard
- Domain hints automaticos para tablas de telecom/billing
- Compatible con **Oracle 12c+** (optimizado para 19c)
- Modo interactivo (menu guiado) y modo CLI (para automatizacion)
- Output 100% CSV, optimizado para analisis por LLM
- **Cross-platform**: Windows (.exe/.bat) y Linux (binario/bash)
- Ejecutable portable - no requiere Python en la maquina destino

## Requisitos

- **sqlplus** instalado y en PATH (Oracle Client o Instant Client)
- Usuario Oracle con permisos:
  - `SELECT ANY DICTIONARY`
  - `SELECT_CATALOG_ROLE`
  - (o rol `DBA` para acceso completo)
- Conexion TNS configurada al instance target

## Uso

### Opcion 1: Script standalone (recomendado)

#### Windows (.bat) — recomendado
Copiar unicamente `MEP_Oracle_Gatherer.bat` al servidor y ejecutar:

```
MEP_Oracle_Gatherer.bat
```

El `.bat` es **100% standalone** — no requiere Python ni el `.exe`. Solo necesita `sqlplus` en PATH. Ejecuta las 31 queries directamente contra Oracle, identico al `.sh` de Linux.

Tambien acepta parametros CLI:
```
MEP_Oracle_Gatherer.bat SIMPLE_PROD mep_reader
MEP_Oracle_Gatherer.bat SIMPLE_PROD mep_reader C:\temp\output
```

> **Nota:** El `.exe` compilado con PyInstaller suele ser detectado como falso positivo por antivirus. El `.bat` evita este problema completamente ya que es un script de texto plano.

#### Linux
Copiar `MEP_Oracle_Gatherer` (binario) o `gather_oracle.sh` al servidor:

```bash
chmod +x MEP_Oracle_Gatherer
./MEP_Oracle_Gatherer
```

O con el script bash directamente:
```bash
chmod +x gather_oracle.sh
./gather_oracle.sh
```

El programa guia paso a paso:

```
==============================================================
  MEP Oracle Gatherer v1.0 -- Stefanini Group
  Recolector automatizado de metadata Oracle
==============================================================

  [OK] SQL*Plus Release 19.0.0.0.0

  CONEXION ORACLE
  ----------------------------------------
  TNS Alias (ej: PROD, orcl, //host:1521/service): _
  Usuario (ej: mep_reader): _
  Password para mep_reader@PROD: _

  Probando conexion a mep_reader@PROD... OK

  Output: /tmp/mep_oracle_20260303_141500

  Presione ENTER para iniciar la recoleccion (Ctrl+C para cancelar)...
```

### Opcion 2: Modo CLI (automatizacion)

```bash
# Ejecutable:
./MEP_Oracle_Gatherer SIMPLE_PROD mep_reader
./MEP_Oracle_Gatherer SIMPLE_PROD mep_reader /tmp/output

# Script bash (solo Linux):
./gather_oracle.sh SIMPLE_PROD mep_reader
./gather_oracle.sh //10.0.1.5:1521/SIMPLEPROD mep_reader /tmp/output

# Python directo:
python mep_oracle_launcher.py SIMPLE_PROD mep_reader
```

El password se solicita interactivamente (no se pasa por parametro).

## Compilar el ejecutable

### Requisitos para compilar

- Python 3.6+
- PyInstaller (`pip install pyinstaller`)

### Windows (.exe)

```powershell
pip install pyinstaller
pyinstaller mep_oracle_launcher.spec
# Output: dist/MEP_Oracle_Gatherer.exe
```

### Linux (binario)

```bash
pip install pyinstaller
pyinstaller mep_oracle_launcher.spec
# Output: dist/MEP_Oracle_Gatherer
```

El ejecutable es self-contained: incluye Python embebido, solo necesita `sqlplus` en el servidor destino.

## Que genera

```
mep_oracle_YYYYMMDD_HHMMSS/
├── 00_instance_info.csv        <- Version, host, status, log mode
├── 01_schemas.csv              <- Usuarios no-sistema con tamano
├── 02_objects_summary.csv      <- Conteo por tipo de objeto y schema
├── 03_tables_columns.csv       <- Diccionario de datos completo + domain hints
├── 04_primary_keys.csv
├── 05_tables_no_pk.csv         <- Tablas sin PK = riesgo CDC
├── 06_foreign_keys.csv
├── 07_indexes.csv
├── 08_constraints_check.csv
├── 09_triggers_code.csv        <- Codigo COMPLETO de triggers
├── 10_views_code.csv           <- Codigo COMPLETO de vistas
├── 11_mviews.csv               <- Materialized views con query
├── 12_sp_code.csv              <- PL/SQL completo (SPs, funciones, packages)
├── 13_synonyms.csv
├── 14_sequences.csv
├── 15_db_links.csv
├── 16_dependencies.csv         <- Dependencias entre objetos
├── 17_jobs_scheduler.csv       <- Oracle Scheduler jobs
├── 18_jobs_dbms.csv            <- Legacy DBMS_JOB
├── 19_grants.csv               <- Privilegios de objeto
├── 20_role_members.csv
├── 21_comments.csv             <- Comentarios/definiciones semanticas
├── 22_segments_sizes.csv       <- Tamanos de segmentos (>1MB)
├── 23_partitions.csv
├── 24_cdc_readiness.csv        <- Supplemental logging, force logging
├── 25_redo_log_sizing.csv
├── 26_archive_rate.csv         <- Tasa de generacion de archive logs (48h)
├── 27_dataguard_status.csv     <- Data Guard (si aplica)
├── 28_tablespaces.csv          <- Espacio usado/libre por tablespace
├── 29_invalid_objects.csv
├── 30_db_parameters.csv        <- Parametros no-default
└── gather_oracle.log           <- Log con timing y errores
```

Total: 31 archivos CSV + 1 log

## Como funciona internamente

### Flujo de ejecucion

```
1. INICIO
   - Detecta sqlplus en PATH (o busca en paths comunes de Oracle)
   - Solicita TNS, usuario, password (interactivo) o los toma de argumentos (CLI)
   - Prueba la conexion antes de empezar (SELECT 'CONNECTION_OK' FROM dual)

2. RECOLECCION (31 queries)
   - Cada query se ejecuta via sqlplus como subproceso
   - Si una query falla (ej: Data Guard no configurado), se loguea el error
     y continua con la siguiente -- NO aborta
   - Errores ORA-/SP2- se detectan en el output y se reportan
   - Timeout de 10 minutos por query

3. RESUMEN
   - Muestra conteo de queries exitosas vs con error
   - Lista archivos generados con tamano
   - Instrucciones para comprimir y entregar
```

### Manejo de errores

Los errores **no detienen** la ejecucion. Si una query falla:
- Se marca como `[ERROR]` en el log
- Se muestra el codigo ORA-/SP2- especifico
- El archivo CSV se genera (puede contener el error o estar vacio)
- Se continua con la siguiente query

Esto es critico porque no todos los servidores tienen Data Guard, archive logs, o ciertos objetos. El script recolecta todo lo que puede.

### Schemas excluidos

Se excluyen automaticamente los schemas de sistema Oracle:
`SYS, SYSTEM, DBSNMP, OUTLN, XDB, WMSYS, CTXSYS, MDSYS, ORDDATA, ORDSYS, OLAPSYS, EXFSYS, DVSYS, LBACSYS, APEX_*, ANONYMOUS, APPQOSSYS, GSMADMIN_INTERNAL, AUDSYS, DBSFWUSER, DVF` y otros.

## Archivos del proyecto

| Archivo | Descripcion |
|---------|-------------|
| `mep_oracle_launcher.py` | Launcher Python cross-platform (Windows + Linux) |
| `mep_oracle_launcher.spec` | PyInstaller spec para compilar ejecutable |
| `MEP_Oracle_Gatherer.bat` | Script standalone Windows — ejecuta queries directo con sqlplus |
| `gather_oracle.sh` | Script bash standalone (alternativa para Linux) |

## Resultado esperado

Comprimir las carpetas de output y entregar:

```bash
# Linux:
tar czf mep_oracle_evidencia.tar.gz mep_oracle_*/

# Windows:
powershell Compress-Archive -Path ".\mep_oracle_*" -DestinationPath "mep_oracle_evidencia.zip"
```
