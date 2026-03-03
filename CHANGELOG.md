# Changelog

## v1.0 (2026-03-03)
- Release inicial con launcher Python cross-platform + script bash
- Ejecutable portable para Windows (.exe) y Linux (binario) via PyInstaller
- Batch file (.bat) como launcher alternativo en Windows
- Script bash standalone (`gather_oracle.sh`) como alternativa para Linux
- 31 queries de metadata: schemas, tablas, columnas, PKs, FKs, indexes, constraints, triggers, views, materialized views, PL/SQL code, synonyms, sequences, DB links, dependencies, jobs, grants, roles, comments, segments, partitions, CDC readiness, redo logs, archive rate, Data Guard, tablespaces, invalid objects, DB parameters
- Modo interactivo (sin argumentos) con menu guiado
- Modo CLI (con argumentos) para automatizacion
- Validacion de sqlplus al inicio (busca en PATH y paths comunes de Oracle)
- Test de conexion antes de ejecutar queries
- Manejo de errores por query (no aborta en primer fallo)
- Deteccion de errores ORA-/SP2- en output
- Timeout de 10 minutos por query
- Limpieza de password en memoria al finalizar
- Compatible con Oracle 12c+ (optimizado para 19c)
