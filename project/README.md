# project/ — Ejecución técnica (parquet)

## Flujo (mínimo viable)
1. **Ingesta**: lee NDJSON de `data/drops/`, añade `_source_file` y `_ingest_ts`.
2. **Limpieza**: coerción de tipos, rangos/dominos básicos, cuarentena, dedupe “último gana”.
3. **Persistencia**: 
   - **Parquet** (`output/(silver or gold)/events_(silver or gold).parquet`)
4. **Reporte**: **releído desde Parquet** (fuente de verdad) → `output/reports/reporte.md`.

## Comandos
```bash
pip install -r requirements.txt
python ingest/get_data.py      # opcional (genera un NDJSON de ejemplo)
python ingest/run.py           # ejecuta todo: parquet + reporte.md
```
