# Diseño de ingestión

## Resumen
Describe **cómo entran los datos**, la **frecuencia**, y las **garantías** mínimas.

## Fuente
- **Origen:** (p. ej., `data/drops/*.csv` / API / logs)
- **Formato:** CSV / NDJSON / Avro
- **Frecuencia:** diario / semanal / streaming (cada N s)

## Estrategia
- **Modo:** `batch` / `micro-batch` (10–60 s) / `continua`
- **Incremental:** por `fecha_operacion` / CDC / full-refresh controlado
- **Particionado:** por fecha (`YYYY/MM/DD`) / otro

## Idempotencia y deduplicación
- **batch_id:** `hash(nombre_archivo + tamaño + mtime)`
- **clave natural:** `(fecha, id_cliente, id_producto)` o `event_id`
- **Política:** “último gana por `_ingest_ts`”

## Checkpoints y trazabilidad
- **checkpoints/offset:** (si streaming)
- **trazabilidad:** `_ingest_ts`, `_source_file`, `_batch_id`
- **DLQ/quarantine:** ruta y motivos

## SLA
- **Disponibilidad:** (p. ej., 03:00 UTC a diario)
- **Alertas:** (si aplica)

## Riesgos / Antipatrones
- Batch con necesidad de segundos → **no encaja**
- Falta de clave natural → defínela antes de cargar
