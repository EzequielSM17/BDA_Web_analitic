# Reglas de limpieza y calidad

## Tipos y formatos
- `fecha`: ISO (`YYYY-MM-DD` o `TIMESTAMP` UTC)
- `unidades`: entero ≥ 0
- `precio_unitario`: decimal ≥ 0 (no float para dinero en DB)

## Nulos
- Campos obligatorios: (`fecha`, `id_cliente`, `id_producto`, `unidades`, `precio_unitario`)
- Tratamiento: filas inválidas → **quarantine** con causa

## Rangos y dominios
- `unidades >= 0`
- `precio_unitario >= 0`
- `id_producto` coincide con `^P[0-9]+$`

## Deduplicación
- Clave natural: `(fecha, id_cliente, id_producto)`
- Política: **último gana** por `_ingest_ts`

## Estandarización de texto
- `trim`, normalización de tildes, mayúsculas en códigos

## Trazabilidad
- Mantener `_ingest_ts`, `_source_file`, `_batch_id`

## QA rápida
- % de filas a quarantine
- Conteos por día vs. esperado
