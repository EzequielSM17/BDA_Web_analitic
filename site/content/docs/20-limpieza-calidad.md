# 🧽 Reglas de limpieza y calidad (PLATA)

## 📘 Tipos y formatos
| Campo | Tipo esperado | Formato / Validación |
|:------|:--------------|:---------------------|
| `ts` | `datetime64[ns, UTC]` | ISO 8601 (`YYYY-MM-DDTHH:MM:SSZ`) |
| `user_id` | `string` | No vacío, minúsculas (`normalize_string()`) |
| `path` | `string` | Normalizado, comienza con `/`, sin URLs completas |
| `referrer` | `string` | Origen válido (`direct`, `google`, `facebook` o path relativo) |
| `device` | `string` | Uno de `{"mobile", "desktop", "tablet"}` |

**Conversión:**  
`pd.to_datetime(errors="coerce", utc=True)` para `ts`  
Campos de texto: `astype("string")` tras normalización.

---

## 🚫 Nulos
- **Campos obligatorios:**  
  `ts`, `user_id`, `path`, `referrer`, `device`  
- **Tratamiento:**  
  - Si un campo obligatorio queda `NaN` tras la normalización → **fila inválida**.  
  - Registro enviado a **cuarentena** (`quarantine_plata/<day>/error_<campo>.parquet`).  
  - Se añade columna `_error` con el motivo.  

---

## 📊 Rangos y dominios
- `ts` debe caer dentro del día de proceso (`day <= ts < day + 1 día`), caso contrario → `error_out_ts.parquet`.  
- `device` debe pertenecer al dominio permitido.  
- `referrer` válido o `None`.  
- `path` no vacío, sin `http://`, `file://` ni `//double-slash`.  

---

## 🔁 Deduplicación
- **Clave natural:** `(user_id, ts, path)`  
- **Política:** “**último gana**” por `_ingest_ts`.  
- **Implementación:**
  ```python
  valid_day = (
      valid_day.sort_values(["user_id", "ts", "path"])
               .drop_duplicates(subset=["user_id", "ts", "path"], keep="last")
  )```

---
## Estandarización de texto

- `strip()` de espacios.
    
- `lower()` para todo texto.
    
- Normalización de tildes (si existieran).
    
- Eliminación de duplicaciones de `/` en `path`.
    
- Prefijo `/` obligatorio en rutas relativas.
    
- Eliminación de URLs externas (`http://`, `https://`, `file://`).
    

---

## 🧩 Trazabilidad

Todos los registros (válidos o no) conservan metadatos:

|Campo|Descripción|
|---|---|
|`_ingest_ts`|Timestamp UTC del proceso de ingesta|
|`_source_file`|Nombre del archivo origen (`events.ndjson`)|
|`_batch_id`|Identificador único del lote de ingesta|

**Objetivo:** rastrear cada evento desde su fuente original.

---

## 🧪 QA rápida

Verificaciones automáticas posteriores a la limpieza:

|Métrica|Descripción / Fórmula|
|---|---|
|`% de filas a cuarentena`|`(filas_quarantine / filas_bronce) * 100`|
|`conteo esperado`|filas PLATA ≈ filas BRONCE - errores - duplicados|
|`fechas válidas`|todos los `ts` dentro del día objetivo|
|`distribución por device`|debe coincidir con pesos esperados (55% mobile, 38% desktop, 7% tablet)|