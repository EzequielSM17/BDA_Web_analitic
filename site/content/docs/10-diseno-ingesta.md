# 🪶 Diseño de ingestión

## 📘 Resumen
El proceso de **ingestión** constituye la capa **BRONCE** del pipeline.  
Su función es **recibir, validar y almacenar los eventos web crudos** provenientes del simulador (`00_insumo.ipynb`) o de un sistema de logs equivalente.  
El flujo opera en **modo batch**, con ejecución **horaria** y consolidación **diaria**, garantizando trazabilidad y separación entre líneas válidas y corruptas (JSON inválido).

---

## 🌍 Fuente
- **Origen:** archivos locales `data/drops/<YYYY-MM-DD>/events.ndjson` generados por el simulador de tráfico web.  
- **Formato:** **NDJSON** (Newline-Delimited JSON, un evento JSON por línea).  
- **Frecuencia:** **batch horario** (procesamiento diario consolidado).  
- **Volumen esperado:** entre 100 y 1.000 eventos por día (≈100–500 KB).

---

## ⚙️ Estrategia
- **Modo:** `batch` periódico (cada hora).  
- **Incremental:** por **fecha de evento (`day`)**; cada ejecución procesa solo el día especificado.  
- **Particionado:** por fecha `YYYY/MM/DD`.  
- **Procesamiento:** lectura → validación JSON → separación “buenos” vs “rotos” → escritura en `output/plata/`.  
- **Script responsable:** `bronze.py` (función `read_ndjson_bronze()`).

---

## 🔁 Idempotencia y deduplicación
- **batch_id:** generado dinámicamente como  
  ```text
  batch_id = os.getpid() + timestamp_utc_now
garantizando unicidad por corrida.

- **Clave natural:** combinación `(user_id, ts, path)` para identificar un evento único.
    
- **Política de resolución:** “**último gana**” (`keep="first"`, ordenado por `_ingest_ts`).
    
- **Propósito:** evitar duplicados en re-ejecuciones del mismo archivo.
- ---

## 🧩 Checkpoints y trazabilidad

- **checkpoints/offset:** _no aplica_ (modo batch, no streaming).
    
- **trazabilidad:**
    
    - `_ingest_ts` → timestamp UTC de ingesta.
        
    - `_source_file` → nombre del archivo origen.
        
    - `_batch_id` → identificador único del lote.
        
- **DLQ / cuarentena:**
    
    - Ruta: `quarantine_plata/<day>/`
        
    - Motivos: `invalid_json`, `bad_format`, `missing_field`, `empty_user_id`, `timestamp_out_of_day`.
        
    - Formato: `Parquet`, un archivo por tipo de error (`error_<campo>.parquet`).
        

---

## ⏰ SLA

- **Disponibilidad:** los datos del día **D** deben estar disponibles en la capa PLATA a las **03:00 UTC** del día **D+1**.
    
- **Alertas:**
    
    - Archivo faltante o tamaño anómalo.
        
    - % de errores en cuarentena > 10%.
        
    - Falla en la lectura o escritura del batch.