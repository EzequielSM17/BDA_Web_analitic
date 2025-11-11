# Proyecto_UT1_RA1_BA · Solución de ingestión, almacenamiento y reporte (UT1 · RA1)

Este repositorio contiene:
- **project/**: código reproducible (ingesta → clean → oro → reporte Markdown).
- **site/**: web pública con **Quartz 4** (GitHub Pages). El reporte UT1 se publica en `site/content/reportes/`.

## Ejecución rápida
```bash
# 1) Dependencias (elige uno)
python -m venv .venv
.venv\Scripts\activate  # (o source .venv/bin/activate)
pip install -r project/requirements.txt
# o: conda env create -f project/environment.yml && conda activate ut1

# 2) (Opcional) Generar datos de ejemplo
python project/scripts/get_data.py

# 3) Pipeline fin-a-fin (ingesta→clean→oro→reporte.md)
python project/scripts/run.py

# 4) Copiar el reporte a la web Quartz
python project/tools/copy_report_to_site.py

# 5) (Opcional) Previsualizar la web en local
cd site
nvm install 22
npm i
npx quartz build --serve   # abre http://localhost:8080
```

## Publicación web (Cloudflre)
- En **Workers y pages**, selecciona **Source = Crear aplicacion**.
- Elegir tu report de git
- La configuración directorio raíz `site/`, comando de compilación `npx quartz build` y directorio de raiz `public`.

## Flujo de datos
Bronce (`raw`) → Plata (`clean`) → Oro (`analytics`).  
Idempotencia por `batch_id` (batch).  
Deduplicación “último gana” por `ts`.  
Reporte Markdown: `project/output/reports/(fecha)-reporte.md` → `site/content/reportes/(fecha)-reporte.md`.



