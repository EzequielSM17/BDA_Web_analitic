# Guía de instalación y publicación · Proyecto_UT1_RA1_BA

Esta guía explica cómo **clonar**, **ejecutar el pipeline** (ingesta → limpieza → Parquet/SQLite → reporte) y **publicar la web** con **Quartz 4** en **GitHub Pages**. Incluye solución a errores comunes del workflow.

---

## 0) Requisitos

- **Git** y cuenta de **GitHub**.
- **Python 3.11+**.
- **Node 22+** y **npm ≥ 10.9.2** (Quartz 4 lo exige).
  - Windows (nvm-windows):
    ```powershell
    nvm install 22
    nvm use 22
    npm i -g npm@^10.9.2
    ```
  - macOS/Linux (nvm):
    ```bash
    nvm install 22
    nvm use 22
    npm i -g npm@^10.9.2
    ```
- (Opcional) **Conda** si prefieres `environment.yml`.

---

## 1) Descargar el repositorio

```bash
git clone https://github.com/<TU_USUARIO>/Proyecto_UT1_RA1_BA.git
cd Proyecto_UT1_RA1_BA
```

> Si descargas el ZIP desde GitHub, simplemente descomprímelo y entra a la carpeta.

---

## 2) Preparar entorno Python y ejecutar el pipeline

### 2.1 Opción venv (recomendada si no usas conda)

```bash
# crear y activar entorno
python -m venv .venv
# Windows PowerShell
.\.venv\Scripts\Activate.ps1
# macOS/Linux
# source .venv/bin/activate

# dependencias
pip install -r project/requirements.txt

# generar datos de ejemplo y ejecutar pipeline
python project/scripts/get_data.py
python project/scripts/run.py
```

Se generan:
- `project/output/quarantine/`
- `project/output/silver/<day>`
- `project/output/gold/<day>`
- `project/output/reports/(dia)-reporte.md` (**reporte leído desde Parquet**)

### 2.2 Opción conda (alternativa)

```bash
conda env create -f project/environment.yml
conda activate ut1
python project/scripts/get_data.py
python project/scripts/run.py
```

---

## 3) Preparar la web (Quartz 4) en `site/`

Si ya tienes `site/` creado con Quartz, salta al paso 3.2.

### 3.1 Crear `site/` como proyecto Quartz

```bash
# desde la raíz del repo
rm -rf site             # (Windows: rmdir /s /q .\site)
npx create @jackyzha0/quartz site
cd site
npm i
```

Copia el contenido Markdown (si no estaba ya):
- `content/index.md`
- `content/metodologia.md`
- `content/reportes/reporte-UT1.md` (puedes copiar desde `project/output/reporte.md`).

### 3.2 Configurar la URL base

Edita `site/quartz.config.ts` y ajusta:

```ts
import { defineConfig } from "@jackyzha0/quartz"

export default defineConfig({
  site: {
    name: "Proyecto UT1 · RA1 · BA",
    baseUrl: "https://<TU_USUARIO>.github.io/Proyecto_UT1_RA1_BA", // <- ¡muy importante!
    description: "Ingesta · Almacenamiento · Reporte",
  },
})
```

### 3.3 Probar la web en local

```bash
npx quartz build --serve
# abre http://localhost:8080
```

---

## 4) Publicar en Cloudflare (Workers y Pages)

### 4.1 Primer push

```bash
git add .
git commit -m "Proyecto UT1: pipeline + site Quartz"
git branch -M main
git remote add origin https://github.com/<TU_USUARIO>/Proyecto_UT1_RA1_BA.git
git push -u origin main
```

### 4.2 Ir a CloudFlare

En CloudFlare → **Workers y Pages** → **Crear Aplicacion**.

Configuracion:

- Comando de compilación: npx quartz build
- Crear directorio de salida: /public
- Directorio raíz: /site


## 5) Verificar el despliegue

- Workers y Pages → (tu proyecto)  (debe estar en verde).
- URL final: `https://<TU_PROYECTO>.pages.dev`.
- Comprueba portada e informe **Reporte UT1** (`/reportes/reporte-UT1`).

---



