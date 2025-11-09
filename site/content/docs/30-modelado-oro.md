---
title: "Definición de métricas y tablas oro"
owner: "equipo-alumno"
periodicidad: "diaria"
version: "1.0.0"
---
# 🏆 Modelo de negocio (capa ORO)

## 📘 Tablas ORO
- **events_oro** (base): granularidad **evento por usuario**  
  Contiene todos los eventos limpios con `session_id` asignado, y trazabilidad (`_ingest_ts`, `_batch_id`).  

- **sessions** (vista): granularidad **sesión por usuario**  
  Incluye métricas de navegación, duración, páginas vistas y embudo `/ → /productos → /carrito → /checkout`.  

- **users_stats** (vista): granularidad **usuario**  
  Resume actividad agregada: nº sesiones, nº compras, duración media, nº eventos.  

- **top_paths** (vista): granularidad **ruta (path)**  
  Muestra las 10 páginas más vistas y su conteo global.  

- **device_usage** (vista): granularidad **tipo de dispositivo**  
  Suma de eventos por `device`.  

- **sessions_per_day** (vista): granularidad **día**  
  Número de sesiones distintas por fecha (`date`).  

- **funnel_table** (vista): granularidad **paso del embudo**  
  Embudo agregado de conversión con tasas por etapa:
  `/ → /productos → /carrito → /checkout`.  

---

## 📊 Métricas (KPI)
| Métrica | Definición | Fuente |
|:---------|:------------|:-------|
| **Usuarios únicos** | `nunique(user_id)` | `events_oro` |
| **Sesiones totales** | `count(distinct session_id)` | `sessions` |
| **Compras (checkouts)** | `Σ(purchases_in_session)` | `sessions` |
| **Páginas por sesión (media)** | `mean(pageviews)` | `sessions` |
| **Duración media de sesión (min)** | `mean(session_duration_sec)/60` | `sessions` |
| **Top páginas** | `path` con mayor `views` | `top_paths` |
| **Uso de dispositivos** | % de eventos por `device` | `device_usage` |
| **Embudo de conversión** | tasas paso a paso `/ → /productos → /carrito → /checkout` | `funnel_table` |

---

## 💶 Supuestos de negocio
- Cada `/checkout` representa una **compra exitosa**.  
- No se consideran devoluciones ni cancelaciones.  
- Múltiples compras por sesión son posibles.  
- `session_timeout_min = 30` (gap > 30 min → nueva sesión).  
- Moneda y precios no aplican (solo volumen de acciones).  
- Se asume comportamiento realista de navegación: los usuarios pueden volver a `/` o abandonar sin comprar.  

