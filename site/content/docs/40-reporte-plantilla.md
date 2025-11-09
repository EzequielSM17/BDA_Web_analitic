# 📈 Plantilla de reporte (resumen ejecutivo)

> **Titular:** Qué pasa + por qué importa + qué hacemos.  
> Ejemplo: **Sesiones +15%** vs día anterior impulsadas por tráfico **mobile**.  
> Reforzar rendimiento en `/productos` y optimizar tiempos de carga.

---

## 1️⃣ Métricas clave
| KPI | Valor | Variación |
|:----|:------|:----------|
| **Usuarios únicos** | — | — |
| **Sesiones** | — | — |
| **Compras (checkouts)** | — | — |
| **Páginas por sesión (media)** | — | — |
| **Duración media sesión (min)** | — | — |

> **Interpretación:** ¿Aumenta la interacción o solo el tráfico? ¿Mejora la conversión?

---

## 2️⃣ Contribución por página / embudo
| Etapa | Conteo | Ratio paso | Ratio total |
|:------|--------:|------------:|-------------:|
| Sesiones | — | 1.00 | 1.00 |
| → con `/` | — | — | — |
| → luego `/productos` | — | — | — |
| → luego `/carrito` | — | — | — |
| → luego `/checkout` | — | — | — |

> **Insights:** identificar dónde se pierde más tráfico en el embudo y posibles causas (UX, precios, velocidad, etc.).

---

## 3️⃣ Distribución y tendencias
| Dispositivo | Eventos | % |
|:-------------|---------:|--:|
| mobile | — | — |
| desktop | — | — |
| tablet | — | — |

| Fecha | Sesiones |
|:------|----------:|
| 2025-11-08 | — |
| 2025-11-09 | — |

> **Comentario:** analizar picos por campañas, tráfico social u horario.

---

## 4️⃣ Calidad de datos
- **Filas procesadas:**  
  - BRONCE → `__`  
  - PLATA → `__`  
  - **Cuarentena:** `__`
- **Principales motivos de cuarentena:**  
  - Campos vacíos (`user_id`, `path`)  
  - `timestamp` fuera del rango diario  
  - Formato JSON inválido  
- **Cobertura:** `% PLATA/BRONCE = __%`

> **Objetivo:** mantener la cuarentena por debajo del 10%.

---

## 5️⃣ Próximos pasos
- **Acción 1:** Revisar ratio de conversión entre `/productos` y `/carrito`.  
- **Acción 2:** Analizar comportamiento de usuarios mobile (alto tráfico, menor checkout).  
- **Acción 3:** Automatizar alerta diaria de cuarentena > 10%.  

---

> 🧠 **Nota:** Este reporte se genera automáticamente por `report.py` a partir de las tablas **ORO**, consolidando KPIs y embudo de conversión del día.

---
