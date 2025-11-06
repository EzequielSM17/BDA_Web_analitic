---
title: "Definición de métricas y tablas oro"
owner: "equipo-alumno"
periodicidad: "diaria"
version: "1.0.0"
---

# Modelo de negocio (capa oro)

## Tablas oro
- **clean_ventas** (fuente): granularidad **línea de venta**
- **ventas_diarias** (vista): granularidad **día**

## Métricas (KPI)
- **Ingresos netos**: Σ(`unidades * precio_unitario`) sobre `clean_ventas`
- **Ticket medio**: `Ingresos netos / nº_transacciones`
- **Top producto**: `id_producto` con mayor `importe` en periodo

## Supuestos
- Dinero en EUR constantes, sin impuestos
- Dedupe “último gana”

## Consultas base (SQL conceptual)
```sql
-- Ingresos por día
SELECT fecha, SUM(unidades*precio_unitario) AS importe_total, COUNT(*) AS lineas
FROM clean_ventas
GROUP BY fecha;

-- Top productos
SELECT id_producto, SUM(unidades*precio_unitario) AS importe
FROM clean_ventas
GROUP BY id_producto
ORDER BY importe DESC;
```
