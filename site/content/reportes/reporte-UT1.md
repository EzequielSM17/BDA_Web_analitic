# Reporte UT1 · Ventas
**Periodo:** 2025-01-03 a 2025-01-05 · **Fuente:** clean_ventas (Parquet) · **Generado:** 2025-10-23T10:33:01.765159+00:00

## 1. Titular
Ingresos totales 69.50 €; producto líder: P10.

## 2. KPIs
- **Ingresos netos:** 69.50 €
- **Ticket medio:** 17.38 €
- **Transacciones:** 4

## 3. Top productos
| id_producto   |   importe | pct   |
|:--------------|----------:|:------|
| P10           |      37.5 | 54%   |
| P20           |      32   | 46%   |

## 4. Resumen por día
| fecha      |   importe_total |   transacciones |
|:-----------|----------------:|----------------:|
| 2025-01-03 |            25   |               1 |
| 2025-01-04 |            36.5 |               2 |
| 2025-01-05 |             8   |               1 |

## 5. Calidad y cobertura
- Filas bronce: 6 · Plata: 4 · Cuarentena: 2

## 6. Persistencia
- Parquet: D:\Proyectos\work\ESP-IA_BA-25-26\Proyecto_UT1_RA1_BA\project\output\parquet\clean_ventas.parquet
- SQLite : D:\Proyectos\work\ESP-IA_BA-25-26\Proyecto_UT1_RA1_BA\project\output\ut1.db (tablas: raw_ventas, clean_ventas; vista: ventas_diarias)

## 7. Conclusiones
- Reponer producto líder según demanda.
- Revisar filas en cuarentena (rangos/tipos).
- Valorar particionado por fecha para crecer.
