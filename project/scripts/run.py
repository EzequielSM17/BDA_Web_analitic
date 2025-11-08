#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
process_logs.py (versión extendida con CUARENTENA)
BRONCE → PLATA → ORO para logs web en NDJSON usando pandas + pyarrow.
"""
import argparse

from gold import build_gold
from bronze import read_ndjson_bronze
from report import build_report_md
from configs.run_config import BRONZE_DIR, DAY, FILE_BRONZE_NAME, FILE_GOLD_NAME, FILE_SILVER_NAME, GOLD_DIR, QUARANTINE_DIR, REPORT_DIR, SILVER_DIR
from silver import to_silver
from utils.files import write_file, write_parquet


def main():
    ap = argparse.ArgumentParser(
        description="BRONCE→PLATA→ORO + Reporte Markdown (simple)")
    ap.add_argument("--day", default=DAY,
                    help="Fecha (YYYY-MM-DD). Por defecto: hoy")
    ap.add_argument("--bronze", default=BRONZE_DIR,
                    help="Directorio lectura de BRONCE. RECUERDA {bronze}/{day}/{bronze_file_name}")
    ap.add_argument("--bronze-file-name", default=FILE_BRONZE_NAME,
                    help="Nombre del fichero de lectura lectura de BRONCE")
    ap.add_argument("--silver", default=SILVER_DIR,
                    help="Directorio salida PLATA")
    ap.add_argument("--gold", default=GOLD_DIR,
                    help="Directorio salida para reporte")
    ap.add_argument("--report", default=REPORT_DIR,
                    help="Directorio salida para reporte")
    ap.add_argument("--quarantine", default=QUARANTINE_DIR,
                    help="Directorio de cuarentena")
    args = ap.parse_args()

    # ---- BRONCE ----
    path = f"{args.bronze}{args.day}/events.ndjson"
    bronze_df, bad_df = read_ndjson_bronze(
        f"{args.bronze}{args.day}/events.ndjson")
    if len(bad_df) > 0:
        write_parquet(
            bad_df, f"{args.quarantine}/{args.day}", "No_JSON_lines.parquet")

    print(f"[OK] BRONCE leído y cuarentena escrita. Fichero: {path}" +
          (f" ({len(bad_df)} líneas rotas)" if len(bad_df) else ""))

    # ---- PLATA ----
    silver = to_silver(bronze_df, day=args.day, quarantine_dir=args.quarantine)
    write_parquet(silver, args.silver, FILE_SILVER_NAME)
    print("[OK] PLATA generada y guardada")

    # ---- ORO en memoria (no persiste parquets de oro) ----
    events_oro, sessions, users_stats, top_paths, device_usage, sessions_per_day, funnel = build_gold(
        silver, session_timeout_min=30
    )
    write_parquet(events_oro, args.gold, FILE_GOLD_NAME)
    # ---- Reporte Markdown ----

    report_md = build_report_md(args.day, bronze_df, bad_df, silver,
                                sessions, users_stats, top_paths, device_usage,
                                sessions_per_day, funnel)

    write_file(args.report, f"{args.day}-reporte.md", report_md)

    print("[OK] Reporte Markdown generado")

    # ---- Fin (sin más prints) ----
    # Si quieres un único OK final:
    print("[OK] Pipeline BRONCE→PLATA→ORO completado")


if __name__ == "__main__":
    main()
