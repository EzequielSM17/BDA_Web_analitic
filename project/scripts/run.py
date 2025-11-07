#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
process_logs.py (versión extendida con CUARENTENA)
BRONCE → PLATA → ORO para logs web en NDJSON usando pandas + pyarrow.
"""
import argparse
import datetime
import os
import sys

from gold import build_gold
from bronze import read_ndjson_bronze
from report import build_report_md
from silver import to_silver
from utils.files import write_parquet


def main():
    actual_day = datetime.date.today().isoformat()
    ap = argparse.ArgumentParser(
        description="BRONCE→PLATA→ORO + Reporte Markdown (simple)")
    ap.add_argument("--day", default=actual_day,
                    help="Fecha (YYYY-MM-DD). Por defecto: hoy")
    ap.add_argument("--silver", default="output/plata",
                    help="Directorio salida PLATA")
    ap.add_argument("--gold", default="output/oro",
                    help="Directorio salida para reporte")
    ap.add_argument("--quarantine", default="output/cuarentena",
                    help="Directorio de cuarentena")
    args = ap.parse_args()

    # ---- BRONCE ----
    path = os.path.join("data", "drops", args.day)
    input_path = os.path.join(path, "events.ndjson")
    if not os.path.isfile(input_path):
        print(
            f"[ERROR] No se encontró el fichero: {input_path}", file=sys.stderr)
        sys.exit(2)

    bronze_df, bad_df = read_ndjson_bronze(input_path)
    if len(bad_df) > 0:
        bad_out = os.path.join(
            f"{args.quarantine}/{args.day}", "No_JSON.parquet")
        write_parquet(bad_df, bad_out)

    print("[OK] BRONCE leído y cuarentena escrita" +
          (f" ({len(bad_df)} líneas rotas)" if len(bad_df) else ""))

    # ---- PLATA ----
    silver = to_silver(bronze_df, day=args.day, quarantine_dir=args.quarantine)
    os.makedirs(args.silver, exist_ok=True)
    silver_out = os.path.join(args.silver, "events_plata.parquet")
    write_parquet(silver, silver_out)
    print("[OK] PLATA generada y guardada")

    # ---- ORO en memoria (no persiste parquets de oro) ----
    # Usa tu build_gold con contador purchases_in_session
    events_oro, sessions, users_stats, top_paths, device_usage, sessions_per_day, funnel = build_gold(
        silver, session_timeout_min=30
    )

    # ---- Reporte Markdown ----
    os.makedirs(args.gold, exist_ok=True)
    report_md = build_report_md(args.day, bronze_df, bad_df, silver,
                                sessions, users_stats, top_paths, device_usage,
                                sessions_per_day, funnel)
    report_path = os.path.join(args.gold, f"{args.day}-reporte.md")
    with open(report_path, "w", encoding="utf-8") as fh:
        fh.write(report_md)

    print("[OK] Reporte Markdown generado")

    # ---- Fin (sin más prints) ----
    # Si quieres un único OK final:
    print("[OK] Pipeline BRONCE→PLATA→ORO completado")


if __name__ == "__main__":
    main()
