# run.py (fragmentos relevantes)
import argparse
import pandas as pd

from ETL.gold import build_events_gold, aggregate_from_events_gold
from ETL.bronze import read_ndjson_bronze
from ETL.silver import to_silver
from report import build_report_md
from configs.run_config import (
    BRONZE_DIR, DAY, FILE_BRONZE_NAME, FILE_GOLD_NAME, FILE_SILVER_NAME,
    GOLD_DIR, QUARANTINE_DIR, REPORT_DIR, SILVER_DIR
)
from utils.files import write_file, write_parquet


def main():
    ap = argparse.ArgumentParser(
        description="BRONCE→PLATA→ORO + Reporte Markdown (simple)")
    ap.add_argument("--day", default=DAY)
    ap.add_argument("--bronze", default=BRONZE_DIR)
    ap.add_argument("--bronze-file-name", default=FILE_BRONZE_NAME)
    ap.add_argument("--silver", default=SILVER_DIR)
    ap.add_argument("--gold", default=GOLD_DIR)
    ap.add_argument("--report", default=REPORT_DIR)
    ap.add_argument("--quarantine", default=QUARANTINE_DIR)
    args = ap.parse_args()

    # ---- BRONCE ----
    path = f"{args.bronze}{args.day}/events.ndjson"
    bronze_df, bad_df = read_ndjson_bronze(path)
    if len(bad_df) > 0:
        write_parquet(
            bad_df, f"{args.quarantine}/{args.day}", "No_JSON_lines.parquet")
    print(f"[OK] BRONCE leído y cuarentena escrita. Fichero: {path}" +
          (f" ({len(bad_df)} líneas rotas)" if len(bad_df) else ""))

    # ---- PLATA ----
    silver = to_silver(bronze_df, day=args.day, quarantine_dir=args.quarantine)
    write_parquet(silver,  f"{args.silver}/{args.day}", FILE_SILVER_NAME)
    print("[OK] PLATA generada y guardada")

    # ---- ORO: materializar events_gold.parquet ----
    events_gold_df = build_events_gold(silver, session_timeout_min=30)
    write_parquet(events_gold_df, f"{args.gold}/{args.day}", FILE_GOLD_NAME)
    print(
        f"[OK] ORO (events) materializado → {args.gold}/{args.day}/{FILE_GOLD_NAME}")

    # ---- ORO: cargar desde events_gold.parquet y calcular KPI ----
    events_gold_path = f"{args.gold}/{args.day}/{FILE_GOLD_NAME}"
    events_gold_loaded = pd.read_parquet(events_gold_path)

    (sessions,
     users_stats,
     top_paths,
     device_usage,
     sessions_per_day,
     funnel) = aggregate_from_events_gold(events_gold_loaded)

    # ---- Reporte Markdown ----
    report_md = build_report_md(args, bronze_df, bad_df, events_gold_df,
                                sessions, users_stats, top_paths, device_usage,
                                sessions_per_day, funnel)

    write_file(args.report, f"{args.day}-reporte.md", report_md)
    print("[OK] Reporte Markdown generado")
    print("[OK] Pipeline BRONCE→PLATA→ORO completado")


if __name__ == "__main__":
    main()
