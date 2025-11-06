#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
process_logs.py (versión extendida con CUARENTENA)
BRONCE → PLATA → ORO para logs web en NDJSON usando pandas + pyarrow.
"""
import argparse
import datetime
import glob
import json
import os
import re
import sys
import hashlib
from typing import Any, Iterable, List, Tuple
import pandas as pd


# ---------------------- Normalización ---------------------- #

def write_parquet(df: pd.DataFrame, path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_parquet(path, index=False, engine="pyarrow")


def normalize_string(x: Any) -> str | None:

    if not isinstance(x, str):
        return None
    if x == "":
        return None
    s = x.strip().lower()
    return s or None


def normalize_string_path(x: Any) -> str | None:
    s = normalize_string(x)
    if s is None:
        return None
    s = s.split("?", 1)[0]
    s = re.sub(r"/{2,}", "/", s)
    if s.startswith(("http://", "https://", "file://")):
        return None
    return s or None


def normalize_path(x: Any) -> str | None:
    s = normalize_string_path(x)
    if s and not s.startswith("/"):
        s = "/" + s
    return s or None


def normalize_referrer(x: Any) -> str | None:
    s = normalize_string_path(x)
    if s in {"", "(not set)"}:
        return None
    known = {"direct", "google", "facebook"}
    if s not in known and s and not s.startswith("/"):
        s = "/" + s
    return s or None


def normalize_device(x: Any) -> str | None:
    if not isinstance(x, str):
        return None
    s = x.strip().lower()
    return s if s in {"mobile", "desktop", "tablet"} else None


# -------------------- Lectura BRONCE -------------------- #

def iter_lines(path: str) -> Iterable[str]:
    with open(path, "r", encoding="utf-8", errors="ignore") as fh:
        for line in fh:
            yield line


def read_ndjson_bronze(path: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Lee NDJSON y separa las líneas rotas a 'bad_df'."""
    rows: List[dict] = []
    bad: List[dict] = []

    for line in iter_lines(path):
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            obj["_source_file"] = os.path.basename(path)
            rows.append(obj)
        except Exception:
            bad.append({
                "line": line,
                "_source_file": os.path.basename(path),
                "_error": "invalid_json"
            })

    df = pd.DataFrame(rows)
    bad_df = pd.DataFrame(bad)
    ts_now = pd.Timestamp.now(tz="UTC")
    df["_ingest_ts"] = ts_now
    bad_df["_ingest_ts"] = ts_now
    return df, bad_df


# -------------------- Limpieza PLATA -------------------- #

def to_silver(df: pd.DataFrame, day: str, quarantine_dir: str) -> pd.DataFrame:
    """Limpieza y normalización; los registros inválidos van a cuarentena."""
    out = df.copy()

    # Tipado
    out["ts"] = pd.to_datetime(out["ts"], errors="coerce", utc=True)
    out["user_id"] = out["user_id"].apply(normalize_string).astype("string")

    # Normalizaciones
    out["path"] = out["path"].apply(normalize_path).astype("string")
    out["referrer"] = out["referrer"].apply(
        normalize_referrer).astype("string")
    out["device"] = out["device"].apply(normalize_device).astype("string")
    for key in out.keys():
        if key in {"ts", "user_id", "path", "referrer", "device"}:
            mask_errors = out[key].isna()
            invalid = out.loc[mask_errors].copy()
            out.dropna(subset=[key], inplace=True)
            if not invalid.empty:
                invalid["_error"] = "ts"
                invalid_path = os.path.join(
                    f"{quarantine_dir}/{day}", f"error_{key}.parquet")
                write_parquet(invalid, invalid_path)
                print(
                    f"[WARN] {len(invalid)} filas inválidas enviadas a → {invalid_path}")

    # Día objetivo
    day0 = pd.Timestamp(day, tz="UTC")
    mask_day = (out["ts"] >= day0) & (
        out["ts"] < day0 + pd.Timedelta(days=1))
    valid_day = out.loc[mask_day].copy()
    invalid_day = out.loc[~mask_day].copy()
    if not invalid_day.empty:
        invalid_day["_error"] = "outside_day"
        invalid_path = os.path.join(f"{quarantine_dir}/{day}",
                                    "error_ts.parquet")
        write_parquet(invalid_day, invalid_path)

        print(
            f"[WARN] {len(invalid_day)} filas fuera del rango diario enviadas a cuarentena")

    valid_day = (
        valid_day.sort_values(["user_id", "ts", "path"])
        .drop_duplicates(subset=["user_id", "ts", "path"], keep="first")
    )

    valid_day["date"] = valid_day["ts"].dt.date.astype("string")
    return valid_day


# -------------------- Sesiones ORO -------------------- #

def sessionize_and_metrics(silver: pd.DataFrame, session_timeout_min: int = 30):
    df = silver.sort_values(["user_id", "ts"]).copy()
    df["prev_ts"] = df.groupby("user_id")["ts"].shift()
    df["gap_min"] = (df["ts"] - df["prev_ts"]).dt.total_seconds() / 60.0
    df["is_new_session"] = df["prev_ts"].isna() | (
        df["gap_min"] > session_timeout_min)
    df["session_idx"] = df.groupby("user_id")["is_new_session"].cumsum()

    def make_session_id(row) -> str:
        base = f"{row.user_id}|{row.date}|{int(row.session_idx)}"
        return hashlib.sha1(base.encode()).hexdigest()[:16]

    df["session_id"] = df.apply(make_session_id, axis=1)

    return df  # simplificado para ejemplo


# -------------------- MAIN -------------------- #

def main():
    actual_day = datetime.date.today().isoformat()
    ap = argparse.ArgumentParser()
    ap.add_argument("--day", default=actual_day,
                    help="Simula la copia sin escribir")
    ap.add_argument("--silver", default="output/plata",
                    help="Simula la copia sin escribir")
    ap.add_argument("--gold", default="output/oro",
                    help="Simula la copia sin escribir")
    ap.add_argument("--quarantine", default="output/cuarentena",
                    help="Simula la copia sin escribir")

    args = ap.parse_args()
    path = os.path.join("data", "drops", args.day)
    input_path = os.path.join(path, "events.ndjson")

    if not os.path.isfile(input_path):
        print(
            f"[ERROR] No se encontraron ficheros para patrón: {input_path}", file=sys.stderr)
        sys.exit(2)

    print(f"[INFO] BRONCE leyendo {input_path} fichero…")
    bronze_df, bad_df = read_ndjson_bronze(input_path)
    print(
        f"[INFO] BRONCE filas: {len(bronze_df):,}  | líneas rotas: {len(bad_df):,}")

    # 🧩 Guardar CUARENTENA BRONCE
    if len(bad_df) > 0:
        path_bad = os.path.join(
            f"{args.quarantine}/{args.day}", "No_JSON.parquet")
        write_parquet(bad_df, path_bad)
        print(f"[WARN] {len(bad_df)} líneas rotas enviadas a → {path_bad}")

    print("[INFO] PLATA limpiando…")
    silver = to_silver(
        bronze_df, day=args.day, quarantine_dir=args.quarantine)
    print(f"[INFO] PLATA filas válidas: {len(silver):,}")

    silver_out = os.path.join(args.silver, "events_plata.parquet")
    write_parquet(silver, silver_out)
    print(f"[OK] Guardado PLATA → {silver_out}")


if __name__ == "__main__":
    main()
