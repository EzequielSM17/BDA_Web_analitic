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

def detect_session_funnel(paths: list[str]) -> dict:
    """Devuelve flags para el embudo ordenado: / -> /productos -> /carrito -> /checkout."""

    def idx(p):
        try:
            return paths.index(p)
        except ValueError:
            return None

    i_root = idx("/")
    i_prod = idx("/productos")
    i_cart = idx("/carrito")
    i_chk = idx("/checkout")

    saw_root = i_root is not None
    saw_prod_after_root = saw_root and (
        i_prod is not None) and (i_prod > i_root)
    saw_cart_after_prod = saw_prod_after_root and (
        i_cart is not None) and (i_cart > i_prod)
    saw_chk_after_cart = saw_cart_after_prod and (
        i_chk is not None) and (i_chk > i_cart)

    return {
        "saw_root": saw_root,
        "saw_productos_after_root": saw_prod_after_root,
        "saw_carrito_after_productos": saw_cart_after_prod,
        "saw_checkout_after_carrito": saw_chk_after_cart,
        "purchase": saw_chk_after_cart,  # compra si llegó a checkout en orden
    }


def build_gold(silver: pd.DataFrame, session_timeout_min: int = 30):
    """
    Devuelve:
      - events_oro: eventos con session_id
      - sessions: tabla de sesiones con flags embudo y métricas
      - users_stats: visitas por usuario, sesiones y compras
      - top_paths: top 10 páginas
      - device_usage: uso de dispositivos (global)
      - sessions_per_day: nº sesiones por día
      - funnel_table: embudo agregado con tasas
    """
    df = silver.sort_values(["user_id", "ts"]).copy()

    # Sesionizar (gap > timeout => nueva sesión)
    df["prev_ts"] = df.groupby("user_id")["ts"].shift()
    df["gap_min"] = (df["ts"] - df["prev_ts"]).dt.total_seconds() / 60.0
    df["is_new_session"] = df["prev_ts"].isna() | (
        df["gap_min"] > float(session_timeout_min))
    df["session_idx"] = df.groupby("user_id")["is_new_session"].cumsum()
    # session_id estable por (user_id, date, idx)

    def make_session_id(row) -> str:
        base = f"{row.user_id}|{row.date}|{int(row.session_idx)}"
        return hashlib.sha1(base.encode()).hexdigest()[:16]

    df["session_id"] = df.apply(make_session_id, axis=1)

    # Flags de embudo por sesión
    paths_by_session = (
        df.sort_values("ts")
          .groupby("session_id")["path"]
          .apply(list)
          .to_dict()
    )

    flags_rows = []
    for sid, plist in paths_by_session.items():
        flags = detect_session_funnel(plist)
        flags["session_id"] = sid
        flags_rows.append(flags)
    session_flags = pd.DataFrame(flags_rows)
    # Tabla de sesiones con métricas
    sessions = (
        df.groupby("session_id")
          .agg(
              user_id=("user_id", "first"),
              date=("date", "first"),
              start_ts=("ts", "min"),
              end_ts=("ts", "max"),
              pageviews=("path", "count"),
              device_first=("device", "first"),
        )
        .reset_index()
        .merge(session_flags, on="session_id", how="left")
    )
    sessions["session_duration_sec"] = (
        sessions["end_ts"] - sessions["start_ts"]).dt.total_seconds().fillna(0)

    # Métricas por usuario
    users_sessions = sessions.groupby("user_id").agg(
        sessions=("session_id", "nunique"),
        purchases=("purchase", "sum"),
        avg_session_duration_sec=("session_duration_sec", "mean"),
    )
    users_events = df.groupby("user_id").size().rename("events")
    users_stats = (
        users_sessions.merge(users_events, on="user_id", how="left")
                      .reset_index()
                      .sort_values(["purchases", "sessions", "events"], ascending=[False, False, False])
    )

    # Top 10 paths
    top_paths = (
        df["path"].value_counts()
        .rename_axis("path")
        .reset_index(name="views")
        .head(10)
    )

    # Uso de dispositivos (global)
    device_usage = (
        df["device"].value_counts(dropna=True)
        .rename_axis("device")
        .reset_index(name="events")
    )

    # Sesiones por día
    sessions_per_day = (
        sessions.groupby("date")["session_id"].nunique()
                .rename("sessions")
                .reset_index()
    )

    # Embudo agregado
    total_sessions = len(sessions)
    s_root = int(sessions["saw_root"].sum())
    s_prod = int(sessions["saw_productos_after_root"].sum())
    s_cart = int(sessions["saw_carrito_after_productos"].sum())
    s_chk = int(sessions["saw_checkout_after_carrito"].sum())

    funnel_table = pd.DataFrame(
        {
            "step": [
                "Sesiones", "→ con '/'",
                "→ luego '/productos'",
                "→ luego '/carrito'",
                "→ luego '/checkout' (compra)"
            ],
            "count": [total_sessions, s_root, s_prod, s_cart, s_chk],
        }
    )
    # tasas
    def safe_div(a, b): return (a / b) if b else 0.0
    funnel_table["rate_step"] = [
        1.0,
        safe_div(s_root, total_sessions),
        safe_div(s_prod, s_root),
        safe_div(s_cart, s_prod),
        safe_div(s_chk,  s_cart),
    ]
    funnel_table["rate_overall"] = [
        1.0,
        safe_div(s_root, total_sessions),
        safe_div(s_prod, total_sessions),
        safe_div(s_cart, total_sessions),
        safe_div(s_chk,  total_sessions),
    ]

    # Limpieza columnas intermedias en eventos
    events_oro = df.drop(columns=["prev_ts"]).copy()

    return events_oro, sessions, users_stats, top_paths, device_usage, sessions_per_day, funnel_table


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
    print("[INFO] ORO sesionizando + métricas…")
    events_oro, sessions, users_stats, top_paths, device_usage, sessions_per_day, funnel = build_gold(
        silver, session_timeout_min=30
    )

    # Guardar ORO
    write_parquet(events_oro, os.path.join(args.gold, "events_oro.parquet"))
    write_parquet(sessions, os.path.join(args.gold, "sessions_oro.parquet"))
    write_parquet(users_stats, os.path.join(args.gold, "users_stats.parquet"))
    write_parquet(top_paths, os.path.join(args.gold, "top_paths.parquet"))
    write_parquet(device_usage, os.path.join(
        args.gold, "device_usage.parquet"))
    write_parquet(sessions_per_day, os.path.join(
        args.gold, "sessions_per_day.parquet"))
    write_parquet(funnel, os.path.join(args.gold, "funnel.parquet"))

    print(f"[OK] Guardados ORO en {args.gold}/")
    print("\n[RESUMEN ORO]")
    print(
        f"- Sesiones totales: {len(sessions)} | Compras: {int(sessions['purchase'].sum())}")
    print(
        f"- Usuarios con compra: {users_stats.loc[users_stats['purchases'] > 0, 'user_id'].nunique()}")
    print(
        f"- Top device: {device_usage.iloc[0]['device'] if not device_usage.empty else 'N/A'}")


if __name__ == "__main__":
    main()
