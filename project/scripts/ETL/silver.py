import pandas as pd

from utils.files import write_parquet
from utils.normalizes import normalize_device, normalize_referrer, normalize_path, normalize_string


def to_silver(df: pd.DataFrame, day: str, quarantine_dir: str) -> pd.DataFrame:
    """Limpieza y normalización; los registros inválidos van a cuarentena."""
    out = df.copy()

    out["ts"] = pd.to_datetime(out["ts"], errors="coerce", utc=True)
    out["user_id"] = out["user_id"].apply(normalize_string).astype("string")
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

                write_parquet(
                    invalid, f"{quarantine_dir}/{day}", f"error_{key}.parquet")
                print(
                    f"[WARN] {len(invalid)} filas inválidas enviadas a → {quarantine_dir}/{day}/error_{key}.parquet")

    # Día objetivo
    day0 = pd.Timestamp(day, tz="UTC")
    mask_day = (out["ts"] >= day0) & (
        out["ts"] < day0 + pd.Timedelta(days=1))
    valid_day = out.loc[mask_day].copy()
    invalid_day = out.loc[~mask_day].copy()
    if not invalid_day.empty:
        invalid_day["_error"] = "outside_day"
        write_parquet(
            invalid_day, f"{quarantine_dir}/{day}", "error_out_ts.parquet")

        print(
            f"[WARN] {len(invalid_day)} filas inválidas enviadas a → {quarantine_dir}/{day}/error_out_ts.parquet")

    valid_day = (
        valid_day.sort_values(["user_id", "ts", "path"])
        .drop_duplicates(subset=["user_id", "ts", "path"], keep="last")
    )

    valid_day["date"] = valid_day["ts"].dt.date.astype("string")
    return valid_day
