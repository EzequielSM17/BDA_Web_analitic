
import os
from pathlib import Path
import sys
from typing import Iterable
import pandas as pd

DATA = Path(__file__).resolve().parents[2]
DATA.mkdir(parents=True, exist_ok=True)


def make_path_dirs(path_dirs: str):
    return (DATA / path_dirs)


def ensure_dir(path_dirs: str, file_name: str) -> str:
    out_dir = make_path_dirs(path_dirs)
    os.makedirs(out_dir, exist_ok=True)
    return os.path.join(out_dir, file_name)


def write_parquet(df: pd.DataFrame, path_dir: str, file_name: str):
    out_path = ensure_dir(path_dir, file_name)
    df.to_parquet(out_path, index=False, engine="pyarrow")


def write_file(path_dirs: str, file_name: str, content: str):
    out_dir = ensure_dir(path_dirs, file_name)
    with open(out_dir, "w", encoding="utf-8") as fh:
        fh.write(content)


def iter_lines(path: str) -> Iterable[str]:
    if not os.path.isfile(path):
        print(
            f"[ERROR] No se encontró el fichero: {path}", file=sys.stderr)
        sys.exit(2)
    with open(path, "r", encoding="utf-8", errors="ignore") as fh:
        for line in fh:
            yield line
