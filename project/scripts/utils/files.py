
import os
from typing import Iterable
import pandas as pd

from configs.get_data_config import DATA


def write_parquet(df: pd.DataFrame, path: str):
    out_dir = (DATA / path)
    os.makedirs(out_dir, exist_ok=True)
    df.to_parquet(path, index=False, engine="pyarrow")


def iter_lines(path: str) -> Iterable[str]:
    with open(path, "r", encoding="utf-8", errors="ignore") as fh:
        for line in fh:
            yield line
