import json
import os
from typing import List, Tuple

import pandas as pd

from utils.files import iter_lines


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
