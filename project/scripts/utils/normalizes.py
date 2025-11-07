import re
from typing import Any


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
