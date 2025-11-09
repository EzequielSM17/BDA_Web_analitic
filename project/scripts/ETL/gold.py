
import hashlib
import pandas as pd


def idx(lst, val):
    try:
        return lst.index(val)
    except ValueError:
        return None


def make_session_id(row) -> str:
    base = f"{row.user_id}|{row.date}|{int(row.session_idx)}"
    return hashlib.sha1(base.encode()).hexdigest()[:16]


def detect_session_funnel_with_counts(paths: list[str]) -> dict:
    """
    Devuelve flags de 'paso visto' (al menos una vez, en orden) y
    'purchases_in_session' (número de secuencias completas).
    Secuencia base: / -> /productos -> /carrito -> /checkout

    Nota: Para permitir múltiples compras en la misma sesión aunque no se vuelva
    a '/', reiniciamos el ciclo tras un /checkout y permitimos empezar el ciclo
    en /productos si ya hubo '/' en la sesión (más realista en ecommerce).
    """
    saw_root = ("/" in paths)

    i_root = idx(paths, "/")
    i_prod = idx(paths, "/productos")
    i_cart = idx(paths, "/carrito")
    i_chk = idx(paths, "/checkout")

    saw_prod_after_root = (
        i_root is not None and i_prod is not None and i_prod > i_root)
    saw_cart_after_prod = (
        saw_prod_after_root and i_cart is not None and i_cart > i_prod)
    saw_chk_after_cart = (
        saw_cart_after_prod and i_chk is not None and i_chk > i_cart)

    purchases = 0
    have_seen_root = False
    state = "start"

    for p in paths:
        if p == "/":
            have_seen_root = True
            state = "after_root"
            continue

        if state == "after_root":
            if p == "/productos":
                state = "prod"
            continue

        if state == "start" and have_seen_root:
            if p == "/productos":
                state = "prod"
            continue

        if state == "prod":
            if p == "/carrito":
                state = "cart"
            elif p == "/":
                state = "after_root"
            else:
                pass
            continue

        if state == "cart":
            if p == "/checkout":
                purchases += 1
                state = "start"
            elif p == "/":
                state = "after_root"
            else:
                pass
            continue

    return {
        "saw_root": saw_root,
        "saw_productos_after_root": saw_prod_after_root,
        "saw_carrito_after_productos": saw_cart_after_prod,
        "saw_checkout_after_carrito": saw_chk_after_cart,
        "purchases_in_session": purchases,
    }


def build_gold(silver: pd.DataFrame, session_timeout_min: int = 1):
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
        flags = detect_session_funnel_with_counts(plist)
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
        purchases=("purchases_in_session", "sum"),
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
