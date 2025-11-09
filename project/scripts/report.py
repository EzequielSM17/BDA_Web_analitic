from argparse import Namespace
import datetime
import pandas as pd

from configs.run_config import FILE_GOLD_NAME, FILE_SILVER_NAME, SILVER_DIR


def build_report_md(args: Namespace,
                    bronze_df: pd.DataFrame,
                    bad_df: pd.DataFrame,
                    silver: pd.DataFrame,
                    sessions: pd.DataFrame,
                    users_stats: pd.DataFrame,
                    top_paths: pd.DataFrame,
                    device_usage: pd.DataFrame,
                    sessions_per_day: pd.DataFrame,
                    funnel: pd.DataFrame) -> str:
    """Devuelve el texto Markdown del reporte final."""

    gen_ts = datetime.datetime.now(datetime.timezone.utc).isoformat()

    total_sessions = len(sessions)
    total_purchases = int(sessions.get("purchases_in_session", pd.Series(
        dtype=int)).sum()) if "purchases_in_session" in sessions.columns else 0
    uniq_users = int(silver["user_id"].nunique()) if not silver.empty else 0
    avg_pages_per_session = float(sessions["pageviews"].mean(
    )) if "pageviews" in sessions.columns and not sessions.empty else 0.0
    avg_session_min = float(sessions["session_duration_sec"].mean(
    ) / 60.0) if "session_duration_sec" in sessions.columns and not sessions.empty else 0.0

    top_paths_md = top_paths.to_markdown(index=False) if (isinstance(
        top_paths, pd.DataFrame) and not top_paths.empty) else "_(sin datos)_"
    device_usage_md = device_usage.to_markdown(index=False) if (isinstance(
        device_usage, pd.DataFrame) and not device_usage.empty) else "_(sin datos)_"
    sessions_per_day_md = sessions_per_day.to_markdown(index=False) if (isinstance(
        sessions_per_day, pd.DataFrame) and not sessions_per_day.empty) else "_(sin datos)_"
    funnel_md = funnel.to_markdown(index=False) if (isinstance(
        funnel, pd.DataFrame) and not funnel.empty) else "_(sin datos)_"

    bronze_rows = len(bronze_df)
    silver_rows = len(silver)
    bad_json_rows = len(bad_df)

    report = (
        "# Reporte · Web Logs (BRONCE → PLATA → ORO)\n"
        f"**Día:** {args.day} · **Fuente:** events.ndjson · **Generado:** {gen_ts}\n\n"
        "## 1. Titular\n"
        f"Usuarios únicos {uniq_users}; sesiones {total_sessions}; compras {total_purchases}.\n\n"
        "## 2. KPIs\n"
        f"- **Usuarios únicos:** {uniq_users}\n"
        f"- **Sesiones:** {total_sessions}\n"
        f"- **Compras (checkouts):** {total_purchases}\n"
        f"- **Eventos (plata):** {silver_rows}\n"
        f"- **Páginas por sesión (media):** {avg_pages_per_session:.2f}\n"
        f"- **Duración media sesión (min):** {avg_session_min:.2f}\n\n"
        "## 3. Top 10 páginas\n"
        f"{top_paths_md}\n\n"
        "## 4. Uso de dispositivos (por eventos)\n"
        f"{device_usage_md}\n\n"
        "## 5. Sesiones por día\n"
        f"{sessions_per_day_md}\n\n"
        "## 6. Embudo por sesión\n"
        f"{funnel_md}\n\n"
        "## 7. Calidad y cobertura\n"
        f"- Filas BRONCE: {bronze_rows}\n"
        f"- Filas PLATA: {silver_rows}\n"
        f"- Líneas rotas (JSON) a cuarentena: {bad_json_rows}\n"
        f"- Diferencia BRONCE→PLATA (drops/dedupe/fuera de día): {bronze_rows - silver_rows}\n\n"
        f"- Porcentaje de cobertura PLATA/BRONCE:  {(silver_rows/(bronze_rows+bad_json_rows)*100.0) if bronze_rows > 0 else 0.0:.2f}%\n\n"
        "## 8. Persistencia\n"
        f"- Parquet PLATA: `{args.gold}/{FILE_SILVER_NAME}`\n"
        f"- Parquet ORO: `{args.silver}/{FILE_GOLD_NAME}`\n"
        f"- Reporte: `{args.report}/{args.day}-reporte.md`\n"
    )
    return report
