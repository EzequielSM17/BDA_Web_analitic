import json
import os
import random
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any

from configs.get_data_config import DATA, LOOK_SITE, MAX_SIZE_BYTES, PIPELINE_MAKE_PURCHASE, RATE_MAKE_PURCHASE, VALID_DEVICES, VALID_REFERRERS, VALID_USERS


def parse_args(date_str: str = datetime.now().date().isoformat(), events_n: int = 150, seed: int = 17) -> Dict[str, Any]:
    return {"date": date_str, "n": events_n, "seed": seed}


def ensure_dir_for_date(date_str: str) -> str:
    out_dir = (DATA / date_str)
    os.makedirs(out_dir, exist_ok=True)
    return os.path.join(out_dir, "events.ndjson")


def iso(dt: datetime) -> str:
    # Siempre UTC en ISO-8601 con sufijo Z
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def make_purchase(current: datetime, session: Dict[str, Any], rng: random.Random):
    current += timedelta(seconds=rng.randint(5, 30))
    finish_session = False
    if session["path"] == '/carrito':
        finish_session = True

    return [current, {"ts": iso(current), "user_id": session["user_id"],
            "path": PIPELINE_MAKE_PURCHASE[session["path"]], "referrer": session["path"], "device": session["device"]}, finish_session]


def update_session(sessions, user_id, new_session_data):
    idx = next((i for i, s in enumerate(sessions)
               if s["user_id"] == user_id), None)
    if idx is not None:
        sessions[idx].update(new_session_data.copy())
        return True
    return False


def choose_action(user: str, current: datetime, device: str, rng: random.Random) -> Dict[str, Any]:
    current += timedelta(seconds=rng.randint(5, 30))
    rate = rng.randint(1, 100)
    if rate <= RATE_MAKE_PURCHASE:
        return [current, {"ts": iso(current), "user_id": user,
                          "path": PIPELINE_MAKE_PURCHASE["/"], "referrer": "/", "device": device}, False]
    elif rate > 95:
        return [current, None, True]
    else:
        path = rng.choice(LOOK_SITE)
        return [current, {"ts": iso(current), "user_id": user,
                          "path": path, "referrer": "/", "device": device}, True]


def generate_session(current: datetime, rng: random.Random):

    current += timedelta(seconds=rng.randint(5, 30))
    user = rng.choice(VALID_USERS)
    device = rng.choices(population=VALID_DEVICES,
                         weights=[55, 38, 7], k=1)[0]
    ref = rng.choices(population=VALID_REFERRERS, weights=[
        40, 35, 8], k=1)[0]

    return [current, {"ts": iso(current), "user_id": user,
                      "path": "/", "referrer": ref, "device": device}]


def generate_valid_events(date_str: str, n: int, rng: random.Random) -> List[Dict[str, Any]]:
    y, m, d = map(int, date_str.split("-"))
    start = datetime(y, m, d, 0, 0, 0, tzinfo=timezone.utc)

    current = start + timedelta(minutes=rng.randint(0, 180))
    events: List[Dict[str, Any]] = []

    sessions: List[Dict[str, Any]] = []

    for _ in range(n):
        current, event = generate_session(current, rng)
        exist_session = [
            s for s in sessions if event['user_id'] in s['user_id']]
        exist_session = exist_session[0] if exist_session else None
        if rng.random() < 0.10:
            events.append(inject_error(event.copy(), rng))

        if exist_session == None:
            events.append(event)
            sessions.append(event.copy())
        else:
            if exist_session['path'] == "/":
                current, event, finished_session = choose_action(
                    exist_session['user_id'], current, exist_session['device'], rng)
                if not event == None:
                    events.append(event)
                if finished_session:
                    sessions.remove(exist_session)
                else:
                    update_session(sessions, event["user_id"], event)
            else:
                current, event, finished_session = make_purchase(
                    current, exist_session, rng)

                if rng.random() < 0.60:
                    events.append(event)
                    if finished_session:

                        sessions.remove(exist_session)
                    else:
                        update_session(sessions, event["user_id"], event)
                else:
                    sessions.remove(exist_session)

    return events


def inject_error(event: Dict[str, Any], rng: random.Random) -> Dict[str, Any]:
    """
    Devuelve UN solo evento erróneo al azar.
    Se puede usar dentro de generate_valid_events() para insertar errores aleatorios.
    """

    error_type = rng.choice([
        "missing_field",
        "bad_timestamp_format",
        "bad_values",
        "not_json",
        "timestamp_out_of_day",
        "empty_user_id",
    ])

    if error_type == "missing_field":
        field = rng.choice(["referrer", "device", "path"])
        event.pop(field, None)

    elif error_type == "bad_timestamp_format":
        event["ts"] = "03-01-2025 10:15:00"

    elif error_type == "bad_values":
        event["device"] = rng.choice(["toaster", "phon3", "desk-top", ""])
        event["referrer"] = rng.choice(
            [None, "(not set)", "   ", "file://local", "http://malformed"])
        event["path"] = rng.choice(
            ["productos", "checkout", "//double-slash", ""])

    elif error_type == "not_json":
        return "NOT_JSON_LINE this is a broken log line"

    elif error_type == "timestamp_out_of_day":
        event["ts"] = "2024-01-04T00:00:00Z"

    elif error_type == "empty_user_id":
        event["user_id"] = ""

    return event


def write_ndjson_limited(path: str, lines: List[str], max_bytes: int):
    written = 0
    with open(path, "w", encoding="utf-8") as f:
        for line in lines:
            b = (line + "\n").encode("utf-8")
            if written + len(b) > max_bytes:
                break
            f.write(line)
            f.write("\n")
            written += len(b)
    return written


def main():
    args = parse_args(events_n=500, seed=42)
    rng = random.Random(args["seed"])

    out_path = ensure_dir_for_date(args["date"])
    valid = generate_valid_events(args["date"], args["n"], rng)
    lines = [json.dumps(event, ensure_ascii=False) for event in valid]

    written_bytes = write_ndjson_limited(out_path, lines, MAX_SIZE_BYTES)
    print(f"✔ Archivo generado: {out_path} ({written_bytes/1024:.2f} KB)")


if __name__ == "__main__":
    main()
