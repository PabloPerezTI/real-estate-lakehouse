"""
Event simulator, step 3.

Simulates consecutive days of price drops. The simulator keeps a local state
(last simulated day and current price of every listing), so each run continues
where the previous one stopped and every drop starts from the latest price.

Usage:
    python simulator/simulate.py --days 3            # dry run: local files only, state not saved
    python simulator/simulate.py --days 3 --upload   # upload each day to S3 and save the state
"""

import argparse
import json
import os
import random
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import boto3
import pandas as pd

SEED = 42
DROP_PROBABILITY = 0.02
MIN_DROP = 0.02
MAX_DROP = 0.08

EVENT_TYPE = "cambio_precio"
LANDING_PREFIX = "landing/eventos"


def is_true(value) -> bool:
    """Parse a CSV flag as a boolean, whether it comes as bool or as text."""
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() == "true"


def read_source(path: Path) -> pd.DataFrame:
    """Read the source file as CSV or Excel, depending on its extension."""
    if path.suffix.lower() in (".xlsx", ".xls"):
        return pd.read_excel(path)
    return pd.read_csv(path)


def load_listings(source_path: Path) -> list[tuple[int, int]]:
    """
    Read the source file and return the for-sale listings with a known
    price as (anuncio_id, price) tuples, using native Python ints so they
    can be serialised to JSON.
    """
    df = read_source(source_path)

    known_price = df["is_buy_price_known"].apply(is_true)
    for_sale = df["operation"] == "sale"
    listings = df.loc[known_price & for_sale, ["id", "buy_price"]]

    return [(int(row.id), int(row.buy_price)) for row in listings.itertuples(index=False)]


def init_state(source_path: Path, start_date: date) -> dict:
    """
    Build the initial state from the source file: day 0 is the day before
    the first simulated day, and every listing starts at its CSV price.
    """
    return {
        "last_simulated_day": (start_date - timedelta(days=1)).isoformat(),
        "prices": {str(anuncio_id): price for anuncio_id, price in load_listings(source_path)},
    }


def load_state(state_path: Path, source_path: Path, start_date: date) -> dict:
    """Load the saved state, or build the initial one if there is none yet."""
    if state_path.exists():
        return json.loads(state_path.read_text(encoding="utf-8"))
    return init_state(source_path, start_date)


def save_state(state: dict, state_path: Path) -> None:
    """
    Save the state atomically: write to a temporary file and then replace
    the old one, so a crash never leaves a half-written state file.
    """
    tmp_path = state_path.with_suffix(".tmp")
    tmp_path.write_text(json.dumps(state), encoding="utf-8")
    os.replace(tmp_path, state_path)


def day_rng(day: date) -> random.Random:
    """
    Return a generator seeded with the global seed and the simulated day,
    so regenerating a day always produces exactly the same events.
    """
    return random.Random(f"{SEED}-{day.isoformat()}")


def generate_price_drops(listings: list[tuple[int, int]], day: date, rng: random.Random) -> list[dict]:
    """
    Pure logic, no I/O: pick a random share of the listings, lower their
    price within the configured range and return one cambio_precio event
    per drop, sorted by event time. The event_id comes from the seeded
    generator so the output is reproducible.
    """
    n_drops = round(len(listings) * DROP_PROBABILITY)
    chosen = rng.sample(listings, n_drops)
    midnight = datetime.combine(day, datetime.min.time())

    events = []
    for anuncio_id, old_price in chosen:
        drop = rng.uniform(MIN_DROP, MAX_DROP)
        new_price = min(round(old_price * (1 - drop)), old_price - 1)
        event_time = midnight + timedelta(seconds=rng.randint(0, 86_399))

        events.append({
            "event_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
            "event_type": EVENT_TYPE,
            "event_ts": event_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "anuncio_id": anuncio_id,
            "precio_anterior": old_price,
            "precio_nuevo": new_price,
        })

    events.sort(key=lambda e: e["event_ts"])
    return events


def apply_events(state: dict, events: list[dict], day: date) -> None:
    """Update the current prices and the last simulated day in the state."""
    for event in events:
        state["prices"][str(event["anuncio_id"])] = event["precio_nuevo"]
    state["last_simulated_day"] = day.isoformat()


def build_file_name(day: date, run_id: str) -> str:
    """
    Build a file name that is unique per run, so files in the landing zone
    are never overwritten (the landing zone is append-only).
    """
    return f"{EVENT_TYPE}_{day.isoformat()}_run-{run_id}.jsonl"


def write_jsonl(events: list[dict], path: Path) -> None:
    """Write one event per line, not indented (JSON Lines)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for event in events:
            f.write(json.dumps(event, ensure_ascii=False, separators=(",", ":")) + "\n")


def upload_to_s3(local_path: Path, bucket: str, profile: str) -> str:
    """
    Upload the file to landing/eventos/<event_type>/ using the given AWS CLI
    profile. Credentials are read from the local AWS configuration, never
    from the code. Returns the S3 URI of the uploaded object.
    """
    key = f"{LANDING_PREFIX}/{EVENT_TYPE}/{local_path.name}"
    s3 = boto3.Session(profile_name=profile).client("s3")
    s3.upload_file(str(local_path), bucket, key)
    return f"s3://{bucket}/{key}"


def main() -> None:
    """
    Simulate the next N days. For each day: generate the events, write them,
    upload them and only then save the state. Without --upload the run is a
    dry run and the saved state is left untouched.
    """
    parser = argparse.ArgumentParser(description="Simulate consecutive days of price drops.")
    parser.add_argument("--source", default="data/raw_houses_madrid.csv", help="Path to the source file (CSV or Excel)")
    parser.add_argument("--start-date", default="2026-01-01", help="First simulated day, used only when there is no state yet")
    parser.add_argument("--days", type=int, default=1, help="Number of days to simulate")
    parser.add_argument("--state", default="simulator/state.json", help="Path to the state file")
    parser.add_argument("--out-dir", default="simulator/output", help="Local output folder")
    parser.add_argument("--upload", action="store_true", help="Upload to S3 and save the state")
    parser.add_argument("--bucket", default="real-estate-lakehouse-s3", help="Landing zone bucket")
    parser.add_argument("--profile", default="real-estate", help="AWS CLI profile used for the upload")
    args = parser.parse_args()

    state_path = Path(args.state)
    start_date = date.fromisoformat(args.start_date)
    state = load_state(state_path, Path(args.source), start_date)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    for _ in range(args.days):
        day = date.fromisoformat(state["last_simulated_day"]) + timedelta(days=1)
        listings = sorted((int(k), v) for k, v in state["prices"].items())
        events = generate_price_drops(listings, day, day_rng(day))

        out_path = Path(args.out_dir) / build_file_name(day, run_id)
        write_jsonl(events, out_path)
        message = f"{day}: {len(events)} events -> {out_path}"

        if args.upload:
            uri = upload_to_s3(out_path, args.bucket, args.profile)
            message += f" -> {uri}"

        apply_events(state, events, day)
        if args.upload:
            save_state(state, state_path)
        print(message)

    if not args.upload:
        print("Dry run: state not saved.")


if __name__ == "__main__":
    main()