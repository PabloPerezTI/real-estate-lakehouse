"""
Rebuild the simulator state up to a given day by replaying every day from
the initial CSV prices. Event generation is deterministic per day, so the
rebuilt state matches the one saved by the original runs. Nothing is
written to the landing zone and no event files are created.

Usage:
    python simulator/rebuild_state.py --until-day 2026-01-05
"""

import argparse
from datetime import date, timedelta
from pathlib import Path

from simulate import apply_events, day_rng, generate_price_drops, init_state, save_state


def main() -> None:
    """Replay the days from --start-date to --until-day and save the resulting state."""
    parser = argparse.ArgumentParser(description="Rebuild the simulator state by replaying days.")
    parser.add_argument("--source", default="data/raw_houses_madrid.csv", help="Path to the source file")
    parser.add_argument("--start-date", default="2026-01-01", help="First simulated day of the original runs")
    parser.add_argument("--until-day", required=True, help="Last day to include in the rebuilt state")
    parser.add_argument("--state", default="simulator/state.json", help="Path to the state file")
    args = parser.parse_args()

    start = date.fromisoformat(args.start_date)
    until = date.fromisoformat(args.until_day)
    state = init_state(Path(args.source), start)

    day = start
    while day <= until:
        listings = sorted((int(k), v) for k, v in state["prices"].items())
        events = generate_price_drops(listings, day, day_rng(day))
        apply_events(state, events, day)
        day += timedelta(days=1)

    save_state(state, Path(args.state))
    print(f"State rebuilt up to {state['last_simulated_day']}")


if __name__ == "__main__":
    main()