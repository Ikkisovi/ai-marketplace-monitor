#!/usr/bin/env python3
"""Reclassify unknown marketplace rows with current detection logic."""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

from ai_marketplace_monitor.market_data import MarketDataStore, get_market_data_store


def _count_unknown(db_path: Path) -> int:
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT COUNT(*)
            FROM listing_observations
            WHERE COALESCE(detected_model, 'unknown') = 'unknown'
               OR classification_reason IS NULL
            """
        ).fetchone()
        return int(row[0] if row else 0)


def _top_models(db_path: Path, limit: int = 20) -> list[tuple[str, int]]:
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT detected_model, COUNT(*) AS cnt
            FROM listing_observations
            GROUP BY detected_model
            ORDER BY cnt DESC, detected_model ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [(str(r[0]), int(r[1])) for r in rows]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Reclassify unknown detected_model rows using current market-data logic."
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=None,
        help="Path to market_data.db. Defaults to runtime configured path.",
    )
    args = parser.parse_args()

    if args.db_path is None:
        store = get_market_data_store()
    else:
        store = MarketDataStore(args.db_path.expanduser().resolve())

    before_unknown = _count_unknown(store.db_path)
    updated = store.reclassify_unknown_rows()
    after_unknown = _count_unknown(store.db_path)

    print(f"db_path={store.db_path}")
    print(f"updated_rows={updated}")
    print(f"unknown_before={before_unknown}")
    print(f"unknown_after={after_unknown}")
    print("top_detected_models:")
    for model, cnt in _top_models(store.db_path):
        print(f"  {model}: {cnt}")


if __name__ == "__main__":
    main()
