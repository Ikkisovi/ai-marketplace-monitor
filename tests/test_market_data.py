import sqlite3
from pathlib import Path

import pytest

from ai_marketplace_monitor.listing import Listing
from ai_marketplace_monitor.market_data import MarketDataStore


def _listing(
    listing_id: str,
    price: str,
    title: str = "Sony A7C II",
    description: str = "Sony A7C II body only.",
) -> Listing:
    return Listing(
        marketplace="facebook",
        name="sony_a7c2",
        id=listing_id,
        title=title,
        image="",
        price=price,
        post_url=f"https://www.facebook.com/marketplace/item/{listing_id}",
        location="Vancouver, BC",
        seller="seller",
        condition="used_like_new",
        description=description,
    )


def _fetch_one(db_path: Path, sql: str):
    with sqlite3.connect(db_path) as conn:
        return conn.execute(sql).fetchone()


def test_estimate_sold_time_from_last_active(tmp_path: Path) -> None:
    db_path = tmp_path / "market_data.db"
    store = MarketDataStore(db_path)

    listing = _listing("listing-1", "$1900")
    store.record_observation(
        listing=listing,
        item_name="sony_a7c2",
        search_city="vancouver",
        search_phrase="sony a7c2",
        availability="in",
    )
    store.record_observation(
        listing=listing,
        item_name="sony_a7c2",
        search_city="vancouver",
        search_phrase="sony a7c2",
        availability="out",
    )

    sold_row = _fetch_one(
        db_path,
        """
        SELECT sold_estimated_at, sold_time_method
        FROM listing_observations
        WHERE listing_id = 'listing-1' AND availability = 'out'
        ORDER BY id DESC
        LIMIT 1
        """,
    )
    assert sold_row is not None
    assert sold_row[0] is not None
    assert sold_row[1] == "midpoint_last_active_and_first_sold"


def test_refresh_market_price_from_recent_sold_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "market_data.db"
    store = MarketDataStore(db_path)

    store.record_observation(
        listing=_listing("sold-1", "$1800"),
        item_name="sony_a7c2",
        search_city="vancouver",
        search_phrase="sony a7c2",
        availability="out",
    )
    store.record_observation(
        listing=_listing("sold-2", "$2200"),
        item_name="sony_a7c2",
        search_city="vancouver",
        search_phrase="sony a7c2",
        availability="out",
    )

    sample_size, msrp_estimate, currency = store.refresh_market_price(
        item_name="sony_a7c2",
        marketplace="facebook",
        search_city="vancouver",
        window_days=30,
    )

    assert sample_size == 2
    assert msrp_estimate == pytest.approx(2000.0)
    assert currency == "$"

    summary = _fetch_one(
        db_path,
        """
        SELECT sample_size, msrp_estimate
        FROM market_price
        WHERE item_name = 'sony_a7c2' AND marketplace = 'facebook' AND search_city = 'vancouver'
        """,
    )
    assert summary == (2, 2000.0)


def test_record_observation_writes_english_classification_labels(tmp_path: Path) -> None:
    db_path = tmp_path / "market_data.db"
    store = MarketDataStore(db_path)

    store.record_observation(
        listing=_listing(
            listing_id="acc-1",
            price="$60",
            title="SmallRig A7C II Handgrip L-Shape Quick Release Plate",
            description="Quick release plate for Sony Alpha 7C II.",
        ),
        item_name="sony_a7c2",
        search_city="vancouver",
        search_phrase="sony a7c2",
        availability="all",
    )

    row = _fetch_one(
        db_path,
        """
        SELECT detected_model, listing_type, is_target_exact, classification_reason
        FROM listing_observations
        WHERE listing_id = 'acc-1'
        """,
    )
    assert row is not None
    detected_model, listing_type, is_target_exact, reason = row
    assert detected_model == "sony_a7c2"
    assert listing_type == "accessory"
    assert is_target_exact == 0
    assert "not a target camera body" in reason


def test_refresh_market_price_uses_only_exact_target_sold_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "market_data.db"
    store = MarketDataStore(db_path)

    store.record_observation(
        listing=_listing(
            listing_id="target-1",
            price="$1800",
            title="Sony A7C II body",
            description="Sony A7C II in great condition.",
        ),
        item_name="sony_a7c2",
        search_city="vancouver",
        search_phrase="sony a7c2",
        availability="out",
    )
    store.record_observation(
        listing=_listing(
            listing_id="other-1",
            price="$1000",
            title="Sony RX1R compact camera",
            description="Sony RX1R camera body.",
        ),
        item_name="sony_a7c2",
        search_city="vancouver",
        search_phrase="sony a7c2",
        availability="out",
    )

    sample_size, msrp_estimate, currency = store.refresh_market_price(
        item_name="sony_a7c2",
        marketplace="facebook",
        search_city="vancouver",
        window_days=30,
    )
    assert sample_size == 1
    assert msrp_estimate == pytest.approx(1800.0)
    assert currency == "$"


def test_refresh_market_price_deduplicates_by_listing_latest_out_row(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_path = tmp_path / "market_data.db"
    store = MarketDataStore(db_path)

    timestamps = iter(
        [
            "2026-02-25T10:00:00+00:00",
            "2026-02-25T10:05:00+00:00",
            "2026-02-25T10:10:00+00:00",
        ]
    )
    monkeypatch.setattr(
        "ai_marketplace_monitor.market_data._utc_now_iso",
        lambda: next(timestamps),
    )

    store.record_observation(
        listing=_listing("dup-1", "$1800"),
        item_name="sony_a7c2",
        search_city="vancouver",
        search_phrase="sony a7c2",
        availability="out",
    )
    store.record_observation(
        listing=_listing("dup-1", "$1700"),
        item_name="sony_a7c2",
        search_city="vancouver",
        search_phrase="sony a7c2",
        availability="out",
    )
    store.record_observation(
        listing=_listing("dup-2", "$1900"),
        item_name="sony_a7c2",
        search_city="vancouver",
        search_phrase="sony a7c2",
        availability="out",
    )

    sample_size, msrp_estimate, currency = store.refresh_market_price(
        item_name="sony_a7c2",
        marketplace="facebook",
        search_city="vancouver",
        window_days=30,
    )
    assert sample_size == 2
    assert msrp_estimate == pytest.approx(1800.0)
    assert currency == "$"


def test_refresh_market_price_excludes_listings_that_reappear_in_stock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_path = tmp_path / "market_data.db"
    store = MarketDataStore(db_path)

    timestamps = iter(
        [
            "2026-02-25T10:00:00+00:00",
            "2026-02-25T10:02:00+00:00",
            "2026-02-25T10:04:00+00:00",
        ]
    )
    monkeypatch.setattr(
        "ai_marketplace_monitor.market_data._utc_now_iso",
        lambda: next(timestamps),
    )

    store.record_observation(
        listing=_listing("flip-1", "$1800"),
        item_name="sony_a7c2",
        search_city="vancouver",
        search_phrase="sony a7c2",
        availability="out",
    )
    store.record_observation(
        listing=_listing("flip-1", "$1800"),
        item_name="sony_a7c2",
        search_city="vancouver",
        search_phrase="sony a7c2",
        availability="all",
    )
    store.record_observation(
        listing=_listing("flip-2", "$2100"),
        item_name="sony_a7c2",
        search_city="vancouver",
        search_phrase="sony a7c2",
        availability="out",
    )

    sample_size, msrp_estimate, currency = store.refresh_market_price(
        item_name="sony_a7c2",
        marketplace="facebook",
        search_city="vancouver",
        window_days=30,
    )
    assert sample_size == 1
    assert msrp_estimate == pytest.approx(2100.0)
    assert currency == "$"


def test_refresh_market_price_supports_detected_model_from_other_search_item(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "market_data.db"
    store = MarketDataStore(db_path)

    store.record_observation(
        listing=_listing(
            listing_id="a7c-from-a7c2-search",
            price="$1600",
            title="Sony A7C full-frame camera",
            description="Sony A7C camera body in excellent condition.",
        ),
        item_name="sony_a7c2",
        search_city="vancouver",
        search_phrase="sony a7c2",
        availability="out",
    )

    sample_size, msrp_estimate, currency = store.refresh_market_price(
        item_name="sony_a7c",
        marketplace="facebook",
        search_city="vancouver",
        window_days=30,
    )
    assert sample_size == 1
    assert msrp_estimate == pytest.approx(1600.0)
    assert currency == "$"


def test_detects_model_from_description_variants(tmp_path: Path) -> None:
    db_path = tmp_path / "market_data.db"
    store = MarketDataStore(db_path)

    store.record_observation(
        listing=_listing(
            listing_id="desc-a7sii",
            price="$900",
            title="Sony body",
            description="Selling my Sony A7Sii, works perfectly.",
        ),
        item_name="sony_a7c2",
        search_city="vancouver",
        search_phrase="sony a7c2",
        availability="all",
    )
    store.record_observation(
        listing=_listing(
            listing_id="title-a7riii",
            price="$1100",
            title="Sony a7Riii body only",
            description="Body only.",
        ),
        item_name="sony_a7c2",
        search_city="vancouver",
        search_phrase="sony a7c2",
        availability="all",
    )
    store.record_observation(
        listing=_listing(
            listing_id="title-a7c",
            price="$1200",
            title="Sony A7C camera body",
            description="Great condition.",
        ),
        item_name="sony_a7c2",
        search_city="vancouver",
        search_phrase="sony a7c2",
        availability="all",
    )
    store.record_observation(
        listing=_listing(
            listing_id="desc-a6000",
            price="$700",
            title="Mirrorless camera bundle",
            description=(
                "Selling my Sony A6000 mirrorless camera. Includes: Sony a6000, "
                "Sony tripod and mount."
            ),
        ),
        item_name="sony_a7c2",
        search_city="vancouver",
        search_phrase="sony a7c2",
        availability="all",
    )
    store.record_observation(
        listing=_listing(
            listing_id="title-a7-base",
            price="$900",
            title="Sony a7 body only",
            description="Works perfectly.",
        ),
        item_name="sony_a7c2",
        search_city="vancouver",
        search_phrase="sony a7c2",
        availability="all",
    )
    store.record_observation(
        listing=_listing(
            listing_id="title-a7r6-typo",
            price="$1000",
            title="Sony a7r6 body",
            description="Probably meant a7r family.",
        ),
        item_name="sony_a7c2",
        search_city="vancouver",
        search_phrase="sony a7c2",
        availability="all",
    )
    store.record_observation(
        listing=_listing(
            listing_id="desc-rx1r2",
            price="$2300",
            title="Sony compact camera",
            description="Sony RX1R II full-frame compact camera.",
        ),
        item_name="sony_a7c2",
        search_city="vancouver",
        search_phrase="sony a7c2",
        availability="all",
    )
    store.record_observation(
        listing=_listing(
            listing_id="desc-a6100",
            price="$850",
            title="Sony mirrorless setup",
            description="Includes Sony a6100 body and two batteries.",
        ),
        item_name="sony_a7c2",
        search_city="vancouver",
        search_phrase="sony a7c2",
        availability="all",
    )
    store.record_observation(
        listing=_listing(
            listing_id="desc-rx100m7",
            price="$900",
            title="Sony compact bundle",
            description="Mint Sony RX100M7 with extra batteries.",
        ),
        item_name="sony_a7c2",
        search_city="vancouver",
        search_phrase="sony a7c2",
        availability="all",
    )
    store.record_observation(
        listing=_listing(
            listing_id="desc-a9iii",
            price="$4200",
            title="Sony sports camera",
            description="Selling Sony A9iii body only.",
        ),
        item_name="sony_a7c2",
        search_city="vancouver",
        search_phrase="sony a7c2",
        availability="all",
    )
    store.record_observation(
        listing=_listing(
            listing_id="desc-fx30",
            price="$1500",
            title="Cinema camera",
            description="Sony FX30 in excellent condition.",
        ),
        item_name="sony_a7c2",
        search_city="vancouver",
        search_phrase="sony a7c2",
        availability="all",
    )
    store.record_observation(
        listing=_listing(
            listing_id="desc-a7rm4",
            price="$1800",
            title="Mirrorless camera",
            description="Sony A7RM4 body, low shutter count.",
        ),
        item_name="sony_a7c2",
        search_city="vancouver",
        search_phrase="sony a7c2",
        availability="all",
    )
    store.record_observation(
        listing=_listing(
            listing_id="ricoh-gr2",
            price="$700",
            title="Ricoh GR II street camera",
            description="Compact Ricoh GR2 in great condition.",
        ),
        item_name="sony_a7c2",
        search_city="vancouver",
        search_phrase="sony a7c2",
        availability="all",
    )
    store.record_observation(
        listing=_listing(
            listing_id="ricoh-gr3",
            price="$1000",
            title="Ricoh GR III",
            description="Ricoh griii body only.",
        ),
        item_name="sony_a7c2",
        search_city="vancouver",
        search_phrase="sony a7c2",
        availability="all",
    )
    store.record_observation(
        listing=_listing(
            listing_id="ricoh-gr3x",
            price="$1100",
            title="Ricoh GR3X",
            description="Mint condition griiix.",
        ),
        item_name="sony_a7c2",
        search_city="vancouver",
        search_phrase="sony a7c2",
        availability="all",
    )
    store.record_observation(
        listing=_listing(
            listing_id="ricoh-gr3-hdf",
            price="$1200",
            title="Ricoh GR3 HDF",
            description="Latest Ricoh GR3HDF version.",
        ),
        item_name="sony_a7c2",
        search_city="vancouver",
        search_phrase="sony a7c2",
        availability="all",
    )
    store.record_observation(
        listing=_listing(
            listing_id="ricoh-gr3x-hdf",
            price="$1300",
            title="Ricoh GR3X HDF",
            description="Ricoh GR3XHDF package.",
        ),
        item_name="sony_a7c2",
        search_city="vancouver",
        search_phrase="sony a7c2",
        availability="all",
    )
    store.record_observation(
        listing=_listing(
            listing_id="ricoh-gr4",
            price="$1500",
            title="Ricoh GR4 preorder slot",
            description="GR IV mention.",
        ),
        item_name="sony_a7c2",
        search_city="vancouver",
        search_phrase="sony a7c2",
        availability="all",
    )

    rows = []
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT listing_id, item_name, detected_model
            FROM listing_observations
            ORDER BY listing_id
            """
        ).fetchall()

    by_id = {row[0]: (row[1], row[2]) for row in rows}
    assert by_id["desc-a7sii"] == ("sony_a7sii", "sony_a7sii")
    assert by_id["title-a7riii"] == ("sony_a7riii", "sony_a7riii")
    assert by_id["title-a7c"] == ("sony_a7c", "sony_a7c")
    assert by_id["desc-a6000"] == ("sony_a6000", "sony_a6000")
    assert by_id["title-a7-base"] == ("sony_a7", "sony_a7")
    assert by_id["title-a7r6-typo"] == ("sony_a7r", "sony_a7r")
    assert by_id["desc-rx1r2"] == ("sony_rx1r2", "sony_rx1r2")
    assert by_id["desc-a6100"] == ("sony_a6100", "sony_a6100")
    assert by_id["desc-rx100m7"] == ("sony_rx100vii", "sony_rx100vii")
    assert by_id["desc-a9iii"] == ("sony_a9iii", "sony_a9iii")
    assert by_id["desc-fx30"] == ("sony_fx30", "sony_fx30")
    assert by_id["desc-a7rm4"] == ("sony_a7riv", "sony_a7riv")
    assert by_id["ricoh-gr2"] == ("ricoh_gr2", "ricoh_gr2")
    assert by_id["ricoh-gr3"] == ("ricoh_gr3", "ricoh_gr3")
    assert by_id["ricoh-gr3x"] == ("ricoh_gr3x", "ricoh_gr3x")
    assert by_id["ricoh-gr3-hdf"] == ("ricoh_gr3hdf", "ricoh_gr3hdf")
    assert by_id["ricoh-gr3x-hdf"] == ("ricoh_gr3xhdf", "ricoh_gr3xhdf")
    assert by_id["ricoh-gr4"] == ("ricoh_gr4", "ricoh_gr4")
