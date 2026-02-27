from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from ai_marketplace_monitor.facebook import FacebookItemConfig, FacebookMarketplace
from ai_marketplace_monitor.listing import Listing
from ai_marketplace_monitor.utils import cache


class _SingleListingSearchResultPage:
    def __init__(self, *_args, **_kwargs) -> None:
        pass

    def get_listings(self) -> list[Listing]:
        return [
            Listing(
                marketplace="facebook",
                name="",
                id="auto-msg-1",
                title="Sony A7C II body",
                image="",
                price="$1800",
                post_url="https://www.facebook.com/marketplace/item/auto-msg-1/?ref=search",
                location="Vancouver, BC",
                seller="",
                condition="",
                description="",
            )
        ]


@pytest.fixture
def facebook_marketplace(monkeypatch: pytest.MonkeyPatch) -> FacebookMarketplace:
    marketplace = FacebookMarketplace(name="facebook", browser=MagicMock(), logger=MagicMock())
    marketplace.page = MagicMock()
    marketplace.config = SimpleNamespace(
        condition=None,
        date_listed=[1],
        delivery_method=None,
        availability=["in"],
        collect_sold=False,
        search_city=["vancouver"],
        city_name=["Vancouver"],
        radius=[100],
        currency=["CAD"],
        max_price=None,
        min_price=None,
        category=None,
        market_price_window_days=30,
        seller_locations=None,
        exclude_sellers=None,
        auto_send_message=True,
        message_preset=None,
        message_send_delay=0,
    )

    market_store = MagicMock()
    market_store.get_latest_listing_snapshot.return_value = Listing(
        marketplace="facebook",
        name="sony_a7c2",
        id="auto-msg-1",
        title="Sony A7C II body",
        image="",
        price="$1800",
        post_url="https://www.facebook.com/marketplace/item/auto-msg-1/",
        location="Vancouver, BC",
        seller="seller",
        condition="used_good",
        description="Great condition camera body.",
    )
    market_store.has_observation.return_value = False
    market_store.has_non_out_observation.return_value = False

    monkeypatch.setattr(
        "ai_marketplace_monitor.facebook.FacebookSearchResultPage",
        _SingleListingSearchResultPage,
    )
    monkeypatch.setattr("ai_marketplace_monitor.facebook.time.sleep", lambda _seconds: None)
    monkeypatch.setattr(
        "ai_marketplace_monitor.facebook.get_market_data_store",
        lambda: market_store,
    )
    monkeypatch.setattr(
        "ai_marketplace_monitor.facebook.counter.increment",
        lambda *_args, **_kwargs: None,
    )
    return marketplace


def test_auto_send_message_only_once_per_listing(
    facebook_marketplace: FacebookMarketplace,
) -> None:
    send_mock = MagicMock(return_value=True)
    facebook_marketplace.send_preset_message = send_mock  # type: ignore[method-assign]
    facebook_marketplace.goto_url = lambda _url: None  # type: ignore[method-assign]

    item_config = FacebookItemConfig(
        name="sony_a7c2",
        marketplace="facebook",
        search_phrases=["sony a7c2"],
        keywords=None,
        antikeywords=None,
        auto_send_message=True,
        message_send_delay=0,
    )

    cache_key = facebook_marketplace._sent_message_cache_key("auto-msg-1")
    cache.delete(cache_key)

    try:
        list(facebook_marketplace.search(item_config))
        assert send_mock.call_count == 1

        list(facebook_marketplace.search(item_config))
        assert send_mock.call_count == 1
    finally:
        cache.delete(cache_key)
