from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from ai_marketplace_monitor.facebook import (
    FacebookItemConfig,
    FacebookMarketplace,
    _infer_market_status,
)
from ai_marketplace_monitor.listing import Listing


class _EmptySearchResultPage:
    def __init__(self, *_args, **_kwargs) -> None:
        pass

    def get_listings(self) -> list:
        return []


@pytest.fixture
def facebook_marketplace(monkeypatch: pytest.MonkeyPatch) -> FacebookMarketplace:
    marketplace = FacebookMarketplace(name="facebook", browser=MagicMock(), logger=MagicMock())
    marketplace.page = MagicMock()
    marketplace.config = SimpleNamespace(
        condition=["used_good"],
        date_listed=[1],
        delivery_method=["shipping"],
        availability=["in"],
        collect_sold=True,
        search_city=["vancouver"],
        city_name=["Vancouver"],
        radius=["100"],
        currency=["CAD"],
        max_price="2000",
        min_price="1",
        category="electronics",
        market_price_window_days=30,
        seller_locations=None,
        exclude_sellers=None,
    )

    monkeypatch.setattr("ai_marketplace_monitor.facebook.FacebookSearchResultPage", _EmptySearchResultPage)
    monkeypatch.setattr("ai_marketplace_monitor.facebook.time.sleep", lambda _seconds: None)
    monkeypatch.setattr(
        "ai_marketplace_monitor.facebook.get_market_data_store",
        lambda: MagicMock(),
    )
    monkeypatch.setattr(
        "ai_marketplace_monitor.facebook.counter.increment",
        lambda *_args, **_kwargs: None,
    )
    return marketplace


def test_sold_pass_uses_only_broad_filters(facebook_marketplace: FacebookMarketplace) -> None:
    visited_urls: list[str] = []
    facebook_marketplace.goto_url = lambda url: visited_urls.append(url)

    item_config = FacebookItemConfig(
        name="sony_a7c2",
        marketplace="facebook",
        search_phrases=["sony a7c2"],
        min_price=None,
        max_price=None,
        keywords=None,
        antikeywords=None,
    )

    list(facebook_marketplace.search(item_config))

    assert len(visited_urls) == 2
    sold_url = next(url for url in visited_urls if "out%20of%20stock" in url)
    non_sold_url = next(url for url in visited_urls if "in%20stock" in url)

    assert "exact=false" in sold_url
    assert "daysSinceListed=1" in sold_url
    assert "availability=out%20of%20stock" in sold_url
    assert "maxPrice=" not in sold_url
    assert "minPrice=" not in sold_url
    assert "radius=" not in sold_url
    assert "itemCondition=" not in sold_url
    assert "deliveryMethod=" not in sold_url
    assert "category=" not in sold_url

    assert "exact=false" in non_sold_url
    assert "daysSinceListed=1" in non_sold_url
    assert "availability=in%20stock" in non_sold_url
    assert "maxPrice=2000" in non_sold_url
    assert "minPrice=1" in non_sold_url
    assert "radius=100" in non_sold_url


def test_infer_market_status_prefers_pending_then_sold() -> None:
    assert _infer_market_status("Sold · Sony A7C") == "sold"
    assert _infer_market_status("Pending · Sony A7C") == "pending"
    assert _infer_market_status("Pending · Sold · Sony A7C") == "pending"
    assert _infer_market_status("Sony Alpha A230 with 18-55mm SAM") == "unknown"


def test_sold_pass_skips_non_sold_results_without_explicit_sold_marker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _SingleListingSearchResultPage:
        def __init__(self, *_args, **_kwargs) -> None:
            pass

        def get_listings(self) -> list[Listing]:
            listing = Listing(
                marketplace="facebook",
                name="",
                id="4643304949230540",
                title="Sony Alpha A230 with 18-55mm SAM",
                image="",
                price="CA$60 | CA$250",
                post_url="https://www.facebook.com/marketplace/item/4643304949230540/",
                location="Vancouver, BC",
                seller="",
                condition="",
                description="",
            )
            listing.market_status = "unknown"
            return [listing]

    marketplace = FacebookMarketplace(name="facebook", browser=MagicMock(), logger=MagicMock())
    marketplace.page = MagicMock()
    marketplace.config = SimpleNamespace(
        condition=["used_good"],
        date_listed=[1],
        delivery_method=None,
        availability=["out"],
        collect_sold=False,
        search_city=["vancouver"],
        city_name=["Vancouver"],
        radius=["100"],
        currency=["CAD"],
        max_price="2000",
        min_price="1",
        category=None,
        market_price_window_days=30,
        seller_locations=None,
        exclude_sellers=None,
        auto_send_message=False,
        message_preset=None,
        message_send_delay=0,
    )

    market_store = MagicMock()
    market_store.get_latest_listing_snapshot.return_value = Listing(
        marketplace="facebook",
        name="sony_a7c2",
        id="4643304949230540",
        title="Sony Alpha A230 with 18-55mm SAM",
        image="",
        price="CA$60 | CA$250",
        post_url="https://www.facebook.com/marketplace/item/4643304949230540/",
        location="Vancouver, BC",
        seller="seller",
        condition="used_good",
        description="In very good condition",
    )
    market_store.has_observation.return_value = False
    market_store.has_non_out_observation.return_value = False
    market_store.refresh_market_price.return_value = (0, None, None)

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
    marketplace.goto_url = lambda _url: None  # type: ignore[method-assign]

    item_config = FacebookItemConfig(
        name="sony_a7c2",
        marketplace="facebook",
        search_phrases=["sony a7c2"],
        availability=["out"],
        min_price=None,
        max_price=None,
        keywords=None,
        antikeywords=None,
        collect_sold=False,
    )

    list(marketplace.search(item_config))

    market_store.record_observation.assert_not_called()
