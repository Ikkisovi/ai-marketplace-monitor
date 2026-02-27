"""Scrape FilmGrab category pages into per-film folders."""

from __future__ import annotations

import argparse
import html
import json
import random
import re
import time
from dataclasses import dataclass
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlsplit, urlunsplit

import requests

DEFAULT_BASE_URL = "https://film-grab.com"
DEFAULT_DIRECTOR_SLUG = "wong-kar-wai"
DEFAULT_OUTPUT_ROOT = Path("downloads/filmgrab")
DEFAULT_CATEGORY_URL = f"{DEFAULT_BASE_URL}/category/{DEFAULT_DIRECTOR_SLUG}/"
DEFAULT_OUTPUT_DIR = DEFAULT_OUTPUT_ROOT / DEFAULT_DIRECTOR_SLUG
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".avif"}

ABS_MEDIA_URL_RE = re.compile(
    r"https?://film-grab\.com/wp-content/uploads/photo-gallery/[^\s\"'<>]+",
    re.IGNORECASE,
)
REL_MEDIA_URL_RE = re.compile(
    r"/wp-content/uploads/photo-gallery/[^\s\"'<>]+",
    re.IGNORECASE,
)
POST_URL_RE = re.compile(r"^/\d{4}/\d{2}/\d{2}/[^/]+/?$")


class _LinkParser(HTMLParser):
    """Collect anchor and link tags with attributes."""

    def __init__(self) -> None:
        super().__init__()
        self.anchors: list[dict[str, str]] = []
        self.links: list[dict[str, str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr_map = {key.lower(): value for key, value in attrs if key and value is not None}
        lower_tag = tag.lower()
        if lower_tag == "a":
            self.anchors.append(attr_map)
        elif lower_tag == "link":
            self.links.append(attr_map)


class _MediaParser(HTMLParser):
    """Collect candidate media URLs from HTML attributes."""

    URL_ATTRS = {
        "href",
        "src",
        "data-src",
        "data-original",
        "data-lazy-src",
        "data-image-url",
        "data-large-file",
        "data-large_image",
    }

    def __init__(self) -> None:
        super().__init__()
        self.urls: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        lower_tag = tag.lower()
        if lower_tag not in {"a", "img", "source"}:
            return
        for key, value in attrs:
            if key is None or value is None:
                continue
            if key.lower() in self.URL_ATTRS:
                self.urls.append(value)


def _dedupe_preserve_order(urls: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for url in urls:
        if url in seen:
            continue
        seen.add(url)
        ordered.append(url)
    return ordered


def _normalize_url(url: str, base_url: str) -> str:
    absolute = urljoin(base_url, html.unescape(url))
    parts = urlsplit(absolute)
    clean_path = parts.path or "/"
    return urlunsplit((parts.scheme, parts.netloc, clean_path, "", ""))


def _host(url: str) -> str:
    return urlsplit(url).netloc.lower()


def _path(url: str) -> str:
    return urlsplit(url).path


def _is_image_url(url: str) -> bool:
    lower_path = _path(url).lower()
    return any(lower_path.endswith(ext) for ext in IMAGE_EXTENSIONS)


def _split_space_attr(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip().lower() for item in value.split() if item.strip()]


def extract_post_links(category_html: str, base_url: str = DEFAULT_BASE_URL) -> list[str]:
    """Extract film post URLs from a category page."""

    parser = _LinkParser()
    parser.feed(category_html)
    base_host = _host(base_url)

    links: list[str] = []
    for attrs in parser.anchors:
        href = attrs.get("href")
        if not href:
            continue
        normalized = _normalize_url(href, base_url)
        if _host(normalized) != base_host:
            continue
        path = _path(normalized)
        if not POST_URL_RE.match(path):
            continue
        if not path.endswith("/"):
            normalized = f"{normalized}/"
        links.append(normalized)

    return _dedupe_preserve_order(links)


def extract_next_page_url(
    category_html: str,
    current_url: str,
    base_url: str = DEFAULT_BASE_URL,
) -> str | None:
    """Extract the next category page URL if available."""

    parser = _LinkParser()
    parser.feed(category_html)

    for attrs in parser.anchors:
        href = attrs.get("href")
        if not href:
            continue
        classes = _split_space_attr(attrs.get("class"))
        rel = _split_space_attr(attrs.get("rel"))
        if "next" not in classes and "next" not in rel:
            continue
        return _normalize_url(href, base_url)

    for attrs in parser.links:
        href = attrs.get("href")
        rel = _split_space_attr(attrs.get("rel"))
        if href and "next" in rel:
            return _normalize_url(href, base_url)

    return None


def extract_zip_links(post_html: str, base_url: str = DEFAULT_BASE_URL) -> list[str]:
    """Extract ZIP download links from a film post."""

    parser = _LinkParser()
    parser.feed(post_html)
    base_host = _host(base_url)

    zips: list[str] = []
    for attrs in parser.anchors:
        href = attrs.get("href")
        if not href:
            continue
        normalized = _normalize_url(href, base_url)
        if _host(normalized) != base_host:
            continue
        if _path(normalized).lower().endswith(".zip"):
            zips.append(normalized)

    return _dedupe_preserve_order(zips)


def extract_images(post_html: str, base_url: str = DEFAULT_BASE_URL) -> list[str]:
    """Extract image URLs from a film post gallery."""

    text = html.unescape(post_html)
    parser = _MediaParser()
    parser.feed(post_html)

    candidates: list[str] = []
    candidates.extend(ABS_MEDIA_URL_RE.findall(text))
    for rel in REL_MEDIA_URL_RE.findall(text):
        candidates.append(urljoin(base_url, rel))
    candidates.extend(parser.urls)

    cleaned = [_normalize_url(candidate, base_url) for candidate in candidates]
    base_host = _host(base_url)
    images = []
    for url in cleaned:
        path = _path(url).lower()
        if _host(url) != base_host:
            continue
        if "/wp-content/uploads/photo-gallery/" not in path:
            continue
        if "/wp-content/uploads/photo-gallery/thumb/" in path:
            continue
        if _is_image_url(url):
            images.append(url)

    return _dedupe_preserve_order(images)


def extract_post_title(post_html: str, fallback: str) -> str:
    """Extract the post title, falling back to a derived name."""

    match = re.search(r"<h1[^>]*>(.*?)</h1>", post_html, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return fallback

    text = re.sub(r"<[^>]+>", " ", match.group(1))
    clean = " ".join(html.unescape(text).split())
    return clean or fallback


def slugify(text: str) -> str:
    """Convert a title to a filesystem-safe slug."""

    slug = re.sub(r"[^a-zA-Z0-9]+", "-", text.lower()).strip("-")
    return slug or "untitled"


def normalize_director_slug(director: str) -> str:
    """Normalize a director name into a category slug."""

    normalized = re.sub(r"[^a-zA-Z0-9]+", "-", director.strip().lower()).strip("-")
    return normalized or DEFAULT_DIRECTOR_SLUG


def build_category_url(base_url: str, director_slug: str) -> str:
    """Build a FilmGrab category URL from base URL and director slug."""

    return f"{base_url.rstrip('/')}/category/{director_slug.strip('/')}/"


def build_output_dir(director_slug: str, output_root: Path = DEFAULT_OUTPUT_ROOT) -> Path:
    """Build default output directory for a director."""

    return output_root / director_slug


def resolve_category_and_output(
    base_url: str,
    director: str,
    category_url: str | None,
    output_dir: str | Path | None,
) -> tuple[str, str, Path]:
    """Resolve director slug, category URL, and output directory."""

    director_slug = normalize_director_slug(director)
    resolved_category_url = category_url or build_category_url(base_url, director_slug)
    resolved_output_dir = Path(output_dir) if output_dir else build_output_dir(director_slug)
    return director_slug, resolved_category_url, resolved_output_dir


@dataclass
class FilmScrapeResult:
    """Summary for one film post."""

    title: str
    slug: str
    post_url: str
    folder: str
    image_count: int
    zip_count: int
    downloaded_images: int
    downloaded_zips: int
    errors: list[str]


def _sleep_random(min_seconds: float, max_seconds: float) -> None:
    if max_seconds <= 0:
        return
    if min_seconds > max_seconds:
        min_seconds, max_seconds = max_seconds, min_seconds
    delay = random.uniform(min_seconds, max_seconds)
    if delay > 0:
        time.sleep(delay)


def _request_text(session: requests.Session, url: str, timeout: int) -> str:
    response = session.get(url, timeout=timeout)
    response.raise_for_status()
    return response.text


def _download_file(
    session: requests.Session,
    url: str,
    out_path: Path,
    timeout: int,
    referer: str | None,
    skip_existing: bool,
) -> bool:
    if skip_existing and out_path.exists():
        return False

    headers: dict[str, str] = {}
    if referer:
        headers["Referer"] = referer

    response = session.get(url, headers=headers, timeout=timeout)
    response.raise_for_status()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(response.content)
    return True


def _extension_from_url(url: str, fallback: str) -> str:
    path = _path(url)
    suffix = Path(path).suffix.lower()
    return suffix if suffix else fallback


def _iter_category_posts(
    session: requests.Session,
    category_url: str,
    base_url: str,
    timeout: int,
    max_pages: int | None,
    page_delay_min: float,
    page_delay_max: float,
) -> list[str]:
    posts: list[str] = []
    visited_pages: set[str] = set()
    page_url: str | None = category_url
    page_count = 0

    while page_url and page_url not in visited_pages:
        if max_pages is not None and page_count >= max_pages:
            break

        visited_pages.add(page_url)
        html_text = _request_text(session, page_url, timeout)
        posts.extend(extract_post_links(html_text, base_url))
        page_count += 1

        next_page = extract_next_page_url(html_text, page_url, base_url)
        if not next_page or next_page in visited_pages:
            break

        page_url = next_page
        _sleep_random(page_delay_min, page_delay_max)

    return _dedupe_preserve_order(posts)


def scrape_category(
    category_url: str = DEFAULT_CATEGORY_URL,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    base_url: str = DEFAULT_BASE_URL,
    download_mode: str = "auto",
    max_films: int | None = None,
    max_pages: int | None = None,
    timeout: int = 45,
    item_delay_min: float = 0.2,
    item_delay_max: float = 0.6,
    page_delay_min: float = 0.8,
    page_delay_max: float = 1.8,
    skip_existing: bool = True,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Scrape all films in a FilmGrab category page."""

    if download_mode not in {"auto", "zip", "images", "both"}:
        msg = f"Unsupported download mode: {download_mode}"
        raise ValueError(msg)

    output_dir.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (compatible; filmgrab-scraper/1.0)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
    )

    post_urls = _iter_category_posts(
        session=session,
        category_url=category_url,
        base_url=base_url,
        timeout=timeout,
        max_pages=max_pages,
        page_delay_min=page_delay_min,
        page_delay_max=page_delay_max,
    )

    if max_films is not None:
        post_urls = post_urls[:max_films]

    film_results: list[FilmScrapeResult] = []

    for index, post_url in enumerate(post_urls, start=1):
        fallback_name = post_url.rstrip("/").split("/")[-1].replace("-", " ").title()
        film_errors: list[str] = []

        try:
            post_html = _request_text(session, post_url, timeout)
        except requests.RequestException as exc:
            film_errors.append(str(exc))
            film_results.append(
                FilmScrapeResult(
                    title=fallback_name,
                    slug=slugify(fallback_name),
                    post_url=post_url,
                    folder=str(output_dir / slugify(fallback_name)),
                    image_count=0,
                    zip_count=0,
                    downloaded_images=0,
                    downloaded_zips=0,
                    errors=film_errors,
                )
            )
            continue

        title = extract_post_title(post_html, fallback_name)
        slug = slugify(title)
        film_dir = output_dir / slug
        zip_links = extract_zip_links(post_html, base_url)
        image_links = extract_images(post_html, base_url)

        use_zip = download_mode in {"zip", "both"} or (download_mode == "auto" and bool(zip_links))
        use_images = download_mode in {"images", "both"} or (
            download_mode == "auto" and not zip_links
        )

        downloaded_zips = 0
        downloaded_images = 0

        if use_zip:
            for zip_index, zip_url in enumerate(zip_links, start=1):
                zip_name = Path(_path(zip_url)).name or f"gallery-{zip_index:02d}.zip"
                out_path = film_dir / zip_name
                try:
                    if not dry_run:
                        saved = _download_file(
                            session=session,
                            url=zip_url,
                            out_path=out_path,
                            timeout=timeout,
                            referer=post_url,
                            skip_existing=skip_existing,
                        )
                        if saved:
                            downloaded_zips += 1
                    else:
                        downloaded_zips += 1
                except requests.RequestException as exc:
                    film_errors.append(f"ZIP download failed: {zip_url} ({exc})")
                _sleep_random(item_delay_min, item_delay_max)

        if use_images:
            for image_index, image_url in enumerate(image_links, start=1):
                ext = _extension_from_url(image_url, ".jpg")
                out_path = film_dir / f"{image_index:03d}{ext}"
                try:
                    if not dry_run:
                        saved = _download_file(
                            session=session,
                            url=image_url,
                            out_path=out_path,
                            timeout=timeout,
                            referer=post_url,
                            skip_existing=skip_existing,
                        )
                        if saved:
                            downloaded_images += 1
                    else:
                        downloaded_images += 1
                except requests.RequestException as exc:
                    film_errors.append(f"Image download failed: {image_url} ({exc})")
                _sleep_random(item_delay_min, item_delay_max)

        film_results.append(
            FilmScrapeResult(
                title=title,
                slug=slug,
                post_url=post_url,
                folder=str(film_dir),
                image_count=len(image_links),
                zip_count=len(zip_links),
                downloaded_images=downloaded_images,
                downloaded_zips=downloaded_zips,
                errors=film_errors,
            )
        )

        print(
            f"[{index}/{len(post_urls)}] {title} | "
            f"images: {len(image_links)} (saved {downloaded_images}) | "
            f"zips: {len(zip_links)} (saved {downloaded_zips})"
        )

        _sleep_random(page_delay_min, page_delay_max)

    report = {
        "category_url": category_url,
        "download_mode": download_mode,
        "output_dir": str(output_dir),
        "film_count": len(film_results),
        "films": [result.__dict__ for result in film_results],
    }

    index_path = output_dir / "index.json"
    index_path.write_text(json.dumps(report, indent=2, ensure_ascii=False))

    return report


def build_parser() -> argparse.ArgumentParser:
    """Build CLI argument parser."""

    parser = argparse.ArgumentParser(
        description=(
            "Scrape FilmGrab director category pages and download stills organized by film."
        )
    )
    parser.add_argument(
        "--director",
        default=DEFAULT_DIRECTOR_SLUG,
        help="Director name or slug, e.g. 'wong-kar-wai' or 'Ye Lou'.",
    )
    parser.add_argument(
        "--category-url",
        default=None,
        help="Optional explicit category URL override.",
    )
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Optional output directory override; defaults to downloads/filmgrab/<director-slug>.",
    )
    parser.add_argument(
        "--download-mode",
        choices=["auto", "zip", "images", "both"],
        default="auto",
        help="auto uses ZIP when available; otherwise image-by-image downloads.",
    )
    parser.add_argument("--max-films", type=int, default=None)
    parser.add_argument("--max-pages", type=int, default=None)
    parser.add_argument("--timeout", type=int, default=45)
    parser.add_argument("--item-delay-min", type=float, default=0.2)
    parser.add_argument("--item-delay-max", type=float, default=0.6)
    parser.add_argument("--page-delay-min", type=float, default=0.8)
    parser.add_argument("--page-delay-max", type=float, default=1.8)
    parser.add_argument(
        "--skip-existing",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Skip already-downloaded files when true.",
    )
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main() -> None:
    """CLI entrypoint for FilmGrab scraper."""

    args = build_parser().parse_args()
    director_slug, category_url, output_dir = resolve_category_and_output(
        base_url=args.base_url,
        director=args.director,
        category_url=args.category_url,
        output_dir=args.output_dir,
    )
    report = scrape_category(
        category_url=category_url,
        output_dir=output_dir,
        base_url=args.base_url,
        download_mode=args.download_mode,
        max_films=args.max_films,
        max_pages=args.max_pages,
        timeout=args.timeout,
        item_delay_min=args.item_delay_min,
        item_delay_max=args.item_delay_max,
        page_delay_min=args.page_delay_min,
        page_delay_max=args.page_delay_max,
        skip_existing=args.skip_existing,
        dry_run=args.dry_run,
    )

    zip_film_count = sum(1 for film in report["films"] if film["zip_count"] > 0)
    print(
        "Completed scrape: "
        f"director={director_slug}, {report['film_count']} films, {zip_film_count} with ZIP links, "
        f"index at {Path(report['output_dir']) / 'index.json'}"
    )


if __name__ == "__main__":
    main()
