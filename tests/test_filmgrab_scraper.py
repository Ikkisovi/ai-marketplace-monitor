"""Tests for FilmGrab Wong Kar Wai scraper utilities."""

from ai_marketplace_monitor.filmgrab_scraper import (
    build_category_url,
    extract_images,
    extract_next_page_url,
    extract_post_links,
    extract_zip_links,
    normalize_director_slug,
    resolve_category_and_output,
)


def test_extract_post_links_unique_absolute_urls() -> None:
    html = """
    <html><body>
      <h2 class="entry-title"><a href="/2014/10/20/chungking-express/">Chungking</a></h2>
      <h2 class="entry-title"><a href="https://film-grab.com/2013/03/09/in-the-mood-for-love/">ITMFL</a></h2>
      <h2 class="entry-title"><a href="/2014/10/20/chungking-express/">Duplicate</a></h2>
      <h2 class="entry-title"><a href="https://example.com/not-filmgrab">Ignore</a></h2>
    </body></html>
    """

    links = extract_post_links(html, "https://film-grab.com")

    assert links == [
        "https://film-grab.com/2014/10/20/chungking-express/",
        "https://film-grab.com/2013/03/09/in-the-mood-for-love/",
    ]


def test_extract_next_page_url() -> None:
    html = """
    <html><body>
      <a class="next page-numbers" href="https://film-grab.com/category/wong-kar-wai/page/2/">Next</a>
    </body></html>
    """

    next_page = extract_next_page_url(
        html,
        "https://film-grab.com/category/wong-kar-wai/",
        "https://film-grab.com",
    )

    assert next_page == "https://film-grab.com/category/wong-kar-wai/page/2/"


def test_extract_zip_links_detects_download_archives() -> None:
    html = """
    <html><body>
      <div class="entry-content">
        <a href="https://film-grab.com/wp-content/uploads/photo-gallery/chungking/chungking-express.zip">
          Download this gallery
        </a>
        <a href="https://film-grab.com/not-a-zip">not zip</a>
      </div>
    </body></html>
    """

    zips = extract_zip_links(html, "https://film-grab.com")

    assert zips == [
        "https://film-grab.com/wp-content/uploads/photo-gallery/chungking/chungking-express.zip",
    ]


def test_extract_images_collects_gallery_urls_and_dedupes() -> None:
    html = """
    <html><body>
      <img src="https://film-grab.com/wp-content/uploads/photo-gallery/chungking/image-01.jpg" />
      <img data-src="https://film-grab.com/wp-content/uploads/photo-gallery/chungking/image-02.jpg" />
      <script>
        var img = "https://film-grab.com/wp-content/uploads/photo-gallery/chungking/image-03.jpg";
      </script>
      <a href="https://film-grab.com/wp-content/uploads/photo-gallery/chungking/image-02.jpg">dup</a>
    </body></html>
    """

    images = extract_images(html)

    assert images == [
        "https://film-grab.com/wp-content/uploads/photo-gallery/chungking/image-01.jpg",
        "https://film-grab.com/wp-content/uploads/photo-gallery/chungking/image-02.jpg",
        "https://film-grab.com/wp-content/uploads/photo-gallery/chungking/image-03.jpg",
    ]


def test_extract_images_supports_spaces_and_ignores_thumbs() -> None:
    html = """
    <html><body>
      <a href="https://film-grab.com/wp-content/uploads/photo-gallery/01 (494).jpg?bwg=1547222278">full</a>
      <img src="https://film-grab.com/wp-content/uploads/photo-gallery/thumb/01 (494).jpg?bwg=1547222278" />
    </body></html>
    """

    images = extract_images(html)

    assert images == [
        "https://film-grab.com/wp-content/uploads/photo-gallery/01 (494).jpg",
    ]


def test_normalize_director_slug_from_name() -> None:
    assert normalize_director_slug("Ye Lou") == "ye-lou"
    assert normalize_director_slug("Wong Kar-wai") == "wong-kar-wai"


def test_resolve_category_and_output_uses_director_defaults() -> None:
    director_slug, category_url, output_dir = resolve_category_and_output(
        base_url="https://film-grab.com",
        director="Ye Lou",
        category_url=None,
        output_dir=None,
    )

    assert director_slug == "ye-lou"
    assert category_url == build_category_url("https://film-grab.com", "ye-lou")
    assert str(output_dir) == "downloads/filmgrab/ye-lou"


def test_resolve_category_and_output_respects_explicit_overrides() -> None:
    director_slug, category_url, output_dir = resolve_category_and_output(
        base_url="https://film-grab.com",
        director="Ye Lou",
        category_url="https://film-grab.com/category/custom-director/",
        output_dir="downloads/filmgrab/custom",
    )

    assert director_slug == "ye-lou"
    assert category_url == "https://film-grab.com/category/custom-director/"
    assert str(output_dir) == "downloads/filmgrab/custom"
