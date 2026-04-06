"""
hepsiemlak_detail.py — Hepsiemlak.com tekil ilan URL scraper.

Chrome (Selenium) + window.__NUXT__ state çıkarımı.
parse_detail_row() mevcut scraper'dan yeniden kullanılır.
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

_HE_SRC = Path("/usr/src/app/scrapers/hepsiemlak")
if not _HE_SRC.exists():
    _HE_SRC = Path(__file__).parent.parent.parent / "hepsiemlak-scraper" / "src"
if str(_HE_SRC) not in sys.path:
    sys.path.insert(0, str(_HE_SRC))

import hepsiemlak_scraper as _he  # noqa: E402


def scrape_url(url: str) -> Optional[dict]:
    """Hepsiemlak.com ilan detay URL'sini scrape et. dict veya None döner."""
    driver = _he.create_driver(headless=False, no_images=True, proxy_url=None)
    try:
        # fetch_nuxt_data: driver.get(url) + window.__NUXT__ bekleme + parse
        nuxt_data = _he.fetch_nuxt_data(
            driver, url,
            attempts=3,
            settle_seconds=3.0,
            cf_timeout=20,
        )
        # parse_detail_row: nuxt_data["detailData"] → tam ilan dict
        row = _he.parse_detail_row(url, nuxt_data)
        row["domain"] = "hepsiemlak.com"
        row["source"] = "apify_hepsiemlak"
        return row

    except Exception as exc:
        print(f"[HEPSIEMLAK_DETAIL] Hata ({url[:70]}): {exc}")
        return None
    finally:
        try:
            driver.quit()
        except Exception:
            pass
