"""
sahibinden_detail.py — Sahibinden.com tekil ilan URL scraper.

Camoufox (Firefox, CF bypass) + BeautifulSoup HTML parse.
Mevcut sahibinden_scraper modülü yeniden kullanılır; eksik alanlar (price,
title, city, district, description, images) burada çıkarılır.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Optional

_SH_SRC = Path("/usr/src/app/scrapers/sahibinden")
if not _SH_SRC.exists():
    _SH_SRC = Path(__file__).parent.parent.parent / "sahibinden-scraper" / "src"
if str(_SH_SRC) not in sys.path:
    sys.path.insert(0, str(_SH_SRC))

import sahibinden_scraper as _sh  # noqa: E402
from bs4 import BeautifulSoup      # noqa: E402


# ---------------------------------------------------------------------------
# Kapsamlı HTML parser — liste sayfasından gelmediği için tüm anı tek geçişte
# ---------------------------------------------------------------------------

def _parse_full_detail(soup: BeautifulSoup, url: str) -> dict:
    """Sahibinden.com detay sayfasından tüm alanları çıkar."""
    result: dict = {
        "url":         url,
        "domain":      "sahibinden.com",
        "source":      "apify_sahibinden",
        "title":       "",
        "price":       None,
        "currency":    "TL",
        "city":        "",
        "district":    "",
        "neighborhood": "",
        "rooms":       "",
        "gross_sqm":   None,
        "net_sqm":     None,
        "floor":       "",
        "floor_count": "",
        "building_age": "",
        "property_type": "",
        "furnished":   False,
        "images":      [],
        "description": "",
        "publishedDate": "",
        "advertiser_name": "",
        "advertiser_type": "",
    }

    # ── Başlık ──
    for sel in ("h1.classifiedDetailTitle", "h1.classifiedInfoTitle", "h1"):
        el = soup.select_one(sel)
        if el and el.get_text(strip=True):
            result["title"] = el.get_text(strip=True)[:200]
            break

    # ── Fiyat ──
    for sel in (".classifiedPrice h3", ".artPrice", "[class*='price'] h3",
                ".priceValue", ".listing-price h3"):
        el = soup.select_one(sel)
        if el:
            text = el.get_text(strip=True)
            # Para birimi
            if "USD" in text or "$" in text:
                result["currency"] = "USD"
            elif "EUR" in text or "€" in text:
                result["currency"] = "EUR"
            else:
                result["currency"] = "TL"
            # Sayısal değer: "4.500.000 TL" → 4500000.0
            nums = re.findall(r"[\d]+", text.replace(".", "").replace(",", ""))
            for n in nums:
                try:
                    val = float(n)
                    if val > 100:
                        result["price"] = val
                        break
                except ValueError:
                    pass
            if result["price"]:
                break

    # ── Konum — breadcrumb ──
    crumb_items = soup.select(
        ".classifiedInfoBreadCrumb li a, .breadcrumb li a, [class*='breadcrumb'] a"
    )
    crumb_texts = [el.get_text(strip=True) for el in crumb_items if el.get_text(strip=True)]
    # Tipik yapı: Anasayfa > İlan Yeri > İstanbul > Kadıköy > Moda
    non_trivial = [t for t in crumb_texts if t.lower() not in ("anasayfa", "ilanlar", "türkiye")]
    if len(non_trivial) >= 2:
        result["city"]     = non_trivial[0]
        result["district"] = non_trivial[1]
        if len(non_trivial) >= 3:
            result["neighborhood"] = non_trivial[2]
    elif len(non_trivial) == 1:
        result["city"] = non_trivial[0]

    # Fallback: URL'den konum çıkar
    if not result["city"]:
        # sahibinden.com/satilik/istanbul-kadikoy-...
        m = re.search(r"/(satilik|kiralik)[/-]([a-z]+)-?([a-z]+)?", url)
        if m:
            result["city"] = m.group(2).title() if m.group(2) else ""
            result["district"] = m.group(3).title() if m.group(3) else ""

    # ── Supplemental alanlar (mevcut parser ile) ──
    extra = _sh.parse_detail_page(soup)
    result.update(extra)

    # ── Açıklama ──
    desc_el = (
        soup.select_one(".classifiedDescription")
        or soup.select_one("#ilanAciklamasi")
        or soup.select_one("[class*='description']")
    )
    if desc_el:
        result["description"] = desc_el.get_text(strip=True)[:500]

    # ── Görseller ──
    images = []
    # Thumbnail listesi
    for img in soup.select(
        ".classifiedDetailPhotos img, #thumbListUl img, .gallery img, .slick-slide img"
    ):
        src = img.get("src") or img.get("data-src") or ""
        if src and "http" in src and src not in images:
            images.append(src)
    # OG image fallback
    if not images:
        og = soup.select_one('meta[property="og:image"]')
        if og and og.get("content"):
            images = [og["content"]]
    result["images"] = images[:15]

    # ── İlan tarihi ──
    date_el = soup.select_one(
        ".classifiedInfoList .classifiedInfoListItem:last-child span,"
        ".ilan-tarihi, [class*='listingDate'], time"
    )
    if date_el:
        result["publishedDate"] = date_el.get("datetime") or date_el.get_text(strip=True)

    return result


def scrape_url(url: str, cookies: Optional[list] = None) -> Optional[dict]:
    """Sahibinden.com ilan detay URL'sini scrape et. dict veya None döner."""
    if cookies:
        _sh.SESSION_COOKIES = cookies

    ctx = None
    try:
        ctx, pw_page = _sh.create_working_context("[UNIFIED]")
        html = _sh.fetch_detail_html(pw_page, url)

        if not html or len(html) < 1000:
            return None

        soup = BeautifulSoup(html, "lxml")
        return _parse_full_detail(soup, url)

    except Exception as exc:
        print(f"[SAHIBINDEN_DETAIL] Hata ({url[:70]}): {exc}")
        return None
    finally:
        if ctx:
            try:
                ctx.close()
            except Exception:
                pass
