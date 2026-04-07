"""
hepsiemlak_detail.py — Hepsiemlak.com tekil ilan URL scraper.

cloudscraper + BeautifulSoup + API JSON cikartma — Selenium yok, hizli.
"""
from __future__ import annotations

import json
import re
from typing import Optional

import requests
try:
    import cloudscraper as _cloudscraper
    _HAS_CLOUDSCRAPER = True
except ImportError:
    _HAS_CLOUDSCRAPER = False
from bs4 import BeautifulSoup

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "tr-TR,tr;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Referer": "https://www.hepsiemlak.com/",
}


def _fetch_html(url: str, proxy_url: Optional[str] = None, timeout: int = 20) -> str:
    proxies = {"http": proxy_url, "https": proxy_url} if proxy_url else None
    # Detay sayfaları için cloudscraper yeterli — Playwright sadece arama sayfaları için
    # İlk deneme: cloudscraper
    if _HAS_CLOUDSCRAPER:
        try:
            scraper = _cloudscraper.create_scraper(browser={"browser": "chrome", "platform": "windows"})
            scraper.headers.update(_HEADERS)
            if proxies:
                scraper.proxies = proxies
            resp = scraper.get(url, timeout=timeout, allow_redirects=True)
            if resp.status_code == 200 and len(resp.text) > 1000:
                return resp.text
            print(f"[HEPSIEMLAK] cloudscraper {resp.status_code} ({url[:60]})")
        except Exception as exc:
            print(f"[HEPSIEMLAK] cloudscraper hatasi ({url[:60]}): {exc}")
    # Fallback: plain requests
    try:
        session = requests.Session()
        session.headers.update(_HEADERS)
        if proxies:
            session.proxies = proxies
        resp = session.get(url, timeout=timeout, allow_redirects=True)
        if resp.status_code in (403, 429, 503):
            print(f"[HEPSIEMLAK] HTTP {resp.status_code} — bot korumasi: {url[:60]}")
            return ""
        return resp.text
    except Exception as exc:
        print(f"[HEPSIEMLAK] Fetch hatasi ({url[:70]}): {exc}")
        return ""


def extract_listing_urls(html: str) -> list[str]:
    """Hepsiemlak arama sayfasından ilan detay URL'lerini çıkar."""
    urls: list[str] = []
    seen: set = set()

    # Absolute URL pattern: https://www.hepsiemlak.com/ilan/...
    for m in re.finditer(r'href="(https://(?:www\.)?hepsiemlak\.com/ilan/[^"?#]+)"', html):
        u = m.group(1)
        if u not in seen:
            seen.add(u)
            urls.append(u)

    # Relative URL pattern: /ilan/...
    for m in re.finditer(r'href="(/ilan/[^"?#]+)"', html):
        full = "https://www.hepsiemlak.com" + m.group(1)
        if full not in seen:
            seen.add(full)
            urls.append(full)

    # JSON içinde gömülü URL'ler (SPA / __NEXT_DATA__ / Nuxt store)
    for m in re.finditer(r'"(?:url|link|href|detailUrl)"\s*:\s*"((?:https://(?:www\.)?hepsiemlak\.com)?/ilan/[^"]+)"', html):
        u = m.group(1)
        if not u.startswith("http"):
            u = "https://www.hepsiemlak.com" + u
        if u not in seen:
            seen.add(u)
            urls.append(u)

    return urls


def _extract_nuxt(html: str) -> dict:
    """window.__NUXT__ veya __NEXT_DATA__ state'ini cikart."""
    # __NEXT_DATA__
    m = re.search(
        r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>([\s\S]*?)</script>', html
    )
    if m:
        try:
            return json.loads(m.group(1))
        except Exception:
            pass

    # window.__NUXT__ = {...}
    m = re.search(r'window\.__NUXT__\s*=\s*(\{[\s\S]*?\});\s*(?:\n|</script>)', html)
    if m:
        try:
            return json.loads(m.group(1))
        except Exception:
            pass

    # Inline JSON-LD
    for m in re.finditer(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>([\s\S]*?)</script>', html
    ):
        try:
            d = json.loads(m.group(1))
            if isinstance(d, dict) and d.get("@type") in ("Apartment", "SingleFamilyResidence", "RealEstateListing", "Product"):
                return {"__jsonld__": d}
        except Exception:
            pass
    return {}


def _parse_soup(soup: BeautifulSoup, url: str) -> dict:
    """BeautifulSoup ile hepsiemlak HTML'den veri cikart."""
    result: dict = {
        "url": url, "domain": "hepsiemlak.com", "source": "apify_hepsiemlak",
        "title": "", "price": None, "currency": "TL",
        "city": "", "district": "", "neighborhood": "",
        "rooms": "", "grossM2": None, "netM2": None,
        "floor": "", "buildingAge": "",
        "hasElevator": False, "hasParking": False,
        "furnished": False, "isCreditEligible": False,
        "images": [], "description": "", "publishedDate": "",
    }

    # Baslik
    for sel in ("h1.listing-title", "h1", ".he-detail-title h1"):
        el = soup.select_one(sel)
        if el and el.get_text(strip=True):
            result["title"] = el.get_text(strip=True)[:200]
            break

    # Fiyat
    for sel in (".listing-price .price-text", ".price", "[class*='price']"):
        el = soup.select_one(sel)
        if el:
            txt = el.get_text(strip=True)
            nums = re.findall(r"[\d]+", txt.replace(".", "").replace(",", ""))
            for n in nums:
                try:
                    v = float(n)
                    if v > 100:
                        result["price"] = v
                        break
                except ValueError:
                    pass
            if result.get("price"):
                break

    # Konum breadcrumb
    for sel in (".breadcrumb a", ".he-detail-breadcrumb a"):
        items = [el.get_text(strip=True) for el in soup.select(sel)]
        items = [t for t in items if t.lower() not in ("anasayfa", "ilanlar", "turkiye", "türkiye")]
        if len(items) >= 2:
            result["city"] = items[0]
            result["district"] = items[1]
            if len(items) >= 3:
                result["neighborhood"] = items[2]
            break

    # Ozellikler listesi
    for li in soup.select(".spec-item, .listing-spec li, .he-detail-spec li"):
        text = li.get_text(strip=True).lower()
        if "asans" in text:
            result["hasElevator"] = True
        if "otopark" in text or "garaj" in text:
            result["hasParking"] = True
        if "krediye uygun" in text:
            result["isCreditEligible"] = True
        # m2
        m2match = re.search(r"(\d+)\s*m", text)
        if m2match:
            val_ = int(m2match.group(1))
            if "brüt" in text or "gross" in text:
                result["grossM2"] = val_
            elif "net" in text:
                result["netM2"] = val_
        # oda
        room_m = re.search(r"(\d+\+\d+)", text)
        if room_m and not result["rooms"]:
            result["rooms"] = room_m.group(1)

    # Gorseller
    images = []
    for img in soup.select(".he-gallery img, .listing-gallery img, .swiper-slide img"):
        src = img.get("src") or img.get("data-src") or img.get("data-lazy") or ""
        if src and "http" in src and src not in images:
            images.append(src)
    if not images:
        og = soup.select_one('meta[property="og:image"]')
        if og:
            images = [og.get("content", "")]
    result["images"] = [i for i in images if i][:15]

    # Aciklama
    desc_el = soup.select_one(".listing-description, .he-detail-description, #description")
    if desc_el:
        result["description"] = desc_el.get_text(strip=True)[:500]

    return result


def scrape_url(url: str, proxy_url: Optional[str] = None) -> Optional[dict]:
    """Hepsiemlak.com ilan detay URL'sini scrape et. dict veya None doner."""
    html = _fetch_html(url, proxy_url=proxy_url)
    if not html or len(html) < 500:
        return None

    # Once JSON state deneyelim
    state = _extract_nuxt(html)

    if state.get("__jsonld__"):
        jld = state["__jsonld__"]
        price = None
        offer = jld.get("offers") or {}
        if isinstance(offer, dict):
            try:
                price = float(offer.get("price") or 0) or None
            except Exception:
                pass
        return {
            "url": url, "domain": "hepsiemlak.com", "source": "apify_hepsiemlak",
            "title": str(jld.get("name") or jld.get("description") or "")[:200],
            "price": price,
            "currency": str((offer.get("priceCurrency") or "TL")),
            "description": str(jld.get("description") or "")[:500],
            "images": [str(jld.get("image") or "")],
        }

    # NUXT/NEXT state icinden listing verisi bul
    pp = (state.get("props") or {}).get("pageProps") or {}
    listing = (
        pp.get("listing") or pp.get("detail") or pp.get("adDetail")
        or state.get("listing") or {}
    )

    if listing and listing.get("title"):
        attrs = listing.get("attributes") or listing.get("specs") or []
        attr_text = " ".join(str(a.get("value") or "").lower() for a in attrs if isinstance(a, dict))
        rooms_val = listing.get("roomCount") or listing.get("rooms") or ""

        return {
            "url": url, "domain": "hepsiemlak.com", "source": "apify_hepsiemlak",
            "title": str(listing.get("title") or "")[:200],
            "price": listing.get("price"),
            "currency": str(listing.get("currency") or "TL"),
            "city": str(listing.get("city") or listing.get("cityName") or ""),
            "district": str(listing.get("district") or listing.get("county") or ""),
            "rooms": str(rooms_val),
            "grossM2": listing.get("grossSqm") or listing.get("grossM2"),
            "netM2": listing.get("netSqm") or listing.get("netM2"),
            "hasElevator": "asans" in attr_text,
            "hasParking": "otopark" in attr_text or "garaj" in attr_text,
            "furnished": bool(listing.get("furnished")),
            "images": (listing.get("images") or [])[:15],
            "description": str(listing.get("description") or "")[:500],
            "publishedDate": str(listing.get("createdDate") or listing.get("publishedDate") or ""),
        }

    # Son fallback: BeautifulSoup HTML parse
    soup = BeautifulSoup(html, "lxml")
    result = _parse_soup(soup, url)
    if not result.get("title"):
        return None
    return result
