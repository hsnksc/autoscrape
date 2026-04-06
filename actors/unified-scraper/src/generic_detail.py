"""
generic_detail.py — Genel HTTP scraper.

zingat.com, hurriyetemlak.com ve diğer siteler için.
Tarayıcı gerekmez; requests + BeautifulSoup + JSON-LD / __NEXT_DATA__ kombinasyonu.
"""
from __future__ import annotations

import json
import re
import time
from typing import Optional
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "tr-TR,tr;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
}


def _fetch_html(url: str, timeout: int = 20) -> str:
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=timeout)
        resp.raise_for_status()
        return resp.text
    except Exception as exc:
        print(f"[GENERIC] Fetch hatası ({url[:70]}): {exc}")
        return ""


def _extract_next_data(html: str) -> dict:
    """<script id="__NEXT_DATA__"> JSON bloğunu çıkar."""
    m = re.search(
        r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>([\s\S]*?)</script>',
        html,
    )
    if m:
        try:
            return json.loads(m.group(1))
        except Exception:
            pass
    return {}


def _extract_json_ld(html: str) -> list[dict]:
    """Tüm application/ld+json bloklarını çıkar."""
    results = []
    for m in re.finditer(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>([\s\S]*?)</script>',
        html,
    ):
        try:
            results.append(json.loads(m.group(1)))
        except Exception:
            pass
    return results


def _parse_price(text: str) -> tuple[Optional[float], str]:
    """'4.500.000 TL' → (4500000.0, 'TL')"""
    if not text:
        return None, "TL"
    currency = "TL"
    if "USD" in text or "$" in text:
        currency = "USD"
    elif "EUR" in text or "€" in text:
        currency = "EUR"
    # "4.500.000" → "4500000"
    clean = re.sub(r"[^\d]", "", text.replace(",", ""))
    try:
        val = float(clean) if clean else None
        if val and val > 100:
            return val, currency
    except ValueError:
        pass
    return None, currency


def _from_next_data(nd: dict, url: str, result: dict) -> bool:
    """__NEXT_DATA__ → result dict. Doldurduğu alanlar varsa True döner."""
    pp = (nd.get("props") or {}).get("pageProps") or {}
    ad = (
        pp.get("listing")
        or pp.get("ad")
        or pp.get("propertyDetails")
        or pp.get("detail")
        or pp.get("property")
        or {}
    )
    if not isinstance(ad, dict) or not ad:
        return False

    def _str_field(keys: list) -> str:
        for k in keys:
            v = ad.get(k)
            if isinstance(v, dict):
                v = v.get("name") or v.get("title") or ""
            if v:
                return str(v).strip()
        return ""

    result["title"]     = _str_field(["title", "name", "baslik"])
    result["city"]      = _str_field(["cityName", "city", "il"])
    result["district"]  = _str_field(["districtName", "district", "county", "ilce"])
    result["neighborhood"] = _str_field(["neighborhoodName", "neighborhood", "mahalle"])
    result["rooms"]     = _str_field(["roomCount", "rooms", "odaSayisi"])
    result["floor"]     = _str_field(["floor", "floorName", "kat"])
    result["buildingAge"] = _str_field(["buildingAge", "age", "binaYasi"])
    result["description"] = _str_field(["description", "aciklama"])[:500]
    result["publishedDate"] = _str_field(["createDate", "createdDate", "publishedDate"])

    # Fiyat
    price_raw = ad.get("price") or ad.get("salePrice") or ad.get("fiyat")
    if price_raw is not None:
        try:
            result["price"] = float(str(price_raw).replace(".", "").replace(",", "."))
        except (ValueError, TypeError):
            pass

    # m²
    try:
        result["grossM2"] = float(ad.get("grossSqm") or ad.get("squareMeters") or 0) or None
    except (ValueError, TypeError):
        pass
    try:
        result["netM2"] = float(ad.get("netSqm") or ad.get("netSquareMeters") or 0) or None
    except (ValueError, TypeError):
        pass

    # Koordinat
    result["lat"] = str(ad.get("latitude") or ad.get("lat") or "")
    result["lon"] = str(ad.get("longitude") or ad.get("lon") or "")

    # Görseller
    imgs_raw = ad.get("images") or ad.get("photos") or []
    result["images"] = [
        str(img.get("url") or img.get("src") or img) if isinstance(img, dict) else str(img)
        for img in imgs_raw
        if img
    ][:15]

    return bool(result["title"])


def scrape_url(url: str) -> Optional[dict]:
    """Genel HTTP scraper — zingat, hurriyetemlak, vb."""
    html = _fetch_html(url)
    if not html or len(html) < 500:
        return None

    time.sleep(0.5)

    soup = BeautifulSoup(html, "lxml")
    domain = (urlparse(url).hostname or "").replace("www.", "")

    result: dict = {
        "url":           url,
        "domain":        domain,
        "source":        "apify_generic",
        "title":         "",
        "price":         None,
        "currency":      "TL",
        "city":          "",
        "district":      "",
        "neighborhood":  "",
        "rooms":         "",
        "grossM2":       None,
        "netM2":         None,
        "floor":         "",
        "buildingAge":   "",
        "hasElevator":   False,
        "hasParking":    False,
        "furnished":     False,
        "isCreditEligible": False,
        "images":        [],
        "description":   "",
        "publishedDate": "",
        "lat":           "",
        "lon":           "",
    }

    # 1. __NEXT_DATA__ (Next.js — zingat/hurriyetemlak kulllanır)
    nd = _extract_next_data(html)
    if nd:
        _from_next_data(nd, url, result)

    # 2. JSON-LD
    if not result["title"]:
        for jld in _extract_json_ld(html):
            jtype = jld.get("@type", "")
            if jtype in ("Product", "RealEstateListing", "Apartment", "House", "Residence"):
                result["title"] = str(jld.get("name") or "")
                offers = jld.get("offers") or {}
                if offers.get("price"):
                    try:
                        result["price"] = float(str(offers["price"]).replace(".", "").replace(",", "."))
                        result["currency"] = offers.get("priceCurrency", "TL")
                    except (ValueError, TypeError):
                        pass
                result["description"] = str(jld.get("description") or "")[:500]
                break

    # 3. HTML fallback
    if not result["title"]:
        h1 = soup.find("h1")
        if h1:
            result["title"] = h1.get_text(strip=True)[:200]

    if result["price"] is None:
        for sel in ("[class*='price']", "[class*='fiyat']", "[itemprop='price']"):
            el = soup.select_one(sel)
            if el:
                val, cur = _parse_price(el.get_text(strip=True))
                if val:
                    result["price"] = val
                    result["currency"] = cur
                    break

    # OG image fallback
    if not result["images"]:
        og = soup.select_one('meta[property="og:image"]')
        if og and og.get("content"):
            result["images"] = [og["content"]]

    return result if result["title"] else None
