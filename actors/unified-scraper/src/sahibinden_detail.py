"""
sahibinden_detail.py — Sahibinden.com tekil ilan URL scraper.

requests + BeautifulSoup HTML parse — Playwright/Camoufox yok, hizli.
Not: Sahibinden.com bot-korumasina sahiptir; cookies verilmezse 403 donebilir.
"""
from __future__ import annotations

import json
import re
from typing import Optional

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
    "Referer": "https://www.sahibinden.com/",
}


def _fetch_html(url: str, cookies: list, timeout: int = 15) -> str:
    try:
        session = requests.Session()
        session.headers.update(_HEADERS)
        if cookies:
            for ck in cookies:
                if isinstance(ck, dict) and ck.get("name"):
                    session.cookies.set(ck["name"], ck.get("value", ""))
        resp = session.get(url, timeout=timeout, allow_redirects=True)
        if resp.status_code in (403, 429, 503):
            print(f"[SAHIBINDEN] HTTP {resp.status_code} — bot korumasi")
            return ""
        return resp.text
    except Exception as exc:
        print(f"[SAHIBINDEN] Fetch hatasi ({url[:70]}): {exc}")
        return ""


def _parse_detail(soup: BeautifulSoup, url: str) -> dict:
    result: dict = {
        "url": url, "domain": "sahibinden.com", "source": "apify_sahibinden",
        "title": "", "price": None, "currency": "TL",
        "city": "", "district": "", "neighborhood": "",
        "rooms": "", "grossM2": None, "netM2": None,
        "floor": "", "buildingAge": "",
        "hasElevator": False, "hasParking": False,
        "furnished": False, "isCreditEligible": False,
        "images": [], "description": "", "publishedDate": "",
    }

    # Baslik
    for sel in ("h1.classifiedDetailTitle", "h1.classifiedInfoTitle", "h1"):
        el = soup.select_one(sel)
        if el and el.get_text(strip=True):
            result["title"] = el.get_text(strip=True)[:200]
            break

    # Fiyat
    for sel in (".classifiedPrice h3", ".priceValue", ".artPrice"):
        el = soup.select_one(sel)
        if el:
            txt = el.get_text(strip=True)
            if "USD" in txt or "$" in txt:
                result["currency"] = "USD"
            elif "EUR" in txt or "EUR" in txt:
                result["currency"] = "EUR"
            nums = re.findall(r"\d+", txt.replace(".", "").replace(",", ""))
            for n in nums:
                try:
                    v = float(n)
                    if v > 100:
                        result["price"] = v
                        break
                except ValueError:
                    pass
            if result["price"]:
                break

    # Konum breadcrumb
    crumbs = [el.get_text(strip=True) for el in soup.select(".classifiedInfoBreadCrumb li a, .breadcrumb li a")]
    crumbs = [t for t in crumbs if t.lower() not in ("anasayfa", "ilanlar", "turkiye", "türkiye")]
    if len(crumbs) >= 2:
        result["city"] = crumbs[0]
        result["district"] = crumbs[1]
        if len(crumbs) >= 3:
            result["neighborhood"] = crumbs[2]

    # Ozellikler tablosu
    for li in soup.select(".classifiedInfoList li, .classifiedInfoListItem"):
        txt = li.get_text(strip=True).lower()
        if "asans" in txt:
            result["hasElevator"] = True
        if "otopark" in txt or "garaj" in txt:
            result["hasParking"] = True
        if "krediye uygun" in txt:
            result["isCreditEligible"] = True
        if "eşyalı" in txt or "esyali" in txt:
            result["furnished"] = True
        m2m = re.search(r"brüt\s*:?\s*(\d+)", txt)
        if m2m:
            result["grossM2"] = int(m2m.group(1))
        m2m = re.search(r"net\s*:?\s*(\d+)", txt)
        if m2m:
            result["netM2"] = int(m2m.group(1))
        rm = re.search(r"(\d+\+\d+)", txt)
        if rm and not result["rooms"]:
            result["rooms"] = rm.group(1)
        fl = re.search(r"bulunduğu kat\s*:?\s*(.+)", txt)
        if fl and not result["floor"]:
            result["floor"] = fl.group(1).strip()[:30]
        age = re.search(r"bina yaşı\s*:?\s*(.+)", txt)
        if age and not result["buildingAge"]:
            result["buildingAge"] = age.group(1).strip()[:20]

    # Gorseller
    images = []
    for img in soup.select(".classifiedDetailPhotos img, #thumbListUl img, .gallery img"):
        src = img.get("src") or img.get("data-src") or ""
        if src and "http" in src and src not in images:
            images.append(src)
    if not images:
        og = soup.select_one('meta[property="og:image"]')
        if og:
            images = [og.get("content", "")]
    result["images"] = [i for i in images if i][:15]

    # Aciklama
    desc_el = soup.select_one(".classifiedDescription, #ilanAciklamasi")
    if desc_el:
        result["description"] = desc_el.get_text(strip=True)[:500]

    return result


def scrape_url(url: str, cookies: Optional[list] = None) -> Optional[dict]:
    """Sahibinden.com ilan detay URL'sini scrape et. dict veya None doner."""
    html = _fetch_html(url, cookies or [])
    if not html or len(html) < 500:
        return None

    soup = BeautifulSoup(html, "lxml")

    # Bot engeli kontrolu
    if soup.select_one("title") and "403" in (soup.title.get_text() if soup.title else ""):
        print(f"[SAHIBINDEN] 403 engeli: {url[:70]}")
        return None

    result = _parse_detail(soup, url)
    if not result.get("title"):
        return None
    return result
