"""
shb_detail.py — SHB Portal ağı (CB.com.tr, Century21.com.tr, ERA.com.tr) detay scraper.

Tüm üç site aynı HTML motorunu kullanır:
  - <h1> başlık
  - <tr><b>key</b></tr><td>val</td> özellik tablosu
  - googleMapOperations.lat / .lng koordinatlar

Döndürdüğü dict normalize.py > to_autoscrape_schema() ile uyumludur.
"""
from __future__ import annotations

import re
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
    "Connection": "keep-alive",
}


def _source_from_domain(url: str) -> str:
    host = (urlparse(url).hostname or "").replace("www.", "").lower()
    if "cb.com.tr" in host:
        return "apify_cb"
    if "century21.com.tr" in host:
        return "apify_century21"
    if "era.com.tr" in host:
        return "apify_era"
    return "apify_shb"


def _fetch_html(url: str, timeout: int = 20) -> str:
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=timeout)
        resp.raise_for_status()
        return resp.text
    except Exception as exc:
        print(f"[SHB] Fetch hatası ({url[:70]}): {exc}")
        return ""


def _clean(text: str) -> str:
    """HTML tag ve fazla boşlukları temizle."""
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", text or "")).strip()


def _parse_feature_table(html: str) -> dict[str, str]:
    """
    <tr><td><b>Key</b></td><td>Val</td></tr> yapısından özellik dict'i çıkar.
    """
    rows = re.findall(r"<tr[^>]*>(.*?)</tr>", html, re.IGNORECASE | re.DOTALL)
    result: dict[str, str] = {}
    for row in rows:
        keys = re.findall(r"<b[^>]*>(.*?)</b>", row, re.IGNORECASE | re.DOTALL)
        vals = re.findall(r"<td[^>]*>(.*?)</td>", row, re.IGNORECASE | re.DOTALL)
        if len(keys) == 1 and len(vals) >= 2:
            k = _clean(keys[0])
            v = _clean(vals[1]) if len(vals) > 1 else _clean(vals[0])
            if k and v:
                result[k] = v
    return result


def _parse_price(text: str) -> Optional[float]:
    """'14.200.000 ₺' veya '14.200.000 TL' → 14200000.0"""
    if not text:
        return None
    m = re.search(r"([\d]{1,3}(?:\.[\d]{3})+|[\d]+)", text)
    if m:
        val_str = m.group(1).replace(".", "")
        try:
            val = float(val_str)
            if val >= 1000:
                return val
        except ValueError:
            pass
    return None


def _parse_coordinates(html: str) -> tuple[Optional[str], Optional[str]]:
    """googleMapOperations.lat / .lng → ('lat', 'lng') string'leri."""
    m = re.search(
        r"googleMapOperations\.lat\s*=\s*'([0-9,.\-]+)'"
        r"[\s\S]{0,200}?"
        r"googleMapOperations\.lng\s*=\s*'([0-9,.\-]+)'",
        html,
        re.IGNORECASE,
    )
    if m:
        return m.group(1).replace(",", "."), m.group(2).replace(",", ".")
    return None, None


def _parse_location(konum: str) -> tuple[str, str, str]:
    """
    'Türkiye , İstanbul , Kadıköy , Kozyatağı' → ('İstanbul', 'Kadıköy', 'Kozyatağı')
    HTML entity'leri decode edilmiş olan metin beklenir.
    """
    parts = [p.strip() for p in re.split(r"[,\n\r]+", konum) if p.strip()]
    # 'Türkiye' / 'Turkiye' prefix'ini atla
    if parts and parts[0].lower().replace("ü", "u") in ("turkiye", "turkey"):
        parts = parts[1:]
    city         = parts[0] if len(parts) > 0 else ""
    district     = parts[1] if len(parts) > 1 else ""
    neighborhood = parts[2] if len(parts) > 2 else ""
    return city, district, neighborhood


def _parse_images(soup: BeautifulSoup) -> list[str]:
    """Sayfa içindeki ilan fotoğraflarını topla."""
    imgs = []
    for img in soup.find_all("img", src=True):
        src = img["src"]
        if any(x in src.lower() for x in ("icon", "logo", "assets", "favicon", "banner")):
            continue
        if re.search(r"\.(jpg|jpeg|png|webp)", src, re.I):
            imgs.append(src)
    return imgs[:15]


def scrape_url(url: str) -> Optional[dict]:
    """SHB Portal (CB/Century21/ERA) ilan detay sayfasını scrape et."""
    html = _fetch_html(url)
    if not html or len(html) < 500:
        return None

    soup = BeautifulSoup(html, "lxml")
    features = _parse_feature_table(html)

    def feat(*keys: str) -> str:
        for k in keys:
            v = features.get(k)
            if v:
                return v
        return ""

    # ── Başlık ──
    h1 = soup.find("h1")
    title = _clean(str(h1)) if h1 else ""

    # ── Fiyat ──
    price = _parse_price(feat("Fiyat"))

    # ── Konum ──
    konum_raw = feat("Konum")
    city, district, neighborhood = _parse_location(konum_raw)

    # ── Koordinat ──
    lat, lon = _parse_coordinates(html)

    # ── Açıklama ──
    desc = ""
    for pat in [
        r"<h2[^>]*>\s*A[Ç&][A-Z;a-z]*LAMA\s*</h2>([\s\S]{30,4000}?)<h[23]",
        r"<h2[^>]*>\s*AÇIKLAMA\s*</h2>([\s\S]{30,4000}?)<h[23]",
        r"<section[^>]+description[^>]*>([\s\S]{30,2000}?)</section>",
    ]:
        m = re.search(pat, html, re.IGNORECASE)
        if m:
            desc = _clean(m.group(1))[:500]
            break

    # ── Görsel ──
    images = _parse_images(soup)

    # ── Bina özellikleri ──
    has_elevator = any(
        x in html.lower() for x in ("asansör", "asansor", "lift", "elevator")
    )
    has_parking = any(
        x in html.lower() for x in ("otopark", "garaj", "parking", "açık otopark", "kapalı otopark")
    )

    return {
        "url":          url,
        "domain":       (urlparse(url).hostname or "").replace("www.", ""),
        "source":       _source_from_domain(url),
        "title":        title,
        "price":        price,
        "currency":     "TRY",
        "city":         city,
        "district":     district,
        "neighborhood": neighborhood,
        "rooms":        feat("Oda Sayısı"),
        "netM2":        feat("Metre Kare (Net)", "Metre Kare"),
        "grossM2":      feat("Metre Kare (Brüt)", "Metre Kare (Brut)"),
        "floor":        feat("Bulunduğu Kat", "Buldugu Kat", "Bulundugu Kat"),
        "buildingAge":  feat("Bina Yaşı", "Bina Yasi", "Bina Yas\u0131"),
        "hasElevator":  has_elevator,
        "hasParking":   has_parking,
        "description":  desc,
        "images":       images,
        "lat":          lat or "",
        "lon":          lon or "",
    }
