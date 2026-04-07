"""
remax_detail.py — Remax.com.tr tekil ilan URL scraper.

Remax Next.js RSC (React Server Components) kullanır. Ilan verisi
``self.__next_f.push([1, "..."])`` bloklarına JSON-escape edilmiş halde gömülür.
``propertyDetailData.data`` objesinden tüm alanlar çekilir.
"""
from __future__ import annotations

import json
import re
from typing import Optional

import requests

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "tr-TR,tr;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Referer": "https://www.remax.com.tr/",
}


def _fetch_html(url: str, proxy_url: Optional[str] = None, timeout: int = 20) -> str:
    try:
        proxies = {"http": proxy_url, "https": proxy_url} if proxy_url else None
        resp = requests.get(url, headers=_HEADERS, proxies=proxies, timeout=timeout, allow_redirects=True)
        if resp.status_code in (403, 429, 503):
            return ""
        return resp.text
    except Exception as exc:
        print(f"[REMAX] Fetch hatası ({url[:70]}): {exc}")
        return ""


def _extract_property_data(html: str) -> Optional[dict]:
    """
    self.__next_f.push([1, "..."]) bloklarını tarar,
    propertyDetailData.data objesini bulur ve parse eder.
    """
    # Tüm push bloklarını al
    push_blocks = re.findall(
        r'self\.__next_f\.push\(\[1,"([\s\S]*?)"\]\)\s*;?\s*(?=<|\Z)',
        html,
    )

    for raw in push_blocks:
        if "propertyDetailData" not in raw:
            continue
        try:
            decoded = json.loads('"' + raw + '"')
        except Exception:
            continue

        # propertyDetailData:{data:{...}} bloğunu bul
        idx = decoded.find('"propertyDetailData"')
        if idx < 0:
            continue

        # Açılan { sayısını sayarak data objesini kes
        start = decoded.find('{"data":{', idx)
        if start < 0:
            continue
        # İlk "data" objesini bul
        data_start = decoded.find('"data":{', idx) + len('"data":')
        brace_start = decoded.find('{', data_start)
        if brace_start < 0:
            continue

        depth = 0
        end = brace_start
        for i, ch in enumerate(decoded[brace_start:], brace_start):
            if ch == '{':
                depth += 1
            elif ch == '}':
                depth -= 1
                if depth == 0:
                    end = i + 1
                    break

        try:
            data = json.loads(decoded[brace_start:end])
            return data
        except Exception:
            continue

    return None


def _parse_price(raw_price: str) -> Optional[float]:
    """'16,000,000' veya '16.000.000' → 16000000.0"""
    if not raw_price:
        return None
    cleaned = re.sub(r"[^\d]", "", str(raw_price))
    return float(cleaned) if cleaned else None


def _parse_rooms(room_str: str) -> str:
    """'3+1' veya '3 Oda 1 Salon' → '3+1'"""
    m = re.search(r"(\d+)\s*\+\s*(\d+)", str(room_str))
    return f"{m.group(1)}+{m.group(2)}" if m else ""


def _extract_images(data: dict) -> list[str]:
    """images listesinden URL'leri çıkar."""
    images = data.get("images") or data.get("photos") or []
    urls = []
    for p in images:
        if isinstance(p, str) and p.startswith("http"):
            urls.append(p)
        elif isinstance(p, dict):
            url = p.get("largeUrl") or p.get("url") or p.get("src") or ""
            if url and url.startswith("http"):
                urls.append(url)
    return urls


def _normalize_remax_url(url: str) -> str:
    """
    Exa bazen agent sayfası URL'i döndürür:
      /AgentName/Detail?propertyCode=P76755508
    Bu durumda URL'i standart detay URL'ine dönüştür:
      /portfoy/P76755508
    Arama sayfaları (/emlak/... veya /satilik/...) → boş string döndür.
    """
    from urllib.parse import urlparse, parse_qs
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)

    # propertyCode parametresi varsa (agent detail sayfası)
    if "propertyCode" in qs:
        code = qs["propertyCode"][0]  # örn: "P76755508"
        return f"https://www.remax.com.tr/portfoy/{code}"

    # /portfoy/P... formatı zaten doğru
    if "/portfoy/" in parsed.path:
        return url

    # Arama/liste sayfası — scrape etme
    if any(seg in parsed.path for seg in ("/emlak/", "/satilik", "/kiralik")):
        return ""

    return url


def scrape_url(url: str, proxy_url: Optional[str] = None) -> Optional[dict]:
    url = _normalize_remax_url(url)
    if not url:
        return None

    html = _fetch_html(url, proxy_url=proxy_url)
    if not html:
        return None

    data = _extract_property_data(html)
    if not data:
        print(f"[REMAX] propertyDetailData bulunamadı: {url[:70]}")
        return None

    attrs = data.get("headerAttributes") or {}
    other = data.get("otherAttributes") or {}
    ic = other.get("İç Özellikler") or {}
    dis = other.get("Dış Özellikler") or {}

    # Şehir normalizasyonu: "İstanbul Anadolu" → "İstanbul"
    city_raw = data.get("cityName") or ""
    city = city_raw.split()[0] if city_raw else ""

    # Kat bilgisi
    floor_raw = attrs.get("Bulunduğu Kat") or ""
    floor_str = str(floor_raw) if floor_raw else ""

    # Bina yaşı: "Bina Yapım Yılı" varsa yaş hesapla, yoksa doğrudan kullan
    bina_yili = attrs.get("Bina Yapım Yılı") or ""
    building_age = ""
    if bina_yili:
        try:
            import datetime
            age = datetime.date.today().year - int(bina_yili)
            building_age = str(max(0, age))
        except Exception:
            building_age = str(bina_yili)

    return {
        "url": url,
        "source": "apify_remax",
        "domain": "remax.com.tr",
        "title": data.get("title") or "",
        "price": _parse_price(data.get("price")),
        "currency": "TRY",
        "city": city,
        "district": data.get("townName") or "",
        "neighborhood": data.get("neighborhoodName") or "",
        "rooms": _parse_rooms(attrs.get("Oda Sayısı") or ""),
        "netM2": int(attrs["m2 (Net)"]) if attrs.get("m2 (Net)") and str(attrs["m2 (Net)"]).isdigit() else None,
        "grossM2": int(attrs["m2 (Brüt)"]) if attrs.get("m2 (Brüt)") and str(attrs["m2 (Brüt)"]).isdigit() else None,
        "floor": floor_str,
        "buildingAge": building_age,
        "hasElevator": bool(ic.get("Asansör") or dis.get("Asansör")),
        "hasParking": bool(dis.get("Otopark") or dis.get("Kapalı Garaj")),
        "isCreditEligible": str(attrs.get("Krediye Uygun") or "").lower() in ("evet", "true", "yes"),
        "furnished": bool(ic.get("Eşyalı")),
        "description": re.sub(r"<[^>]+>", " ", data.get("description") or "").strip(),
        "images": _extract_images(data),
        "publishedDate": data.get("date") or "",
        "lat": data.get("latitude"),
        "lon": data.get("longitude"),
    }
