"""
normalize.py — Her scraperın raw dict çıktısını AutoScrape ortak şemasına dönüştürür.

AutoScrape schema:
  url, domain, title, price, currency, city, district, rooms,
  netM2, grossM2, floor, buildingAge, isCreditEligible, hasElevator,
  hasParking, furnished, description, images[], publishedDate,
  score, highlights, summary, source, lat, lon
"""
from __future__ import annotations

import re
from typing import Optional
from urllib.parse import urlparse


def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val) if float(val) > 0 else None
    try:
        # "4.500.000" veya "4,500,000" → 4500000
        s = str(val).strip()
        # Nokta binlik ayırıcı mı yoksa ondalık mı? Heuristic: son 3 karakter
        if re.match(r"^\d{1,3}(\.\d{3})+$", s):
            s = s.replace(".", "")
        else:
            s = s.replace(".", "").replace(",", ".")
        return float(s) if s else None
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> Optional[int]:
    f = _safe_float(val)
    return int(f) if f is not None else None


def _bool_field(raw: dict, keys: list[str], default: bool = False) -> bool:
    for k in keys:
        v = raw.get(k)
        if v is None:
            continue
        if isinstance(v, bool):
            return v
        s = str(v).lower().strip()
        if s in ("true", "evet", "1", "yes", "var", "uygun"):
            return True
        if s in ("false", "hayır", "hayir", "0", "no", "yok", "uygun değil"):
            return False
    return default


def _rooms_normalize(raw: dict) -> str:
    """["3", "1"] / "3+1" / "3,1" → "3+1" """
    v = raw.get("rooms") or raw.get("room_count") or raw.get("roomCount") or ""
    if isinstance(v, (list, tuple)):
        return "+".join(str(x) for x in v)
    return str(v).replace(",", "+").strip()


def _domain_from_url(url: str) -> str:
    try:
        return (urlparse(url).hostname or "").replace("www.", "")
    except Exception:
        return ""


def to_autoscrape_schema(raw: dict, url: str) -> dict:
    """Ham scraper dict → AutoScrape normalize edilmiş ilan nesnesi."""

    images = raw.get("images") or []
    if isinstance(images, str):
        images = [images] if images else []

    # currency normalize: TL / TRY → TRY
    currency = str(raw.get("currency") or "TRY").strip().upper()
    if currency == "TL" or currency == "₺":
        currency = "TRY"

    return {
        # ── Kimlik ──
        "url":    url,
        "domain": raw.get("domain") or _domain_from_url(url),

        # ── İlan metni ──
        "title":  str(raw.get("title") or "").strip()[:200],

        # ── Fiyat ──
        "price":    _safe_float(raw.get("price")),
        "currency": currency,

        # ── Konum ──
        "city":         str(raw.get("city")         or raw.get("province")      or "").strip(),
        "district":     str(raw.get("district")     or raw.get("county")        or "").strip(),
        "neighborhood": str(raw.get("neighborhood") or raw.get("mahalle")       or "").strip(),

        # ── Emlak özellikleri ──
        "rooms":       _rooms_normalize(raw),
        "netM2":       _safe_int(raw.get("netM2")       or raw.get("net_sqm")    or raw.get("netSqm")),
        "grossM2":     _safe_int(raw.get("grossM2")     or raw.get("gross_sqm")  or raw.get("grossSqm")),
        "floor":       str(raw.get("floor")       or raw.get("floor_count") or "").strip(),
        "buildingAge": str(raw.get("buildingAge") or raw.get("building_age") or raw.get("age") or "").strip(),

        # ── Boolean özellikler ──
        "isCreditEligible": _bool_field(raw, ["isCreditEligible", "credit_status", "creditStatus"]),
        "hasElevator":      _bool_field(raw, ["hasElevator", "has_elevator"]),
        "hasParking":       _bool_field(raw, ["hasParking",  "has_parking",  "otopark"]),
        "furnished":        _bool_field(raw, ["furnished",   "esyali"]),

        # ── Medya / Metin ──
        "description":   str(raw.get("description") or "")[:500].strip(),
        "images":        [str(i) for i in images if i][:10],
        "publishedDate": str(
            raw.get("publishedDate") or raw.get("created_at")
            or raw.get("listing_date") or raw.get("createDate") or ""
        ),

        # ── Exa ile gelen ek alanlar (Apify'dan gelince boş) ──
        "score":      float(raw.get("score") or 0),
        "highlights": list(raw.get("highlights") or []),
        "summary":    str(raw.get("summary") or ""),

        # ── Kaynak ──
        "source": str(raw.get("source") or "apify"),

        # ── Koordinat ──
        "lat": str(raw.get("lat") or raw.get("latitude")  or ""),
        "lon": str(raw.get("lon") or raw.get("longitude") or ""),
    }
