"""
Ortak veri modeli - tüm scraper'lar bu dataclass'ı kullanır.
"""
from __future__ import annotations

from dataclasses import dataclass, field, fields, asdict
from datetime import datetime, timezone
from typing import Optional
import json


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def to_float(v) -> Optional[float]:
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        try:
            return float(v.strip().replace(",", "."))
        except ValueError:
            return None
    return None


@dataclass
class CanonicalListing:
    source: str
    url: str
    title: Optional[str] = None
    listing_no: Optional[str] = None
    product_id: Optional[str] = None
    category: Optional[str] = None
    transaction_type: Optional[str] = None
    property_type: Optional[str] = None
    price: Optional[str] = None
    currency: Optional[str] = None
    location: Optional[str] = None
    district: Optional[str] = None
    neighborhood: Optional[str] = None
    m2: Optional[str] = None
    m2_brut: Optional[str] = None
    m2_net: Optional[str] = None
    room_count: Optional[str] = None
    floor: Optional[str] = None
    total_floors: Optional[str] = None
    building_age: Optional[str] = None
    build_year: Optional[str] = None
    heating: Optional[str] = None
    description: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    scraped_at_utc: Optional[str] = None
    raw_json: Optional[str] = None

    def to_dict(self) -> dict:
        return asdict(self)


COLUMNS = [f.name for f in fields(CanonicalListing)]
