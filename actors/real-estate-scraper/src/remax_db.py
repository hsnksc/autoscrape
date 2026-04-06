"""
remax_db.py — Remax.com.tr scraper için SQLite şema ve yardımcı fonksiyonlar.
"""
from __future__ import annotations

import csv
import sqlite3
import time
from dataclasses import dataclass, fields
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


@dataclass
class RemaxListingUrl:
    """Kategori sayfalarından toplanan ilan URL'leri."""
    url: str
    category: str
    page_found: int = 0
    status: str = "pending"


@dataclass
class RemaxListing:
    """Bir ilanın tam detay verisi."""
    url: str
    category: str
    listing_no: Optional[str] = None
    title: Optional[str] = None
    property_type: Optional[str] = None
    price: Optional[str] = None
    currency: Optional[str] = None
    location: Optional[str] = None
    district: Optional[str] = None
    neighborhood: Optional[str] = None
    m2_brut: Optional[str] = None
    m2_net: Optional[str] = None
    room_count: Optional[str] = None
    floor: Optional[str] = None
    total_floors: Optional[str] = None
    build_year: Optional[str] = None
    heating: Optional[str] = None
    description: Optional[str] = None
    emlak_endeksi: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    scraped_at_utc: str = ""

    def __post_init__(self):
        if not self.scraped_at_utc:
            self.scraped_at_utc = utc_now_iso()


def connect(db_path: Path) -> sqlite3.Connection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.execute("PRAGMA busy_timeout=30000;")
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def _execute_write_with_retry(conn: sqlite3.Connection, fn, retries: int = 5) -> None:
    for attempt in range(retries):
        try:
            fn(conn)
            conn.commit()
            return
        except sqlite3.OperationalError as e:
            msg = str(e).lower()
            if any(kw in msg for kw in ("disk i/o", "database is locked", "readonly database", "cannot commit")):
                time.sleep(0.5 * (attempt + 1))
            else:
                raise


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS remax_listing_urls (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            url         TEXT NOT NULL UNIQUE,
            category    TEXT NOT NULL,
            page_found  INTEGER NOT NULL DEFAULT 0,
            status      TEXT NOT NULL DEFAULT 'pending'
        );
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS remax_listings (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            url             TEXT NOT NULL UNIQUE,
            category        TEXT NOT NULL,
            listing_no      TEXT,
            title           TEXT,
            property_type   TEXT,
            price           TEXT,
            currency        TEXT,
            location        TEXT,
            district        TEXT,
            neighborhood    TEXT,
            m2_brut         TEXT,
            m2_net          TEXT,
            room_count      TEXT,
            floor           TEXT,
            total_floors    TEXT,
            build_year      TEXT,
            heating         TEXT,
            description     TEXT,
            emlak_endeksi   TEXT,
            latitude        REAL,
            longitude       REAL,
            scraped_at_utc  TEXT NOT NULL
        );
    """)
    # Add lat/lng columns if upgrading from older schema
    existing = {row[1] for row in conn.execute("PRAGMA table_info(remax_listings);")}
    if "latitude" not in existing:
        conn.execute("ALTER TABLE remax_listings ADD COLUMN latitude REAL;")
    if "longitude" not in existing:
        conn.execute("ALTER TABLE remax_listings ADD COLUMN longitude REAL;")
    conn.commit()


def bulk_upsert_listing_urls(
    conn: sqlite3.Connection,
    items: list[RemaxListingUrl],
    batch_size: int = 500,
) -> int:
    """Toplu URL ekleme; mevcut olanları atla. Eklenen sayısını döndür."""
    total_added = 0
    for i in range(0, len(items), batch_size):
        batch = items[i : i + batch_size]
        before = conn.total_changes
        conn.executemany(
            """
            INSERT INTO remax_listing_urls (url, category, page_found, status)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(url) DO NOTHING;
            """,
            [(r.url, r.category, r.page_found, r.status) for r in batch],
        )
        conn.commit()
        total_added += conn.total_changes - before
    return total_added


def get_pending_urls(
    conn: sqlite3.Connection,
    categories: Optional[list[str]] = None,
) -> list[tuple[str, str]]:
    """Henüz scrape edilmemiş (status='pending') (url, category) listesi döndür."""
    if categories and len(categories) > 0:
        placeholders = ",".join("?" * len(categories))
        rows = conn.execute(
            f"SELECT url, category FROM remax_listing_urls "
            f"WHERE status='pending' AND category IN ({placeholders}) ORDER BY id;",
            categories,
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT url, category FROM remax_listing_urls WHERE status='pending' ORDER BY id;"
        ).fetchall()
    return [(r[0], r[1]) for r in rows]


def mark_url_status(conn: sqlite3.Connection, url: str, status: str) -> None:
    def _fn(c):
        c.execute("UPDATE remax_listing_urls SET status=? WHERE url=?;", (status, url))
    _execute_write_with_retry(conn, _fn)


def upsert_listing(conn: sqlite3.Connection, listing: RemaxListing) -> None:
    def _fn(c):
        c.execute(
            """
            INSERT INTO remax_listings (
                url, category, listing_no, title, property_type,
                price, currency, location, district, neighborhood,
                m2_brut, m2_net, room_count, floor, total_floors,
                build_year, heating, description, emlak_endeksi,
                latitude, longitude, scraped_at_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(url) DO UPDATE SET
                title=excluded.title,
                property_type=excluded.property_type,
                price=excluded.price,
                currency=excluded.currency,
                location=excluded.location,
                district=excluded.district,
                neighborhood=excluded.neighborhood,
                m2_brut=excluded.m2_brut,
                m2_net=excluded.m2_net,
                room_count=excluded.room_count,
                floor=excluded.floor,
                total_floors=excluded.total_floors,
                build_year=excluded.build_year,
                heating=excluded.heating,
                description=excluded.description,
                emlak_endeksi=excluded.emlak_endeksi,
                latitude=COALESCE(excluded.latitude, remax_listings.latitude),
                longitude=COALESCE(excluded.longitude, remax_listings.longitude),
                scraped_at_utc=excluded.scraped_at_utc;
            """,
            (
                listing.url, listing.category, listing.listing_no, listing.title,
                listing.property_type, listing.price, listing.currency, listing.location,
                listing.district, listing.neighborhood, listing.m2_brut, listing.m2_net,
                listing.room_count, listing.floor, listing.total_floors, listing.build_year,
                listing.heating, listing.description, listing.emlak_endeksi,
                listing.latitude, listing.longitude, listing.scraped_at_utc,
            ),
        )
    _execute_write_with_retry(conn, _fn)


def get_stats(conn: sqlite3.Connection) -> dict[str, int]:
    return {
        "total_urls": conn.execute("SELECT COUNT(*) FROM remax_listing_urls;").fetchone()[0],
        "pending":    conn.execute("SELECT COUNT(*) FROM remax_listing_urls WHERE status='pending';").fetchone()[0],
        "done":       conn.execute("SELECT COUNT(*) FROM remax_listing_urls WHERE status='done';").fetchone()[0],
        "error":      conn.execute("SELECT COUNT(*) FROM remax_listing_urls WHERE status='error';").fetchone()[0],
        "listings":   conn.execute("SELECT COUNT(*) FROM remax_listings;").fetchone()[0],
    }


def export_csv(conn: sqlite3.Connection, csv_path: Path) -> int:
    cols = [row[1] for row in conn.execute("PRAGMA table_info(remax_listings);") if row[1] != "id"]
    rows = conn.execute(f"SELECT {', '.join(cols)} FROM remax_listings ORDER BY id;").fetchall()
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(cols)
        w.writerows(rows)
    return len(rows)
