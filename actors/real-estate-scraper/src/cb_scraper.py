"""
cb_scraper.py — CB.com.tr tüm ilan kategorileri (konut + ticari + devren)

Kaldığı yerden devam eder: checkpoint.json dosyası üzerinden izleme.
Dış bağımlılık yok; sadece stdlib kullanır.
"""
from __future__ import annotations

import csv
import json
import random
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

from .http_utils import fetch_html, clean_text, USER_AGENT
from .checkpoint import load_checkpoint, save_checkpoint
from .models import CanonicalListing, utc_now_iso

BASE_URL = "https://www.cb.com.tr"

CATEGORIES: dict[str, str] = {
    "konut":  "/konut",
    "ticari": "/ticari",
    "devren": "/devren",
}

COLUMNS = list(CanonicalListing.__dataclass_fields__.keys())

# ── HTML parsers ──────────────────────────────────────────────────────────────

def _parse_max_page(html: str) -> int:
    nums = re.findall(r"pager_p=(\d+)", html, re.IGNORECASE)
    return max((int(n) for n in nums), default=1)


def _listing_page_url(category_path: str, page: int) -> str:
    return BASE_URL + category_path + (f"?pager_p={page}" if page > 1 else "")


def _extract_detail_links(html: str, category_path: str) -> list[str]:
    pattern = (
        r"/[a-z0-9çğıöşü\-]+-(?:satilik|kiralik|devren)"
        r"[a-z0-9\-]*/[a-z0-9\-]+/\d+"
    )
    seen: set[str] = set()
    result: list[str] = []
    for href in re.findall(r'href=["\']([^"\']+)["\']', html, re.IGNORECASE):
        if re.search(pattern, href, re.IGNORECASE):
            full = urljoin(BASE_URL, href)
            if full not in seen:
                seen.add(full)
                result.append(full)
    return result


def _parse_feature_table(html: str) -> dict[str, str]:
    rows = re.findall(r"<tr[^>]*>(.*?)</tr>", html, re.IGNORECASE | re.DOTALL)
    result: dict[str, str] = {}
    for row in rows:
        keys = re.findall(r"<b[^>]*>(.*?)</b>", row, re.IGNORECASE | re.DOTALL)
        vals = re.findall(r"<td[^>]*>(.*?)</td>", row, re.IGNORECASE | re.DOTALL)
        if len(keys) == 1 and len(vals) >= 2:
            result[clean_text(keys[0])] = clean_text(vals[1])
    return result


def _parse_coordinates(html: str) -> tuple[Optional[float], Optional[float]]:
    m = re.search(
        r"c\.googleMapOperations\.lat\s*=\s*'([0-9,.-]+)'"
        r"\s*;\s*c\.googleMapOperations\.lng\s*=\s*'([0-9,.-]+)'",
        html, re.IGNORECASE | re.DOTALL,
    )
    if not m:
        return None, None
    try:
        lat = float(m.group(1).replace(",", "."))
        lng = float(m.group(2).replace(",", "."))
        return lat, lng
    except ValueError:
        return None, None


def _parse_title(html: str) -> Optional[str]:
    m = re.search(r"<h1[^>]*>(.*?)</h1>", html, re.IGNORECASE | re.DOTALL)
    return clean_text(m.group(1)) if m else None


def _parse_description(html: str) -> Optional[str]:
    for pat in [
        r"<h2[^>]*>\s*A&Ccedil;IKLAMA\s*</h2>([\s\S]{50,5000}?)<h3[^>]*>",
        r"<h2[^>]*>\s*AÇIKLAMA\s*</h2>([\s\S]{50,5000}?)<h3[^>]*>",
    ]:
        m = re.search(pat, html, re.IGNORECASE)
        if m:
            return clean_text(m.group(1))
    return None


def _parse_location_from_header(html: str) -> Optional[str]:
    m = re.search(
        r"([A-ZÇĞİÖŞÜa-zçğıöşü\s]+/\s*[A-ZÇĞİÖŞÜa-zçğıöşü\s]+"
        r"/\s*[A-ZÇĞİÖŞÜa-zçğıöşü\s]+)",
        html, re.IGNORECASE,
    )
    return clean_text(m.group(1)).strip() if m else None


def parse_detail(url: str, html: str) -> CanonicalListing:
    features = _parse_feature_table(html)
    lat, lng = _parse_coordinates(html)

    def feat(key: str, *aliases: str) -> Optional[str]:
        for k in (key, *aliases):
            v = features.get(k)
            if v:
                return v
        return None

    price_raw = feat("Fiyat")
    price = None
    currency = None
    if price_raw:
        m = re.search(r"([\d\.]+\s*₺)", price_raw, re.IGNORECASE)
        if m:
            price = m.group(1)
            currency = "TRY"

    listing_no = None
    m = re.search(r"Portf[öo]y\s*No\s*:?\s*(\d+)", html, re.IGNORECASE)
    if m:
        listing_no = m.group(1)

    tx_type = feat("İşlem Tipi")
    if tx_type:
        if "satilik" in tx_type.lower() or "satılık" in tx_type.lower():
            tx_type = "Satılık"
        elif "kiralik" in tx_type.lower() or "kiralık" in tx_type.lower():
            tx_type = "Kiralık"

    category_url = url.split("/")
    category = feat("Portföy Kategorisi")

    return CanonicalListing(
        source="CB",
        url=url,
        title=_parse_title(html),
        listing_no=listing_no,
        location=feat("Konum") or _parse_location_from_header(html),
        category=category,
        transaction_type=tx_type,
        price=price,
        currency=currency,
        m2_brut=feat("Metre Kare (Brüt)"),
        m2_net=feat("Metre Kare (Net)"),
        room_count=feat("Oda Sayısı"),
        building_age=feat("Bina Yaşı"),
        floor=feat("Bulunduğu Kat"),
        total_floors=feat("Kat Sayısı"),
        heating=feat("Isıtma"),
        description=_parse_description(html),
        latitude=lat,
        longitude=lng,
        scraped_at_utc=utc_now_iso(),
        raw_json=json.dumps(features, ensure_ascii=False),
    )


# ── Checkpoint-based scrape ───────────────────────────────────────────────────

def scrape_all(
    csv_path: Path,
    checkpoint_path: Optional[Path] = None,
    max_pages: int = 0,
    delay: float = 1.5,
    concurrency: int = 1,
    on_item=None,
) -> list[CanonicalListing]:
    """
    Tüm kategorileri tara. Kaldığı yerden devam eder.
    on_item: her ilan scraplanınca çağrılır (CanonicalListing) → None
    """
    if checkpoint_path is None:
        checkpoint_path = csv_path.with_suffix(".checkpoint.json")

    cp = load_checkpoint(checkpoint_path)
    collected: dict[str, list[str]] = {k: list(v) for k, v in cp.get("collected", {}).items()}
    done_urls: set[str] = set(cp.get("done_urls", []))

    results: list[CanonicalListing] = []

    # ── Phase 1: URL toplama ──
    for cat_name, cat_path in CATEGORIES.items():
        if cat_name in collected and collected[cat_name]:
            print(f"[CB:{cat_name}] {len(collected[cat_name])} URL checkpoint'ten yüklendi")
            continue

        if cat_name == "devren":
            # Devren sayfası bazen kaldırılmış olabilir
            try:
                html = fetch_html(_listing_page_url(cat_path, 1))
            except Exception:
                print(f"[CB:{cat_name}] Devren liste rotası mevcut değil; atlanıyor")
                collected[cat_name] = []
                save_checkpoint(checkpoint_path, {"collected": collected, "done_urls": list(done_urls)})
                continue

        urls: list[str] = []
        try:
            html = fetch_html(_listing_page_url(cat_path, 1))
        except Exception as exc:
            print(f"[CB:{cat_name}] Liste sayfası alınamadı: {exc}")
            collected[cat_name] = []
            save_checkpoint(checkpoint_path, {"collected": collected, "done_urls": list(done_urls)})
            continue

        total_pages = _parse_max_page(html)
        if max_pages > 0:
            total_pages = min(total_pages, max_pages)

        print(f"[CB:{cat_name}] Toplam sayfa: {total_pages}")
        seen: set[str] = set()
        # Sayfa 1 zaten indirildi
        new_p1 = [u for u in _extract_detail_links(html, cat_path) if u not in seen]
        seen.update(new_p1)
        urls.extend(new_p1)
        print(f"[CB:{cat_name}] Sayfa 1/{total_pages}: {len(new_p1)} yeni URL")

        if total_pages > 1:
            def _fetch_cb_page(page_num: int, _cp=cat_path, _d=delay) -> tuple[int, list[str]]:
                pg_html = fetch_html(_listing_page_url(_cp, page_num))
                time.sleep(random.uniform(_d * 0.3, _d * 0.7))
                return page_num, _extract_detail_links(pg_html, _cp)

            with ThreadPoolExecutor(max_workers=4) as page_pool:
                page_futs = {page_pool.submit(_fetch_cb_page, p): p for p in range(2, total_pages + 1)}
                for fut in as_completed(page_futs):
                    p_num = page_futs[fut]
                    try:
                        _, page_urls = fut.result()
                        new = [u for u in page_urls if u not in seen]
                        seen.update(new)
                        urls.extend(new)
                        print(f"[CB:{cat_name}] Sayfa {p_num}/{total_pages}: {len(new)} yeni URL")
                    except Exception as exc:
                        print(f"[CB:{cat_name}] Sayfa {p_num} alınamadı: {exc}")

        print(f"[CB:{cat_name}] Toplam: {len(urls)} benzersiz URL")
        collected[cat_name] = urls
        save_checkpoint(checkpoint_path, {"collected": collected, "done_urls": list(done_urls)})

    all_urls = [u for urls in collected.values() for u in urls]
    remaining = [u for u in all_urls if u not in done_urls]
    print(f"[CB] Toplam: {len(all_urls)} URL | Tamamlanan: {len(done_urls)} | Kalan: {len(remaining)}")

    # ── Phase 2: Detay scraping (ThreadPoolExecutor ile eşzamanlı) ──
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not csv_path.exists() or csv_path.stat().st_size == 0

    def _fetch_one(url: str) -> CanonicalListing:
        html = fetch_html(url)
        time.sleep(random.uniform(delay * 0.5, delay * 1.5))
        return parse_detail(url, html)

    total      = len(remaining)
    done_count = 0
    abort_exc  = None

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        all_futures = {executor.submit(_fetch_one, u): u for u in remaining}
        for future in as_completed(all_futures):
            url = all_futures[future]
            done_count += 1
            try:
                listing = future.result()
            except Exception as exc:
                print(f"  [CB] [{done_count}/{total}] HATA {url} -> {exc}")
                continue
            results.append(listing)
            with open(csv_path, "a", newline="", encoding="utf-8-sig", errors="ignore") as f:
                w = csv.DictWriter(f, fieldnames=COLUMNS, extrasaction="ignore")
                if write_header:
                    w.writeheader()
                    write_header = False
                w.writerow(listing.to_dict())
            try:
                if on_item:
                    on_item(listing)
            except BaseException as exc:
                done_urls.add(url)
                save_checkpoint(checkpoint_path, {"collected": collected, "done_urls": list(done_urls)})
                abort_exc = exc
                for f in all_futures:
                    f.cancel()
                break
            done_urls.add(url)
            print(f"  [CB] [{done_count}/{total}] OK {url}")
            save_checkpoint(checkpoint_path, {"collected": collected, "done_urls": list(done_urls)})

    if abort_exc is not None:
        raise abort_exc

    return results


def main():
    import argparse
    p = argparse.ArgumentParser(description="CB.com.tr tüm kategoriler scraper")
    p.add_argument("--max-pages", type=int, default=0)
    p.add_argument("--delay", type=float, default=1.5)
    p.add_argument("--csv", default="data/cb_latest.csv")
    args = p.parse_args()
    result = scrape_all(
        csv_path=Path(args.csv),
        max_pages=args.max_pages,
        delay=args.delay,
    )
    print(f"\n[CB] Tamamlandı. Toplam: {len(result)} ilan")


if __name__ == "__main__":
    main()
