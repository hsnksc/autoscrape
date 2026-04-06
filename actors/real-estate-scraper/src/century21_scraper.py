"""
century21_scraper.py — Century21.com.tr tüm ilan kategorileri (konut + ticari + devren)

Kaldığı yerden devam eder. Dış bağımlılık yok; sadece stdlib kullanır.
CB scraper ile aynı yapıya sahip (aynı site motoru).
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

from .http_utils import fetch_html, clean_text
from .checkpoint import load_checkpoint, save_checkpoint
from .models import CanonicalListing, utc_now_iso

BASE_URL = "https://www.century21.com.tr"

CATEGORIES: dict[str, str] = {
    "konut":  "/konut",
    "ticari": "/ticari",
    "devren": "/devren",
}

COLUMNS = list(CanonicalListing.__dataclass_fields__.keys())


def _parse_max_page(html: str) -> int:
    nums = re.findall(r"pager_p=(\d+)", html, re.IGNORECASE)
    return max((int(n) for n in nums), default=1)


def _listing_page_url(category_path: str, page: int) -> str:
    return BASE_URL + category_path + (f"?pager_p={page}" if page > 1 else "")


def _extract_detail_links(html: str) -> list[str]:
    pattern = (
        r"/[a-z0-9\u00e7\u011f\u0131\u00f6\u015f\u00fc\-]+"
        r"-(?:satilik|kiralik|devren)[a-z0-9\-]*/[a-z0-9\-]+/\d+"
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
        return float(m.group(1).replace(",", ".")), float(m.group(2).replace(",", "."))
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


def parse_detail(url: str, html: str) -> CanonicalListing:
    features = _parse_feature_table(html)
    lat, lng = _parse_coordinates(html)

    def feat(*keys: str) -> Optional[str]:
        for k in keys:
            v = features.get(k)
            if v:
                return v
        return None

    price_raw = feat("Fiyat")
    price, currency = None, None
    if price_raw:
        m = re.search(r"([\d\.]+\s*₺)", price_raw)
        if m:
            price, currency = m.group(1), "TRY"

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

    return CanonicalListing(
        source="Century21",
        url=url,
        title=_parse_title(html),
        listing_no=listing_no,
        location=feat("Konum"),
        category=feat("Portföy Kategorisi"),
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


def scrape_all(
    csv_path: Path,
    checkpoint_path: Optional[Path] = None,
    max_pages: int = 0,
    delay: float = 1.5,
    concurrency: int = 1,
    on_item=None,
) -> list[CanonicalListing]:
    if checkpoint_path is None:
        checkpoint_path = csv_path.with_suffix(".checkpoint.json")

    cp = load_checkpoint(checkpoint_path)
    collected: dict[str, list[str]] = {k: list(v) for k, v in cp.get("collected", {}).items()}
    done_urls: set[str] = set(cp.get("done_urls", []))
    results: list[CanonicalListing] = []

    for cat_name, cat_path in CATEGORIES.items():
        if cat_name in collected and collected[cat_name]:
            print(f"[C21:{cat_name}] {len(collected[cat_name])} URL checkpoint'ten yüklendi")
            continue
        urls: list[str] = []
        try:
            html = fetch_html(_listing_page_url(cat_path, 1))
        except Exception as exc:
            print(f"[C21:{cat_name}] Liste alınamadı: {exc}")
            collected[cat_name] = []
            save_checkpoint(checkpoint_path, {"collected": collected, "done_urls": list(done_urls)})
            continue

        if cat_name == "devren" and "hata" in html.lower():
            print(f"[C21:{cat_name}] Devren rotası yok; atlanıyor")
            collected[cat_name] = []
            save_checkpoint(checkpoint_path, {"collected": collected, "done_urls": list(done_urls)})
            continue

        total_pages = _parse_max_page(html)
        if max_pages > 0:
            total_pages = min(total_pages, max_pages)
        print(f"[C21:{cat_name}] Toplam sayfa: {total_pages}")
        seen: set[str] = set()
        # Sayfa 1 zaten indirildi
        new_p1 = [u for u in _extract_detail_links(html) if u not in seen]
        seen.update(new_p1)
        urls.extend(new_p1)

        if total_pages > 1:
            def _fetch_c21_page(page_num: int, _cp=cat_path, _d=delay) -> tuple[int, list[str]]:
                pg_html = fetch_html(_listing_page_url(_cp, page_num))
                time.sleep(random.uniform(_d * 0.3, _d * 0.7))
                return page_num, _extract_detail_links(pg_html)

            with ThreadPoolExecutor(max_workers=4) as page_pool:
                page_futs = {page_pool.submit(_fetch_c21_page, p): p for p in range(2, total_pages + 1)}
                for fut in as_completed(page_futs):
                    p_num = page_futs[fut]
                    try:
                        _, page_urls = fut.result()
                        new = [u for u in page_urls if u not in seen]
                        seen.update(new)
                        urls.extend(new)
                    except Exception as exc:
                        print(f"[C21:{cat_name}] Sayfa {p_num} hatası: {exc}")

        collected[cat_name] = urls
        save_checkpoint(checkpoint_path, {"collected": collected, "done_urls": list(done_urls)})

    all_urls = [u for urls in collected.values() for u in urls]
    remaining = [u for u in all_urls if u not in done_urls]
    print(f"[C21] Toplam: {len(all_urls)} | Tamamlanan: {len(done_urls)} | Kalan: {len(remaining)}")

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
                print(f"  [C21] [{done_count}/{total}] HATA {url} -> {exc}")
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
            print(f"  [C21] [{done_count}/{total}] OK {url}")
            save_checkpoint(checkpoint_path, {"collected": collected, "done_urls": list(done_urls)})

    if abort_exc is not None:
        raise abort_exc

    return results


def main():
    import argparse
    p = argparse.ArgumentParser(description="Century21.com.tr scraper")
    p.add_argument("--max-pages", type=int, default=0)
    p.add_argument("--delay", type=float, default=1.5)
    p.add_argument("--csv", default="data/century21_latest.csv")
    args = p.parse_args()
    result = scrape_all(Path(args.csv), max_pages=args.max_pages, delay=args.delay)
    print(f"\n[C21] Tamamlandı: {len(result)} ilan")


if __name__ == "__main__":
    main()
