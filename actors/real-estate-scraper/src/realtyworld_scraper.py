"""
realtyworld_scraper.py — RealtyWorld.com.tr portföy scraper

Liste: /tr/portfoyler?Page_No={page}
Detay: /tr/emlak/{slug}/{id}
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

from .http_utils import fetch_html, clean_text
from .checkpoint import load_checkpoint, save_checkpoint
from .models import CanonicalListing, utc_now_iso

BASE_URL = "https://www.realtyworld.com.tr"
LIST_URL = BASE_URL + "/tr/portfoyler?Page_No={page}"

COLUMNS = list(CanonicalListing.__dataclass_fields__.keys())


def _parse_max_page(html: str) -> int:
    nums = re.findall(r"Page_No=(\d+)", html, re.IGNORECASE)
    return max((int(n) for n in nums), default=1)


def _extract_listing_links(html: str) -> list[str]:
    hrefs = re.findall(r'href=[\'"](/tr/emlak/[^\'"]+/\d+)[\'"]', html, re.IGNORECASE)
    return sorted(set(BASE_URL + h for h in hrefs))


def _parse_coordinates(html: str) -> tuple[Optional[float], Optional[float]]:
    for pat in [
        r"_latitude\s*=\s*(-?\d+(?:\.\d+)?)\s*;\s*_longitude\s*=\s*(-?\d+(?:\.\d+)?)",
        r"ll=([-0-9.]+),([-0-9.]+)",
    ]:
        m = re.search(pat, html, re.IGNORECASE | re.DOTALL)
        if m:
            try:
                return float(m.group(1)), float(m.group(2))
            except ValueError:
                continue
    return None, None


def _infer_category(features: dict[str, str]) -> Optional[str]:
    prop_type = features.get("Gayrimenkul Tipi") or features.get("Emlak Tipi") or ""
    if any(w in prop_type for w in ["Arsa"]):
        return "arsa"
    if any(w in prop_type for w in ["Ticari"]):
        return "ticari"
    if any(w in prop_type for w in ["Konut"]):
        return "konut"
    return None


def parse_detail(url: str, html: str) -> CanonicalListing:
    def _clean(s: str) -> str:
        return clean_text(s).strip()

    def _first(patterns: list[str], flags=re.IGNORECASE | re.DOTALL) -> Optional[str]:
        for pat in patterns:
            m = re.search(pat, html, flags)
            if m:
                return _clean(m.group(1))
        return None

    title = _first([
        r"<h1[^>]*>(.*?)</h1>",
        r"<meta[^>]+property=[\"']og:title[\"'][^>]+content=[\"'](.*?)[\"']",
        r"<title[^>]*>(.*?)</title>",
    ])

    dt_dd_pairs = re.findall(r"<dt[^>]*>(.*?)</dt>\s*<dd[^>]*>(.*?)</dd>", html, re.IGNORECASE | re.DOTALL)
    features: dict[str, str] = {_clean(k): _clean(v) for k, v in dt_dd_pairs}

    description = _first([
        r"<div[^>]*class=[\"'][^\"']*property-description[^\"']*[\"'][^>]*>(.*?)</div>",
        r"<meta[^>]+name=[\"']description[\"'][^>]+content=[\"'](.*?)[\"']",
    ])

    location = _first([r"<address[^>]*>(.*?)</address>"])
    if not location:
        location = features.get("Konum") or features.get("Adres")

    listing_no = features.get("İlan No") or features.get("lan No")
    category = features.get("Gayrimenkul Tipi") or features.get("Emirlik Tipi") or _infer_category(features)
    price = features.get("Fiyat")
    lat, lng = _parse_coordinates(html)

    return CanonicalListing(
        source="RealtyWorld",
        url=url,
        title=title,
        listing_no=listing_no,
        location=location,
        category=category,
        transaction_type=features.get("İşlem Tipi") or features.get("lem Tipi"),
        property_type=features.get("Gayrimenkul Tipi"),
        price=price,
        m2=features.get("Metrekare"),
        room_count=features.get("Oda Sayısı") or features.get("Oda Says"),
        building_age=features.get("Bina Yaşı") or features.get("Bina Ya"),
        floor=features.get("Bulunduğu Kat") or features.get("Bulunduu Kat"),
        total_floors=features.get("Kat Sayısı") or features.get("Kat Says"),
        heating=features.get("Isıtma") or features.get("Istma"),
        description=description,
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
    collected: list[str] = list(cp.get("collected", []))
    collecting_done: bool = cp.get("collecting_done", False)
    done_urls: set[str] = set(cp.get("done_urls", []))
    results: list[CanonicalListing] = []

    if not collecting_done:
        seen: set[str] = set(collected)
        try:
            first_html = fetch_html(LIST_URL.format(page=1))
            total_pages = _parse_max_page(first_html)
            if max_pages > 0:
                total_pages = min(total_pages, max_pages)
            print(f"[REALTY] Toplam sayfa: {total_pages}")

            # Sayfa 1 zaten indirildi
            new_p1 = [u for u in _extract_listing_links(first_html) if u not in seen]
            seen.update(new_p1)
            collected.extend(new_p1)
            print(f"[REALTY] Sayfa 1/{total_pages}: {len(new_p1)} yeni URL")
            save_checkpoint(checkpoint_path, {
                "collected": collected,
                "collecting_done": False,
                "done_urls": list(done_urls),
            })

            if total_pages > 1:
                def _fetch_realty_page(page_num: int, _d=delay) -> tuple[int, list[str]]:
                    pg_html = fetch_html(LIST_URL.format(page=page_num))
                    time.sleep(random.uniform(_d * 0.3, _d * 0.7))
                    return page_num, _extract_listing_links(pg_html)

                with ThreadPoolExecutor(max_workers=4) as page_pool:
                    page_futs = {page_pool.submit(_fetch_realty_page, p): p for p in range(2, total_pages + 1)}
                    for fut in as_completed(page_futs):
                        p_num = page_futs[fut]
                        try:
                            _, page_urls = fut.result()
                            new = [u for u in page_urls if u not in seen]
                            seen.update(new)
                            collected.extend(new)
                            print(f"[REALTY] Sayfa {p_num}/{total_pages}: {len(new)} yeni URL")
                        except Exception as exc:
                            print(f"[REALTY] Sayfa {p_num} hatası: {exc}")
                save_checkpoint(checkpoint_path, {
                    "collected": collected,
                    "collecting_done": False,
                    "done_urls": list(done_urls),
                })
        except Exception as exc:
            print(f"[REALTY] Koleksiyon hatası: {exc}")

        collecting_done = True
        save_checkpoint(checkpoint_path, {
            "collected": collected,
            "collecting_done": True,
            "done_urls": list(done_urls),
        })
        print(f"[REALTY] Toplam benzersiz URL: {len(collected)}")
    else:
        print(f"[REALTY] {len(collected)} URL checkpoint'ten yüklendi")

    remaining = [u for u in collected if u not in done_urls]
    print(f"[REALTY] Tamamlanan: {len(done_urls)} | Kalan: {len(remaining)}")

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
                print(f"  [REALTY] [{done_count}/{total}] HATA {url} -> {exc}")
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
                save_checkpoint(checkpoint_path, {
                    "collected": collected, "collecting_done": True, "done_urls": list(done_urls),
                })
                abort_exc = exc
                for f in all_futures:
                    f.cancel()
                break
            done_urls.add(url)
            print(f"  [REALTY] [{done_count}/{total}] OK {url}")
            save_checkpoint(checkpoint_path, {
                "collected": collected, "collecting_done": True, "done_urls": list(done_urls),
            })

    if abort_exc is not None:
        raise abort_exc

    return results


def main():
    import argparse
    p = argparse.ArgumentParser(description="RealtyWorld.com.tr scraper")
    p.add_argument("--max-pages", type=int, default=0)
    p.add_argument("--delay", type=float, default=1.5)
    p.add_argument("--csv", default="data/realtyworld_latest.csv")
    args = p.parse_args()
    result = scrape_all(Path(args.csv), max_pages=args.max_pages, delay=args.delay)
    print(f"\n[REALTY] Tamamlandı: {len(result)} ilan")


if __name__ == "__main__":
    main()
