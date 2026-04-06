"""hepsiemlak_scraper.py

Hepsiemlak liste sayfalarini Nuxt state uzerinden scrape eder.
Liste sayfalari page-range worker'lari ile paralel taranir, detaylar ise
thread basina bir Chrome instance ile cekilir.
"""
from __future__ import annotations

import argparse
import atexit
import base64
import csv
import io
import json
import random
import re
import threading
import time
import zipfile
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urljoin, urlparse

from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait

from scraper_base import ThreadedDetailLoop

BASE_URL = "https://www.hepsiemlak.com"

# Apify proxy URL'si – main.py tarafindan ayarlanir (None = proxy yok)
PROXY_URL: Optional[str] = None


CATEGORIES: dict[str, str] = {
    "satilik": BASE_URL + "/satilik",
    "kiralik": BASE_URL + "/kiralik",
    "satilik_isyeri": BASE_URL + "/satilik/isyeri",
    "kiralik_isyeri": BASE_URL + "/kiralik/isyeri",
}

COLUMNS = [
    "url",
    "listing_id",
    "title",
    "category",
    "property_type",
    "seller_type",
    "advertiser_type",
    "advertiser_name",
    "city",
    "county",
    "district",
    "price",
    "currency",
    "gross_sqm",
    "net_sqm",
    "room_count",
    "bathroom_count",
    "floor",
    "floor_count",
    "building_age",
    "heating",
    "credit_status",
    "deed_status",
    "furnished",
    "usage_status",
    "trade_status",
    "image_count",
    "has_video",
    "latitude",
    "longitude",
    "description",
    "in_attributes",
    "location_attributes",
    "room_attributes",
    "service_attributes",
    "usage_attributes",
    "created_at",
    "updated_at",
    "listing_updated_at",
    "scraped_at_utc",
]

_THREAD_LOCAL = threading.local()
_DRIVER_REGISTRY: list[webdriver.Chrome] = []
_DRIVER_REGISTRY_LOCK = threading.Lock()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def clean_html_text(value: str | None) -> str:
    if not value:
        return ""
    text = re.sub(r"<[^>]+>", " ", value)
    text = unescape(text)
    text = text.replace("\xa0", " ")
    return re.sub(r"\s+", " ", text).strip()


def join_names(items: Any) -> str:
    if not isinstance(items, list):
        return ""
    names = []
    for item in items:
        if isinstance(item, dict):
            name = clean_html_text(str(item.get("name") or ""))
            if name:
                names.append(name)
    return " | ".join(names)


_PROXY_DEFAULT = object()  # sentinel: "modül seviyesi PROXY_URL kullan"


def _make_proxy_auth_extension(host: str, port: int, username: str, password: str) -> str:
    """Chrome Manifest V3 extension olarak proxy kimlik dogrulama paketi olusturur.
    Base64 kodlu zip dondurur; opts.add_encoded_extension() ile kullanilir.
    MV3 kullaniliyor: Chrome 127+ MV2 destegini kaldirdi.
    """
    safe_user = username.replace("\\", "\\\\").replace('"', '\\"')
    safe_pass = password.replace("\\", "\\\\").replace('"', '\\"')
    manifest = json.dumps({
        "manifest_version": 3,
        "name": "Proxy Auth",
        "version": "1.0.0",
        "permissions": [
            "proxy", "webRequest", "webRequestAuthProvider",
        ],
        "host_permissions": ["<all_urls>"],
        "background": {"service_worker": "background.js"},
    })
    # MV3 service worker: proxy ayarla + auth credentials don
    background = (
        f'chrome.proxy.settings.set({{'
        f'value:{{'
        f'mode:"fixed_servers",'
        f'rules:{{'
        f'singleProxy:{{scheme:"http",host:"{host}",port:{port}}},'
        f'bypassList:["localhost"]'
        f'}}'
        f'}},'
        f'scope:"regular"'
        f'}}, ()=>{{}});'
        f'chrome.webRequest.onAuthRequired.addListener('
        f'(details)=>{{return{{authCredentials:{{username:"{safe_user}",password:"{safe_pass}"}}}};}},'
        f'{{urls:["<all_urls>"]}},'
        f'["blocking"]'
        f');'
    )
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("manifest.json", manifest)
        zf.writestr("background.js", background)
    return base64.b64encode(buf.getvalue()).decode()


def create_driver(headless: bool = False, no_images: bool = True, proxy_url: object = _PROXY_DEFAULT) -> webdriver.Chrome:
    # proxy_url=_PROXY_DEFAULT → modül değişkeni PROXY_URL kullanılır
    # proxy_url=None → proxy yok (datacenter fallback)
    # proxy_url=<str> → o URL kullanılır
    effective_proxy: Optional[str] = PROXY_URL if proxy_url is _PROXY_DEFAULT else proxy_url  # type: ignore[assignment]

    # Proxy auth extension gerekiyor mu?
    _proxy_ext_b64: Optional[str] = None
    if effective_proxy:
        _pp = urlparse(effective_proxy)
        if _pp.username and _pp.password:
            try:
                _proxy_ext_b64 = _make_proxy_auth_extension(
                    _pp.hostname or "", _pp.port or 8011, _pp.username, _pp.password
                )
                print(f"[HEPSIEMLAK] Proxy auth extension olusturuldu: {_pp.hostname}:{_pp.port}")
            except Exception as _e:
                print(f"[HEPSIEMLAK] Proxy auth extension olusturulamadi: {_e}")

    opts = Options()
    opts.page_load_strategy = "eager"
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )
    if headless:
        opts.add_argument("--headless=new")
        opts.add_argument("--window-size=1600,2200")
    else:
        opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-hang-monitor")
    opts.add_argument("--disable-renderer-backgrounding")
    opts.add_argument("--disable-background-timer-throttling")
    opts.add_argument("--js-flags=--max-old-space-size=256")  # V8 heap limiti – crash onleme
    opts.add_argument("--memory-pressure-off")
    opts.add_argument("--single-process")  # Ayri renderer process yok – RAM tasarrufu
    # --disable-extensions kullanilmaz: proxy auth extension'in yuklenmesini engeller
    opts.add_argument("--disable-plugins")
    opts.add_argument("--disable-background-networking")
    opts.add_argument("--disable-default-apps")
    opts.add_argument("--disable-sync")
    opts.add_argument("--disable-translate")
    opts.add_argument("--hide-scrollbars")
    opts.add_argument("--mute-audio")
    # Single-process, farklı render işlemlerini iptal eder, CF/Chrome crash'leri için riskli
    # opts.add_argument("--single-process")
    if no_images:
        opts.add_experimental_option(
            "prefs",
            {"profile.managed_default_content_settings.images": 2},
        )
    if _proxy_ext_b64:
        # Kimlik dogrulamali proxy – extension ile
        opts.add_encoded_extension(_proxy_ext_b64)
    elif effective_proxy:
        # Kimlik dogrulamasiz proxy – dogrudan --proxy-server
        _pp2 = urlparse(effective_proxy)
        proxy_bare = f"{_pp2.scheme}://{_pp2.hostname}:{_pp2.port}"
        opts.add_argument(f"--proxy-server={proxy_bare}")
    last_err: Optional[Exception] = None
    for attempt in range(3):
        try:
            driver = webdriver.Chrome(options=opts)
            driver.set_page_load_timeout(35)
            try:
                driver.execute_cdp_cmd(
                    "Page.addScriptToEvaluateOnNewDocument",
                    {"source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"},
                )
            except Exception:
                pass
            with _DRIVER_REGISTRY_LOCK:
                _DRIVER_REGISTRY.append(driver)
            return driver
        except Exception as exc:
            last_err = exc
            print(f"[HEPSIEMLAK] Driver baslatma hatasi ({attempt + 1}/3): {exc}")
            time.sleep(2 + attempt)
    raise RuntimeError(f"Chrome driver baslatilamadi: {last_err}")


def _cleanup_drivers() -> None:
    with _DRIVER_REGISTRY_LOCK:
        drivers = list(_DRIVER_REGISTRY)
        _DRIVER_REGISTRY.clear()
    for driver in drivers:
        try:
            driver.quit()
        except Exception:
            pass


atexit.register(_cleanup_drivers)


def get_thread_driver(headless: bool, no_images: bool) -> webdriver.Chrome:
    driver = getattr(_THREAD_LOCAL, "driver", None)
    if driver is None:
        driver = create_driver(headless=headless, no_images=no_images)
        _THREAD_LOCAL.driver = driver
    return driver


def wait_for_cf_challenge(driver: webdriver.Chrome, timeout: int = 45) -> None:
    """Wait for Cloudflare Turnstile / Just a moment protection to complete."""
    _CF_PHRASES = ["just a moment", "bir dakika", "güvenlik doğrulaması", "help us verify"]
    deadline = time.monotonic() + timeout
    last_title = ""
    while time.monotonic() < deadline:
        title = ""
        try:
            title = driver.title or ""
        except Exception:
            pass
        lower_title = title.lower()
        last_title = title
        if not any(phrase in lower_title for phrase in _CF_PHRASES):
            return
        time.sleep(2)
    raise TimeoutException(
        f"Cloudflare challenge did not resolve in {timeout}s (last title={last_title!r})"
    )


def wait_for_nuxt_data(driver: webdriver.Chrome, timeout: int = 45) -> dict[str, Any]:
    # Nuxt 2: window.__NUXT__.data[0]
    # Nuxt 3: window.__NUXT__.payload  veya  window.__NUXT_DATA__
    _CHECK_JS = (
        "return !!("
        "  (window.__NUXT__ && window.__NUXT__.data && window.__NUXT__.data[0]) ||"
        "  (window.__NUXT__ && window.__NUXT__.payload) ||"
        "  window.__NUXT_DATA__"
        ")"
    )
    try:
        WebDriverWait(driver, timeout).until(lambda d: d.execute_script(_CHECK_JS))
    except TimeoutException:
        # Teshis: __NUXT__ var mi, hangi anahtarlara sahip?
        try:
            diag = driver.execute_script(
                "return window.__NUXT__ ? Object.keys(window.__NUXT__) : 'NUXT_YOK'"
            )
            print(f"[HEPSIEMLAK DIAG] __NUXT__ keys={diag}")
        except Exception:
            pass
        raise
    # Veriyi oku: Nuxt 2 once, sonra Nuxt 3
    data = driver.execute_script(
        "return (window.__NUXT__ && window.__NUXT__.data && window.__NUXT__.data[0]) "
        "|| (window.__NUXT__ && window.__NUXT__.payload) "
        "|| window.__NUXT_DATA__ "
        "|| null"
    )
    if not isinstance(data, dict):
        raise RuntimeError(f"Nuxt data bulunamadi (tip={type(data).__name__})")
    return data


def fetch_nuxt_data(
    driver: webdriver.Chrome,
    url: str,
    attempts: int = 3,
    settle_seconds: float = 2.0,
    cf_timeout: int = 12,
) -> dict[str, Any]:
    last_err: Optional[Exception] = None
    for attempt in range(1, attempts + 1):
        try:
            driver.get(url)
            # Settle: eager load sonrasi JS hydration icin bekle
            time.sleep(3.0)
            try:
                src_early = driver.page_source or ""
                cur_url = driver.current_url or url
            except Exception:
                src_early = ""
                cur_url = url
            if len(src_early) < 100:
                print(
                    f"[HEPSIEMLAK DIAG] current_url={cur_url!r} "
                    f"src_raw={repr(src_early[:120])}"
                )
                raise RuntimeError(f"Bos sayfa alindi (src_len={len(src_early)}, url={cur_url})")
            wait_for_cf_challenge(driver, timeout=cf_timeout)
            data = wait_for_nuxt_data(driver, timeout=45)
            time.sleep(settle_seconds + random.uniform(0, 0.8))
            return data
        except (TimeoutException, WebDriverException, RuntimeError) as exc:
            exc_str = str(exc)
            # ERR_TUNNEL_CONNECTION_FAILED: proxy tunnel koptu, driver yenile
            if "err_tunnel_connection_failed" in exc_str.lower() or "err_proxy_connection_failed" in exc_str.lower():
                print(f"[HEPSIEMLAK] Proxy tunnel hatasi, driver yenileniyor: {exc_str[:120]}")
                raise  # bootstrap_nuxt_data yeni driver olusturur
            # NOT: CF hatasi re-raise edilmiyor – ayni driver ile 2. deneme CF session cookie'leri
            # sayesinde CF'yi atlar (bootstrap icin kritik). run_range zaten kendi CF tespitini yapar.
            last_err = exc
            # Debug: bot engeli veya bos sayfa ise kaydet
            try:
                title = driver.title or "(bos)"
                src = driver.page_source or ""
                print(
                    f"[HEPSIEMLAK DEBUG] Nuxt data alinamadi ({attempt}/{attempts}): "
                    f"title={title!r} src_len={len(src)} exc={exc}"
                )
                debug_path = Path("/tmp/hepsiemlak_debug_page.html")
                if not debug_path.exists() and src:
                    debug_path.write_text(src[:50000], encoding="utf-8")
            except Exception:
                pass
            if attempt < attempts:
                time.sleep(2.0 * attempt)
    raise RuntimeError(f"Nuxt data alinamadi: {url} -> {last_err}")


def bootstrap_nuxt_data(
    url: str,
    headless: bool,
    no_images: bool,
    attempts: int = 3,
) -> dict[str, Any]:
    last_err: Optional[Exception] = None
    proxy_error_count = 0

    for i in range(attempts):
        # Tum onceki denemeler proxy engeli idiyse son denemede proxy'siz dene
        use_proxy: object = _PROXY_DEFAULT
        if i == attempts - 1 and PROXY_URL and proxy_error_count >= i:
            use_proxy = None
            print("[HEPSIEMLAK] Tum denemeler proxy engeli – son denemede proxysiz baglanti deneniyor...")
        driver = create_driver(headless=headless, no_images=no_images, proxy_url=use_proxy)
        try:
            return fetch_nuxt_data(driver, url, attempts=2, settle_seconds=2.5, cf_timeout=60)
        except Exception as exc:
            last_err = exc
            if "proxy engeli" in str(exc).lower() or "bos sayfa" in str(exc).lower():
                proxy_error_count += 1
            time.sleep(2.0 + random.uniform(0, 1.0))
        finally:
            try:
                driver.quit()
            except Exception:
                pass
    raise RuntimeError(f"Bootstrap Nuxt data alinamadi: {url} -> {last_err}")


def bootstrap_nuxt_data_from_pages(
    base_url: str,
    pages: list[int],
    headless: bool,
    no_images: bool,
    attempts_per_url: int = 2,
) -> tuple[dict[str, Any], int]:
    seen = set()
    last_err: Optional[Exception] = None
    candidates = [page for page in pages if page > 0]
    candidates.append(1)

    for page in candidates:
        if page in seen:
            continue
        seen.add(page)
        url = listing_page_url(base_url, page)
        try:
            data = bootstrap_nuxt_data(
                url=url,
                headless=headless,
                no_images=no_images,
                attempts=attempts_per_url,
            )
            return data, page
        except Exception as exc:
            last_err = exc

    raise RuntimeError(f"Bootstrap Nuxt data alinamadi: {base_url} -> {last_err}")


def listing_page_url(base_url: str, page: int) -> str:
    if page <= 1:
        return base_url
    return f"{base_url}?page={page}"


def parse_page_starts(raw: str | None) -> list[int]:
    if not raw:
        return []
    starts = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        value = int(part)
        if value > 0:
            starts.append(value)
    return sorted(set(starts))


def parse_page_ranges(raw: str | None) -> list[tuple[int, int]]:
    if not raw:
        return []
    ranges: list[tuple[int, int]] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" not in part:
            raise SystemExit(f"Gecersiz page range: {part}")
        start_s, end_s = part.split("-", 1)
        start = int(start_s.strip())
        end = int(end_s.strip())
        if start <= 0 or end < start:
            raise SystemExit(f"Gecersiz page range: {part}")
        ranges.append((start, end))
    return ranges


def build_page_ranges(total_pages: int, workers: int, starts: list[int]) -> list[tuple[int, int]]:
    if total_pages <= 0:
        return []

    if starts:
        valid = [x for x in starts if x <= total_pages]
        if not valid:
            valid = [1]
        ranges = []
        for idx, start in enumerate(valid):
            end = (valid[idx + 1] - 1) if idx + 1 < len(valid) else total_pages
            if start <= end:
                ranges.append((start, end))
        return ranges

    workers = max(1, min(workers, total_pages))
    base = total_pages // workers
    extra = total_pages % workers
    ranges = []
    start = 1
    for i in range(workers):
        size = base + (1 if i < extra else 0)
        end = start + size - 1
        ranges.append((start, end))
        start = end + 1
    return ranges


def clamp_page_ranges(
    ranges: list[tuple[int, int]],
    total_pages: int,
    max_pages: int,
) -> list[tuple[int, int]]:
    if not ranges:
        return []
    effective_last_page = total_pages if max_pages <= 0 else min(max_pages, total_pages)
    clamped: list[tuple[int, int]] = []
    for start, end in ranges:
        if start > effective_last_page:
            continue
        clamped.append((start, min(end, effective_last_page)))
    return clamped


def bootstrap_candidates_from_ranges(
    page_ranges: list[tuple[int, int]],
    page_starts: list[int],
) -> list[int]:
    candidates: list[int] = []

    for start, end in page_ranges:
        candidates.append(start)
        if start + 1 <= end:
            candidates.append(start + 1)
        if start + 2 <= end:
            candidates.append(start + 2)

    for start in page_starts:
        candidates.append(start)
        candidates.append(start + 1)

    if not candidates:
        candidates.append(1)

    deduped: list[int] = []
    seen = set()
    for page in candidates:
        if page > 0 and page not in seen:
            seen.add(page)
            deduped.append(page)
    return deduped


def extract_listing_urls(list_data: dict[str, Any]) -> list[str]:
    rows = list_data.get("list") or []
    urls = []
    seen = set()
    for item in rows:
        if not isinstance(item, dict):
            continue
        detail_url = item.get("detailUrl")
        if not detail_url:
            continue
        full_url = urljoin(BASE_URL + "/", str(detail_url).lstrip("/"))
        if full_url not in seen:
            seen.add(full_url)
            urls.append(full_url)
    return urls


def collect_page_range(
    category_name: str,
    base_url: str,
    page_start: int,
    page_end: int,
    headless: bool,
    no_images: bool,
    delay: float,
) -> list[str]:
    driver = create_driver(headless=headless, no_images=no_images)
    urls: list[str] = []
    seen = set()
    try:
        for page in range(page_start, page_end + 1):
            url = listing_page_url(base_url, page)
            try:
                data = fetch_nuxt_data(driver, url)
            except Exception as exc:
                print(f"[HEPSIEMLAK:{category_name}] Sayfa {page} atlandi: {exc}")
                time.sleep(delay + 2 + random.uniform(0, 1.0))
                continue
            page_urls = extract_listing_urls(data)
            fresh = 0
            for item in page_urls:
                if item not in seen:
                    seen.add(item)
                    urls.append(item)
                    fresh += 1
            total_page = data.get("totalPage") or "?"
            print(
                f"[HEPSIEMLAK:{category_name}] Sayfa {page}/{total_page} "
                f"| {fresh} yeni URL"
            )
            time.sleep(delay + random.uniform(0, 0.5))
    finally:
        try:
            driver.quit()
        except Exception:
            pass
    return urls


def collect_category_urls(
    category_name: str,
    base_url: str,
    page_workers: int,
    max_pages: int,
    page_starts: list[int],
    headless: bool,
    no_images: bool,
    delay: float,
) -> tuple[list[str], int]:
    data = bootstrap_nuxt_data(
        url=listing_page_url(base_url, 1),
        headless=headless,
        no_images=no_images,
    )

    detected_total_pages = int(data.get("totalPage") or 1)
    last_page = detected_total_pages if max_pages <= 0 else min(max_pages, detected_total_pages)
    total_ads = int(data.get("totalAdvertisement") or 0)
    print(
        f"[HEPSIEMLAK:{category_name}] Toplam sayfa: {detected_total_pages} "
        f"| Islenecek: 1..{last_page} | Toplam ilan: {total_ads}"
    )

    ranges = build_page_ranges(last_page, page_workers, page_starts)
    print(f"[HEPSIEMLAK:{category_name}] Page worker araliklari: {ranges}")

    if len(ranges) == 1:
        urls = collect_page_range(
            category_name=category_name,
            base_url=base_url,
            page_start=ranges[0][0],
            page_end=ranges[0][1],
            headless=headless,
            no_images=no_images,
            delay=delay,
        )
    else:
        from concurrent.futures import ThreadPoolExecutor, as_completed

        urls = []
        with ThreadPoolExecutor(max_workers=len(ranges)) as exe:
            futures = [
                exe.submit(
                    collect_page_range,
                    category_name,
                    base_url,
                    start,
                    end,
                    headless,
                    no_images,
                    delay,
                )
                for start, end in ranges
            ]
            for fut in as_completed(futures):
                urls.extend(fut.result())

    deduped = []
    seen = set()
    for url in urls:
        if url not in seen:
            seen.add(url)
            deduped.append(url)
    print(f"[HEPSIEMLAK:{category_name}] Toplam benzersiz URL: {len(deduped)}")
    return deduped, detected_total_pages


def parse_detail_row(url: str, data: dict[str, Any]) -> dict[str, Any]:
    detail = data.get("detailData") or {}
    if not isinstance(detail, dict):
        raise RuntimeError("detailData bulunamadi")

    sqm = detail.get("sqm") or {}
    floor = detail.get("floor") or {}
    map_location = detail.get("mapLocation") or {}
    firm = detail.get("firm") or {}

    description_html = (
        detail.get("description")
        or ((data.get("description") or {}).get("content") if isinstance(data.get("description"), dict) else "")
    )

    row = {
        "url": url,
        "listing_id": detail.get("listingId") or "",
        "title": clean_html_text(str(detail.get("title") or data.get("title") or "")),
        "category": clean_html_text(str((detail.get("category") or {}).get("typeName") or "")),
        "property_type": clean_html_text(str((detail.get("subCategory") or {}).get("typeName") or "")),
        "seller_type": clean_html_text(str(detail.get("sellerType") or "")),
        "advertiser_type": clean_html_text(str((firm.get("typeName") or detail.get("advertiseOwner") or ""))),
        "advertiser_name": clean_html_text(str(firm.get("name") or "")),
        "city": clean_html_text(str((detail.get("city") or {}).get("name") or "")),
        "county": clean_html_text(str((detail.get("county") or {}).get("name") or "")),
        "district": clean_html_text(str((detail.get("district") or {}).get("name") or "")),
        "price": detail.get("price"),
        "currency": clean_html_text(str(detail.get("currency") or "")),
        "gross_sqm": ((sqm.get("grossSqm") or [None])[0]),
        "net_sqm": sqm.get("netSqm"),
        "room_count": ",".join(detail.get("roomAndLivingRoom") or []),
        "bathroom_count": detail.get("bathRoom"),
        "floor": clean_html_text(str(floor.get("name") or "")),
        "floor_count": floor.get("count"),
        "building_age": detail.get("age"),
        "heating": clean_html_text(str((detail.get("heating") or {}).get("name") or "")),
        "credit_status": clean_html_text(str((detail.get("credit") or {}).get("name") or "")),
        "deed_status": clean_html_text(str(detail.get("landRegisterName") or detail.get("registerState") or "")),
        "furnished": "Evet" if detail.get("furnished") is True else ("Hayir" if detail.get("furnished") is False else ""),
        "usage_status": clean_html_text(str((detail.get("usage") or {}).get("name") or "")),
        "trade_status": clean_html_text(str((detail.get("barter") or {}).get("name") or "")),
        "image_count": len(detail.get("images") or []),
        "has_video": "Evet" if detail.get("videoUrl") else "Hayir",
        "latitude": map_location.get("lat") or data.get("coordsLat"),
        "longitude": map_location.get("lon") or data.get("coordsLng"),
        "description": clean_html_text(str(description_html or "")),
        "in_attributes": join_names(((detail.get("attributes") or {}).get("inAttributes"))),
        "location_attributes": join_names(((detail.get("attributes") or {}).get("locationAttributes"))),
        "room_attributes": join_names(((detail.get("attributes") or {}).get("roomAttributes"))),
        "service_attributes": join_names(((detail.get("attributes") or {}).get("serviceAttributes"))),
        "usage_attributes": join_names(((detail.get("attributes") or {}).get("usageAttributes"))),
        "created_at": detail.get("createdDate") or "",
        "updated_at": detail.get("updatedDate") or "",
        "listing_updated_at": detail.get("listingUpdatedDate") or "",
        "scraped_at_utc": utc_now_iso(),
    }
    return row


def parse_list_row(item: dict[str, Any]) -> dict[str, Any]:
    firm = item.get("firm") or {}
    sqm = item.get("sqm") or {}
    floor = item.get("floor") or {}
    map_location = item.get("mapLocation") or {}
    url = urljoin(BASE_URL + "/", str(item.get("detailUrl") or "").lstrip("/"))

    row = {
        "url": url,
        "listing_id": item.get("listingId") or "",
        "title": clean_html_text(str(item.get("title") or "")),
        "category": clean_html_text(str((item.get("category") or {}).get("typeName") or "")),
        "property_type": clean_html_text(str((item.get("subCategory") or {}).get("typeName") or "")),
        "seller_type": clean_html_text(str(item.get("sellerType") or "")),
        "advertiser_type": clean_html_text(str((firm.get("typeName") or item.get("advertiseOwner") or ""))),
        "advertiser_name": clean_html_text(str(firm.get("name") or (item.get("owner") or {}).get("name") or "")),
        "city": clean_html_text(str((item.get("city") or {}).get("name") or "")),
        "county": clean_html_text(str((item.get("county") or {}).get("name") or "")),
        "district": clean_html_text(str((item.get("district") or {}).get("name") or "")),
        "price": item.get("price"),
        "currency": clean_html_text(str(item.get("currency") or "")),
        "gross_sqm": ((sqm.get("grossSqm") or [None])[0]),
        "net_sqm": sqm.get("netSqm"),
        "room_count": ",".join(item.get("roomAndLivingRoom") or []),
        "bathroom_count": item.get("bathRoom"),
        "floor": clean_html_text(str(floor.get("name") or "")),
        "floor_count": floor.get("count"),
        "building_age": item.get("age"),
        "heating": clean_html_text(str((item.get("heating") or {}).get("name") or "")),
        "credit_status": clean_html_text(str((item.get("credit") or {}).get("name") or "")),
        "deed_status": clean_html_text(str(item.get("landRegisterName") or item.get("registerState") or "")),
        "furnished": "Evet" if item.get("furnished") is True else ("Hayir" if item.get("furnished") is False else ""),
        "usage_status": clean_html_text(str((item.get("usage") or {}).get("name") or "")),
        "trade_status": clean_html_text(str((item.get("barter") or {}).get("name") or "")),
        "image_count": len(item.get("images") or []),
        "has_video": "Evet" if item.get("videoUrl") else "Hayir",
        "latitude": map_location.get("lat"),
        "longitude": map_location.get("lon"),
        "description": clean_html_text(str(item.get("detailDescription") or "")),
        "in_attributes": "",
        "location_attributes": "",
        "room_attributes": "",
        "service_attributes": "",
        "usage_attributes": "",
        "created_at": item.get("createDate") or "",
        "updated_at": item.get("updatedDate") or "",
        "listing_updated_at": item.get("listingUpdatedDate") or "",
        "scraped_at_utc": utc_now_iso(),
    }
    return row


def load_checkpoint(path: Path) -> dict[str, Any]:
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def save_checkpoint(path: Path, checkpoint: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(checkpoint, ensure_ascii=False, indent=2), encoding="utf-8")


def merge_unique_urls(existing: list[str], fresh: list[str]) -> list[str]:
    merged: list[str] = []
    seen = set()
    for url in existing + fresh:
        if url not in seen:
            seen.add(url)
            merged.append(url)
    return merged


def append_csv_row(path: Path, row: dict[str, Any], lock: threading.Lock) -> None:
    with lock:
        path.parent.mkdir(parents=True, exist_ok=True)
        file_exists = path.exists()
        with path.open("a", newline="", encoding="utf-8-sig") as fh:
            writer = csv.DictWriter(fh, fieldnames=COLUMNS, extrasaction="ignore")
            if not file_exists:
                writer.writeheader()
            writer.writerow({c: row.get(c, "") for c in COLUMNS})


def scrape_list_only(
    categories: list[str],
    csv_path: Path,
    max_pages: int,
    page_workers: int,
    delay: float,
    headless: bool,
    no_images: bool,
    page_starts: list[int],
    page_ranges: list[tuple[int, int]],
) -> None:
    checkpoint_path = csv_path.with_suffix("").with_suffix(".checkpoint.json")
    checkpoint = load_checkpoint(checkpoint_path)
    list_done_pages = checkpoint.get("list_done_pages", {})
    meta = checkpoint.get("meta", {})
    csv_lock = threading.Lock()
    checkpoint_lock = threading.Lock()  # Checkpoint dosyasina yazma kilidi (paralel kategori destegi)

    from concurrent.futures import ThreadPoolExecutor, as_completed

    # --- 1. Adim: Bootstrap sirayla yap (ayni anda birden fazla CF tetiklememek icin) ---
    CategoryMeta = dict  # typing alias
    cat_meta: dict[str, CategoryMeta] = {}
    for category_name in categories:
        if category_name not in CATEGORIES:
            raise SystemExit(f"Bilinmeyen kategori: {category_name}")
        bootstrap_pages = bootstrap_candidates_from_ranges(page_ranges, page_starts)
        data, bootstrap_page = bootstrap_nuxt_data_from_pages(
            base_url=CATEGORIES[category_name],
            pages=bootstrap_pages,
            headless=headless,
            no_images=no_images,
        )
        detected_total_pages = int(data.get("totalPage") or 1)
        last_page = detected_total_pages if max_pages <= 0 else min(max_pages, detected_total_pages)
        total_ads = int(data.get("totalAdvertisement") or 0)
        meta[category_name] = {
            "total_pages": detected_total_pages,
            "total_ads": total_ads,
            "mode": "list_only",
            "updated_at_utc": utc_now_iso(),
        }
        checkpoint["meta"] = meta
        checkpoint["list_done_pages"] = list_done_pages
        save_checkpoint(checkpoint_path, checkpoint)
        cat_meta[category_name] = {
            "bootstrap_page": bootstrap_page,
            "detected_total_pages": detected_total_pages,
            "last_page": last_page,
            "total_ads": total_ads,
        }
        print(
            f"[HEPSIEMLAK:{category_name}] LIST_ONLY "
            f"| Bootstrap sayfasi: {bootstrap_page} "
            f"| Toplam sayfa: {detected_total_pages} | Islenecek: 1..{last_page} | ilan: {total_ads}"
        )

    # --- 2. Adim: Tum kategorilerin worker'larini paralel baslat ---
    def process_category(category_name: str) -> None:
        cm = cat_meta[category_name]
        detected_total_pages = cm["detected_total_pages"]
        last_page = cm["last_page"]
        done_pages: set[int] = {int(x) for x in list_done_pages.get(category_name, [])}
        cat_page_lock = threading.Lock()
        ranges = (
            clamp_page_ranges(page_ranges, detected_total_pages, max_pages)
            if page_ranges
            else build_page_ranges(last_page, page_workers, page_starts)
        )
        print(f"[HEPSIEMLAK:{category_name}] Page worker araliklari: {ranges}")

        def run_range(start: int, end: int) -> int:
            driver = create_driver(headless=headless, no_images=no_images)
            written = 0
            cf_streak = 0  # Arka arkaya CF sayaci
            try:
                for page in range(start, end + 1):
                    with cat_page_lock:
                        if page in done_pages:
                            continue
                    url = listing_page_url(CATEGORIES[category_name], page)
                    try:
                        page_data = fetch_nuxt_data(driver, url)
                        cf_streak = 0  # Basarili istek -> streak sifirla
                    except Exception as exc:
                        exc_str = str(exc)
                        is_cf = "just a moment" in exc_str.lower() or "cloudflare" in exc_str.lower()
                        is_crash = "tab crashed" in exc_str.lower() or "timed out receiving message from renderer" in exc_str.lower()
                        if is_cf or is_crash:
                            cf_streak += 1 if is_cf else 0
                            # Driver artik kullanilabilir degil – yenile
                            try:
                                driver.quit()
                            except Exception:
                                pass
                            driver = create_driver(headless=headless, no_images=no_images)
                            if is_cf and cf_streak >= 2:
                                wait_sec = 5
                                print(
                                    f"[HEPSIEMLAK:{category_name}] CF streak={cf_streak}, "
                                    f"{wait_sec}s bekleniyor..."
                                )
                                time.sleep(wait_sec)
                        print(f"[HEPSIEMLAK:{category_name}] Sayfa {page} atlandi: {exc}")
                        time.sleep(delay + 2 + random.uniform(0, 1.0))
                        continue

                    items = page_data.get("list") or []
                    fresh = 0
                    for item in items:
                        if not isinstance(item, dict):
                            continue
                        row = parse_list_row(item)
                        if not row["url"]:
                            continue
                        append_csv_row(csv_path, row, csv_lock)
                        fresh += 1
                        written += 1

                    with cat_page_lock:
                        done_pages.add(page)
                        with checkpoint_lock:
                            list_done_pages[category_name] = sorted(done_pages)
                            checkpoint["list_done_pages"] = list_done_pages
                            checkpoint["meta"] = meta
                            save_checkpoint(checkpoint_path, checkpoint)

                    total_page = page_data.get("totalPage") or "?"
                    print(
                        f"[HEPSIEMLAK:{category_name}] Sayfa {page}/{total_page} "
                        f"| {fresh} satir yazildi"
                    )
                    time.sleep(delay + random.uniform(0, 0.4))
            finally:
                try:
                    driver.quit()
                except Exception:
                    pass
            return written

        total_written = 0
        with ThreadPoolExecutor(max_workers=len(ranges) or 1) as exe:
            futures = [exe.submit(run_range, start, end) for start, end in ranges]
            for fut in as_completed(futures):
                total_written += fut.result()

        print(f"[HEPSIEMLAK:{category_name}] LIST_ONLY tamamlandi | {total_written} satir yazildi")

    # Kategorilerin worker'larini paralel calistir (bootstrap bitti, sirayla yapildi)
    with ThreadPoolExecutor(max_workers=len(categories) or 1) as cat_exe:
        cat_futures = [cat_exe.submit(process_category, cn) for cn in categories]
        for f in as_completed(cat_futures):
            f.result()


def scrape_all(
    categories: list[str],
    csv_path: Path,
    max_pages: int,
    page_workers: int,
    detail_workers: int,
    delay: float,
    headless: bool,
    no_images: bool,
    page_starts: list[int],
    worker_cfg_path: Optional[Path] = None,
) -> None:
    checkpoint_path = csv_path.with_suffix("").with_suffix(".checkpoint.json")
    checkpoint = load_checkpoint(checkpoint_path)
    collected = checkpoint.get("collected", {})
    done_urls = checkpoint.get("done_urls", [])
    meta = checkpoint.get("meta", {})

    for category_name in categories:
        if category_name not in CATEGORIES:
            raise SystemExit(f"Bilinmeyen kategori: {category_name}")
        if category_name not in collected:
            try:
                urls, total_pages = collect_category_urls(
                    category_name=category_name,
                    base_url=CATEGORIES[category_name],
                    page_workers=page_workers,
                    max_pages=max_pages,
                    page_starts=page_starts,
                    headless=headless,
                    no_images=no_images,
                    delay=delay,
                )
            except Exception as exc:
                partial_urls = collected.get(category_name, [])
                checkpoint["collected"] = collected
                checkpoint["meta"] = meta
                checkpoint["collect_errors"] = checkpoint.get("collect_errors", {})
                checkpoint["collect_errors"][category_name] = {
                    "error": str(exc),
                    "failed_at_utc": utc_now_iso(),
                    "partial_url_count": len(partial_urls),
                }
                save_checkpoint(checkpoint_path, checkpoint)
                raise
            collected[category_name] = merge_unique_urls(collected.get(category_name, []), urls)
            meta[category_name] = {
                "total_pages": total_pages,
                "collected_at_utc": utc_now_iso(),
            }
            checkpoint["collected"] = collected
            checkpoint["meta"] = meta
            save_checkpoint(checkpoint_path, checkpoint)
        else:
            print(
                f"[HEPSIEMLAK:{category_name}] {len(collected[category_name])} URL "
                "checkpoint'ten yuklendi, toplama atlandi"
            )

    all_urls: list[str] = []
    seen = set()
    for category_name in categories:
        for url in collected.get(category_name, []):
            if url not in seen:
                seen.add(url)
                all_urls.append(url)

    print(
        f"[HEPSIEMLAK] Toplam URL: {len(all_urls)} "
        f"| Tamamlanan: {len(done_urls)} | Kalan: {len([u for u in all_urls if u not in set(done_urls)])}"
    )

    def fetch_and_parse(url: str) -> Optional[dict[str, Any]]:
        driver = get_thread_driver(headless=headless, no_images=no_images)
        data = fetch_nuxt_data(driver, url)
        return parse_detail_row(url, data)

    loop = ThreadedDetailLoop(
        source="hepsiemlak",
        csv_path=csv_path,
        done_urls=list(done_urls),
        cp_path=checkpoint_path,
        cp=checkpoint,
        columns=list(COLUMNS),
        workers=detail_workers,
        delay=delay,
        worker_cfg_path=worker_cfg_path,
    )
    loop.run(all_urls, fetch_and_parse)


def main() -> None:
    parser = argparse.ArgumentParser(description="Hepsiemlak satilik/kiralik scraper")
    parser.add_argument("--mode", choices=["full", "list_only"], default="full", help="full = detay sayfasi, list_only = liste payload'i")
    parser.add_argument("--categories", default="satilik,kiralik", help="Virgulle ayrilmis kategori listesi")
    parser.add_argument("--csv", default="data/hepsiemlak_listings.csv", help="CSV cikti yolu")
    parser.add_argument("--max-pages", type=int, default=0, help="Her kategoride en fazla sayfa sayisi (0 = sinirsiz)")
    parser.add_argument("--page-workers", type=int, default=4, help="Liste sayfasi worker sayisi")
    parser.add_argument("--detail-workers", type=int, default=4, help="Detay worker sayisi")
    parser.add_argument("--page-starts", default="", help="Ornek: 1,101,201,301 -> worker aralik baslangiclari")
    parser.add_argument("--page-ranges", default="", help="Ornek: 1-100,101-200 -> net segment araliklari")
    parser.add_argument("--delay", type=float, default=1.0, help="Istekler arasi taban bekleme")
    parser.add_argument("--headless", action=argparse.BooleanOptionalAction, default=True, help="Chrome'u headless calistir")
    parser.add_argument("--no-images", action=argparse.BooleanOptionalAction, default=True, help="Resimleri yukleme")
    parser.add_argument("--worker-cfg", default="", help="Opsiyonel worker_cfg.json yolu")
    args = parser.parse_args()

    categories = [x.strip() for x in args.categories.split(",") if x.strip()]
    page_starts = parse_page_starts(args.page_starts)
    page_ranges = parse_page_ranges(args.page_ranges)
    worker_cfg_path = Path(args.worker_cfg) if args.worker_cfg else None

    if args.mode == "list_only":
        scrape_list_only(
            categories=categories,
            csv_path=Path(args.csv),
            max_pages=args.max_pages,
            page_workers=args.page_workers,
            delay=args.delay,
            headless=bool(args.headless),
            no_images=bool(args.no_images),
            page_starts=page_starts,
            page_ranges=page_ranges,
        )
    else:
        scrape_all(
            categories=categories,
            csv_path=Path(args.csv),
            max_pages=args.max_pages,
            page_workers=args.page_workers,
            detail_workers=args.detail_workers,
            delay=args.delay,
            headless=bool(args.headless),
            no_images=bool(args.no_images),
            page_starts=page_starts,
            worker_cfg_path=worker_cfg_path,
        )


if __name__ == "__main__":
    main()
