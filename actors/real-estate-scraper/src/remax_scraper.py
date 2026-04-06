"""
remax_scraper.py — Remax.com.tr için Selenium tabanlı ilan toplayıcı.

İki aşamalı süreç:
  Aşama 1 (phase_collect): Kategori listelerinden ilan URL'lerini topla → remax_db
  Aşama 2 (phase_scrape):  Bekleyen URL'leri sırayla scrape et
"""
from __future__ import annotations

import re
import time
from pathlib import Path
from typing import Callable, Optional

import json
import os
import shutil
import tempfile
import zipfile
import undetected_chromedriver as uc
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from .models import CanonicalListing, utc_now_iso
from .checkpoint import load_checkpoint, save_checkpoint
from . import remax_db as db

# ---------------------------------------------------------------------------
# Sabitler
# ---------------------------------------------------------------------------
BASE_URL = "https://www.remax.com.tr"

CATEGORIES: dict[str, str] = {
    "konut_satilik":         f"{BASE_URL}/konut/satilik",
    "konut_kiralik":         f"{BASE_URL}/konut/kiralik",
    "ticari_kiralik":        f"{BASE_URL}/ticari/kiralik",
    "ticari_satilik":        f"{BASE_URL}/ticari/satilik",
    "ticari_devren-satilik": f"{BASE_URL}/ticari/devren-satilik",
    "ticari_devren-kiralik": f"{BASE_URL}/ticari/devren-kiralik",
}

_DETAIL_LINK_RE     = re.compile(r"remax\.com\.tr/ilan/", re.IGNORECASE)
_DETAIL_FULL_URL_RE = re.compile(r'https?://(?:www\.)?remax\.com\.tr/ilan/[^\s"\' <>&#\\[\\]{}]+', re.IGNORECASE)
_PRICE_RE       = re.compile(r"([\d.,]+)\s*(TL|USD|EUR|₺|\$|€)", re.IGNORECASE)
_LAT_RE         = re.compile(r"\"latitude\"\s*:\s*([-\d.]+)")
_LNG_RE         = re.compile(r"\"longitude\"\s*:\s*([-\d.]+)")
_M2_RE          = re.compile(r"(\d+(?:[.,]\d+)?)\s*m²", re.IGNORECASE)
_LISTING_NO_RE  = re.compile(r"ilan\s*no\s*[:\-]?\s*(\d+)", re.IGNORECASE)

PAGE_LOAD_WAIT  = 12   # saniye


# ---------------------------------------------------------------------------
# Proxy yardımcıları
# ---------------------------------------------------------------------------
def _proxy_auth_extension(proxy_url: str) -> str | None:
    """http://user:pass@host:port formatındaki proxy için Chrome extension ZIP oluşturur.
    Auth gerektirmeyen proxy için None döner.
    """
    m = re.match(r'https?://([^:@]+):([^@]+)@([^:]+):(\d+)', proxy_url)
    if not m:
        return None
    user, password, host, port = m.groups()
    # MV3 — Chrome 127+ MV2 extension'ları devre dışı bırakıyor
    manifest = json.dumps({
        "version": "1.0.0",
        "manifest_version": 3,
        "name": "Proxy Auth",
        "permissions": ["proxy", "webRequest", "webRequestAuthProvider"],
        "host_permissions": ["<all_urls>"],
        "background": {"service_worker": "bg.js"},
    })
    bg = (
        'var config={mode:"fixed_servers",rules:{singleProxy:{scheme:"http",'
        'host:"' + host + '",port:' + str(port) + '},bypassList:["localhost"]}}; '
        'chrome.proxy.settings.set({value:config,scope:"regular"}); '
        'chrome.webRequest.onAuthRequired.addListener('
        'function(details,callback){callback({authCredentials:{username:"' + user + '",password:"' + password + '"}});},'
        '{urls:["<all_urls>"]},'
        '["asyncBlocking"]);'
    )
    ext_path = os.path.join(tempfile.gettempdir(), "remax_proxy_ext.zip")
    with zipfile.ZipFile(ext_path, "w") as zf:
        zf.writestr("manifest.json", manifest)
        zf.writestr("bg.js", bg)
    return ext_path


# ---------------------------------------------------------------------------
# Driver oluşturma
# ---------------------------------------------------------------------------
def create_driver(headless: bool = True, no_images: bool = True, proxy_url: str | None = None) -> uc.Chrome:
    opts = uc.ChromeOptions()
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1280,900")
    opts.add_argument("--lang=tr-TR")
    if no_images:
        prefs = {
            "profile.managed_default_content_settings.images": 2,
            "profile.managed_default_content_settings.stylesheets": 2,
        }
        opts.add_experimental_option("prefs", prefs)
    if proxy_url:
        ext = _proxy_auth_extension(proxy_url)
        if ext:
            # Auth gerektiren proxy: extension ile kimlik doğrulama
            opts.add_extension(ext)
            print(f"[remax] Driver proxy extension yüklendi")
        else:
            # Auth gerektirmeyen proxy: doğrudan --proxy-server
            opts.add_argument(f"--proxy-server={proxy_url}")
            print(f"[remax] Driver proxy: {proxy_url}")
    # Dockerfile'da build-time'da yüklenen chromedriver'ı kullan (versiyon uyuşmazlığını önler)
    # UC binary'yi patch ettiği için yazılabilir bir tmp kopyasına ihtiyaç duyar
    _cd_src = "/usr/local/bin/chromedriver"
    if os.path.exists(_cd_src):
        _tmp_dir = tempfile.mkdtemp()
        _cd_path = os.path.join(_tmp_dir, "chromedriver")
        shutil.copy2(_cd_src, _cd_path)
        os.chmod(_cd_path, 0o755)
        driver = uc.Chrome(options=opts, headless=headless, driver_executable_path=_cd_path)
    else:
        driver = uc.Chrome(options=opts, headless=headless, version_main=146)
    return driver


# ---------------------------------------------------------------------------
# Cloudflare Challenge Bekleme
# ---------------------------------------------------------------------------
def _wait_for_cloudflare(driver, timeout=60):
    """Cloudflare JS challenge sayfasının çözülmesini bekle."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        title = (driver.title or "").lower()
        if "just a moment" not in title:
            return True
        time.sleep(2)
    return False


# ---------------------------------------------------------------------------
# Cookie yardımcıları
# ---------------------------------------------------------------------------
_CF_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
          "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")


def _parse_cookies(cookies_json: str) -> dict:
    """EditThisCookie JSON dizisini {name: value} dict'e çevirir."""
    if not cookies_json:
        return {}
    try:
        items = json.loads(cookies_json)
        result = {c["name"]: c["value"] for c in items if "name" in c and "value" in c}
        print(f"[remax] {len(result)} çerez parse edildi ({', '.join(result.keys())})")
        return result
    except Exception as e:
        print(f"[remax] Cookie parse hatası: {e}")
        return {}


# ---------------------------------------------------------------------------
# HTTP tabanlı CF bypass (curl_cffi / cloudscraper)
# ---------------------------------------------------------------------------
def _http_session():
    """curl_cffi veya cloudscraper oturumu döner; (session, method_name) tuple."""
    try:
        from curl_cffi import requests as cfr  # type: ignore
        sess = cfr.Session(impersonate="chrome124")
        sess.headers.update({
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "tr-TR,tr;q=0.9,en;q=0.8",
        })
        return sess, "curl_cffi"
    except ImportError:
        pass
    try:
        import cloudscraper  # type: ignore
        sess = cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "windows", "mobile": False}
        )
        return sess, "cloudscraper"
    except ImportError:
        return None, None


def _collect_via_http(conn, categories=None, max_pages=0, delay=1.5, cookies: dict | None = None, proxy_url: str | None = None) -> int:
    """curl_cffi / cloudscraper ile tarayıcısız URL toplama."""
    session, method = _http_session()
    if session is None:
        print("[remax] curl_cffi/cloudscraper kurulu değil, Selenium'a geçilecek")
        return 0

    if proxy_url:
        session.proxies = {"http": proxy_url, "https": proxy_url}
        print(f"[remax] HTTP oturumu proxy üzerinden: {proxy_url[:40]}...")

    if cookies:
        session.headers["User-Agent"] = _CF_UA
        session.cookies.update(cookies)
        print(f"[remax] HTTP oturumuna {len(cookies)} çerez enjekte edildi")

    print(f"[remax] HTTP CF bypass başlıyor ({method})")
    target_cats = {k: v for k, v in CATEGORIES.items() if categories is None or k in categories}
    total_added = 0

    for cat_name, base_list_url in target_cats.items():
        page = 1
        zero_streak = 0
        while True:
            url = f"{base_list_url}?sayfa={page}" if page > 1 else base_list_url
            try:
                resp = session.get(url, timeout=30)
                html = resp.text

                if resp.status_code != 200 or "just a moment" in html[:2000].lower():
                    print(f"[remax:{cat_name}] {method}: CF engeli (HTTP {resp.status_code})")
                    break

                links = list(dict.fromkeys(_DETAIL_FULL_URL_RE.findall(html)))
                print(f"[remax:{cat_name}] Sayfa {page}: {len(links)} ilan ({method})")

                if not links:
                    zero_streak += 1
                    if page == 1:
                        print(f"[remax:{cat_name}] İlk sayfada link yok — SPA/CSR olabilir, Selenium gerekebilir")
                    if zero_streak >= 2:
                        break
                else:
                    zero_streak = 0
                    url_objs = [db.RemaxListingUrl(url=l, category=cat_name, page_found=page) for l in links]
                    total_added += db.bulk_upsert_listing_urls(conn, url_objs)

            except Exception as exc:
                print(f"[remax:{cat_name}] HTTP hata sayfa {page}: {exc}")
                break

            if max_pages > 0 and page >= max_pages:
                break
            page += 1
            time.sleep(delay)

    return total_added


# ---------------------------------------------------------------------------
# Aşama 1: URL Toplama
# ---------------------------------------------------------------------------
def collect_listing_urls(
    driver: uc.Chrome,
    conn,
    categories: Optional[list[str]] = None,
    max_pages: int = 0,
    delay: float = 1.5,
    cookies: dict | None = None,
    proxy_url: str | None = None,
) -> int:
    """Kategori sayfalarını tarayarak ilan URL'lerini remax_db'ye yaz.

    Önce tarayıcısız HTTP (curl_cffi) dener; CF geçilemezse Selenium'a geçer.
    Döndürdüğü: toplam yeni eklenen URL sayısı
    """
    # --- 1. HTTP yaklaşımı (curl_cffi / cloudscraper) ---
    http_count = _collect_via_http(conn, categories, max_pages, delay, cookies=cookies, proxy_url=proxy_url)
    if http_count > 0:
        print(f"[remax] HTTP ile {http_count} URL toplandı — Selenium atlanıyor")
        return http_count

    # --- 2. Selenium fallback ---
    print("[remax] HTTP yöntemi URL toplamadı — Selenium (UC+Xvfb) ile deneniyor")
    target_cats = {k: v for k, v in CATEGORIES.items() if categories is None or k in categories}
    total_added = 0

    for cat_name, base_list_url in target_cats.items():
        page = 1
        while True:
            url = f"{base_list_url}?sayfa={page}" if page > 1 else base_list_url
            try:
                driver.get(url)
                WebDriverWait(driver, PAGE_LOAD_WAIT).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                # Cloudflare challenge varsa çözülmesini bekle
                if not _wait_for_cloudflare(driver, timeout=20):
                    print(f"[remax:{cat_name}] Sayfa {page}: Cloudflare aşılamadı — kategori atlanıyor")
                    break
                WebDriverWait(driver, PAGE_LOAD_WAIT).until(
                    lambda d: d.execute_script("return document.readyState") == "complete"
                )
                time.sleep(3)  # JS kartlarının render edilmesi için
            except TimeoutException:
                print(f"[remax:{cat_name}] Sayfa {page}: sayfa yüklenemedi (timeout) — atlanıyor")
                break

            _ilan_hrefs = [
                el.get_attribute("href") or ""
                for el in driver.find_elements(By.CSS_SELECTOR, "a[href]")
                if _DETAIL_LINK_RE.search(el.get_attribute("href") or "")
            ]
            links = list(dict.fromkeys(_ilan_hrefs))
            print(f"[remax:{cat_name}] Sayfa {page}: {len(links)} ilan linki (title={driver.title!r})")
            if not links:
                print(f"[remax:{cat_name}] Sayfa {page}: ilan linki bulunamadı — sonraki sayfaya bak")

            url_objs = [db.RemaxListingUrl(url=l, category=cat_name, page_found=page) for l in links]
            added = db.bulk_upsert_listing_urls(conn, url_objs)
            total_added += added

            # Sonraki sayfa var mı?
            has_next = False
            try:
                next_btn = driver.find_element(By.CSS_SELECTOR, "a.pagination-next, li.next > a")
                has_next = next_btn.is_enabled() and next_btn.get_attribute("href") is not None
            except NoSuchElementException:
                pass

            if not has_next or (max_pages > 0 and page >= max_pages):
                break

            page += 1
            time.sleep(delay)

    return total_added


# ---------------------------------------------------------------------------
# Aşama 2: Detay Scraping
# ---------------------------------------------------------------------------
def _safe_text(driver: uc.Chrome, selector: str, by: str = By.CSS_SELECTOR) -> str:
    try:
        return driver.find_element(by, selector).text.strip()
    except NoSuchElementException:
        return ""


def _get_meta(driver: uc.Chrome, name_or_prop: str) -> str:
    for attr in ("name", "property", "itemprop"):
        try:
            el = driver.find_element(
                By.CSS_SELECTOR, f'meta[{attr}="{name_or_prop}"]'
            )
            return (el.get_attribute("content") or "").strip()
        except NoSuchElementException:
            continue
    return ""


def scrape_detail(driver: uc.Chrome, url: str) -> Optional[CanonicalListing]:
    try:
        driver.get(url)
        _wait_for_cloudflare(driver, timeout=15)
        WebDriverWait(driver, PAGE_LOAD_WAIT).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "h1, .detail-title"))
        )
    except TimeoutException:
        return None

    page_src = driver.page_source

    # Başlık
    title = (
        _safe_text(driver, "h1.detail-title")
        or _safe_text(driver, "h1")
        or _get_meta(driver, "og:title")
    )

    # İlan No
    listing_no = ""
    m = _LISTING_NO_RE.search(page_src)
    if m:
        listing_no = m.group(1)

    # Fiyat
    price = ""
    currency = ""
    price_raw = (_safe_text(driver, ".price, .detail-price, [class*='price']")
                 or _get_meta(driver, "price"))
    pm = _PRICE_RE.search(price_raw or page_src[:2000])
    if pm:
        price    = pm.group(1).replace(".", "").replace(",", "")
        currency = pm.group(2).replace("₺", "TL").replace("$", "USD").replace("€", "EUR").upper()

    # Özellikler tablosu / liste
    features: dict[str, str] = {}
    for row in driver.find_elements(By.CSS_SELECTOR, "li.detail-spec, tr.spec-row, .property-list li"):
        spans  = row.find_elements(By.TAG_NAME, "span")
        labels = row.find_elements(By.TAG_NAME, "strong")
        if len(spans) >= 2:
            k, v = spans[0].text.strip().lower(), spans[1].text.strip()
        elif labels and spans:
            k, v = labels[0].text.strip().lower(), spans[0].text.strip()
        else:
            text = row.text.strip()
            if ":" in text:
                k, v = (t.strip() for t in text.split(":", 1))
                k = k.lower()
            else:
                continue
        if k and v:
            features[k] = v

    def _feat(*keys: str) -> str:
        for k in keys:
            for fk, fv in features.items():
                if k in fk:
                    return fv
        return ""

    # Konum
    location     = _get_meta(driver, "og:street-address") or _feat("il", "şehir", "city")
    district     = _feat("ilçe", "district")
    neighborhood = _feat("mahalle", "neighborhood")

    # m2
    m2_brut = _feat("brüt", "gross")
    m2_net  = _feat("net")
    if not m2_brut and not m2_net:
        mm = _M2_RE.search(page_src[:3000])
        if mm:
            m2_net = mm.group(1)

    # Diğer
    room_count   = _feat("oda", "room")
    floor        = _feat("bulunduğu kat", "kat (bulunduğu")
    total_floors = _feat("toplam kat", "bina kat")
    build_year   = _feat("yapı yaşı", "bina yaşı", "yıl", "year")
    heating      = _feat("ısıtma", "heating")
    property_type = _feat("emlak tipi", "konut tipi", "ticaret tipi", "property type")

    # Açıklama
    description = _safe_text(driver, ".detail-description, .description-text, #description")

    # Koordinatlar
    latitude = longitude = None
    lat_m = _LAT_RE.search(page_src)
    lng_m = _LNG_RE.search(page_src)
    if lat_m and lng_m:
        try:
            latitude  = float(lat_m.group(1))
            longitude = float(lng_m.group(1))
        except ValueError:
            pass

    # Kategori bilgisi URL'den çıkar
    category = "bilinmiyor"
    for cat_key in CATEGORIES:
        cat_path = cat_key.replace("_", "/", 1)
        if cat_path in url.lower():
            category = cat_key
            break

    return CanonicalListing(
        source        = "remax",
        url           = url,
        title         = title,
        listing_no     = listing_no,
        product_id    = listing_no,
        category      = category,
        transaction_type = "kiralik" if "kiralik" in category else "satilik",
        property_type = property_type,
        price         = price,
        currency      = currency,
        location      = location,
        district      = district,
        neighborhood  = neighborhood,
        m2_net        = m2_net,
        m2_brut       = m2_brut,
        room_count    = room_count,
        floor         = floor,
        total_floors  = total_floors,
        build_year    = build_year,
        heating       = heating,
        description   = description,
        latitude      = latitude,
        longitude     = longitude,
        scraped_at_utc = utc_now_iso(),
    )


# ---------------------------------------------------------------------------
# Ana Scraping Fonksiyonu
# ---------------------------------------------------------------------------
def scrape_all(
    db_path: Path,
    checkpoint_path: Optional[Path] = None,
    categories: Optional[list[str]] = None,
    max_pages: int = 0,
    delay: float = 1.5,
    headless: bool = True,
    on_item: Optional[Callable[[CanonicalListing], None]] = None,
    cookies_json: str = "",
    proxy_url: str | None = None,
) -> None:
    """
    Remax scraper'ı iki aşamalı çalıştırır.
    checkpoint_path: JSON shadow checkpoint (runs arası KV Store kalıcılığı için)
    on_item: her ilan için çağrılır (Apify push_data vb.)
    cookies_json: EditThisCookie JSON dizisi (cf_clearance vb.) — CF bypass için
    proxy_url: http://user:pass@host:port — Residential proxy CF bypass için
    """
    conn = db.connect(db_path)
    db.ensure_schema(conn)
    _cookies = _parse_cookies(cookies_json)

    # JSON checkpoint'ten önceki run verilerini SQLite'a yükle
    if checkpoint_path is not None:
        cp = load_checkpoint(checkpoint_path)
        collected_cp: list[list[str]] = cp.get("collected", [])
        done_cp: list[str] = cp.get("done_urls", [])
        if collected_cp:
            url_objs = [db.RemaxListingUrl(url=u, category=c) for u, c in collected_cp]
            db.bulk_upsert_listing_urls(conn, url_objs)
            for u in done_cp:
                db.mark_url_status(conn, u, "done")
            print(f"[remax] Checkpoint'ten {len(collected_cp)} URL yüklendi ({len(done_cp)} tamamlanmış).")

    def _save_cp() -> None:
        if checkpoint_path is None:
            return
        all_rows = conn.execute("SELECT url, category FROM remax_listing_urls ORDER BY id").fetchall()
        done_rows = conn.execute("SELECT url FROM remax_listing_urls WHERE status='done'").fetchall()
        save_checkpoint(checkpoint_path, {
            "collected": [[r[0], r[1]] for r in all_rows],
            "done_urls": [r[0] for r in done_rows],
        })

    # Cloudflare bypass: virtual display varsa headed mode, yoksa UC headless
    display = None
    use_headless = headless
    try:
        from pyvirtualdisplay import Display
        display = Display(visible=False, size=(1280, 900))
        display.start()
        use_headless = False
        print("[remax] Virtual display aktif — headed mode (CF bypass)")
    except Exception:
        print("[remax] Xvfb yok — UC headless modda devam")

    driver = create_driver(headless=use_headless, proxy_url=proxy_url)
    try:
        # --- Cookie Enjeksiyonu ---
        if _cookies:
            try:
                driver.get("https://www.remax.com.tr")
                _wait_for_cloudflare(driver, timeout=5)
                for name, value in _cookies.items():
                    try:
                        driver.add_cookie({"name": name, "value": value, "domain": ".remax.com.tr", "path": "/"})
                    except Exception:
                        pass
                print(f"[remax] Selenium driver'a {len(_cookies)} çerez enjekte edildi")
            except Exception as ce:
                print(f"[remax] Cookie enjeksiyon hatası: {ce}")

        # --- Aşama 1: URL Toplama ---
        stats = db.get_stats(conn)
        pending_before = stats["pending"]
        collecting_done = stats["total_urls"] > 0
        if not collecting_done:
            print("[remax] Aşama 1: URL toplanıyor...")
            added = collect_listing_urls(driver, conn, categories, max_pages, delay, cookies=_cookies, proxy_url=proxy_url)
            print(f"[remax] {added} yeni URL eklendi.")
            _save_cp()
        else:
            print(f"[remax] {pending_before} bekleyen URL mevcut, Aşama 2'ye geçiliyor.")

        # --- Aşama 2: Detay Scraping ---
        pending_urls = db.get_pending_urls(conn, categories)
        print(f"[remax] Aşama 2: {len(pending_urls)} ilan scrape edilecek.")

        done_count = 0
        for url, cat in pending_urls:
            try:
                listing = scrape_detail(driver, url)
                if listing:
                    listing.category = cat
                    db.upsert_listing(conn, db.RemaxListing(
                        url           = listing.url,
                        category      = listing.category,
                        listing_no     = listing.listing_no,
                        title         = listing.title,
                        property_type = listing.property_type,
                        price         = listing.price,
                        currency      = listing.currency,
                        location      = listing.location,
                        district      = listing.district,
                        neighborhood  = listing.neighborhood,
                        m2_brut       = listing.m2_brut,
                        m2_net        = listing.m2_net,
                        room_count    = listing.room_count,
                        floor         = listing.floor,
                        total_floors  = listing.total_floors,
                        build_year    = listing.build_year,
                        heating       = listing.heating,
                        description   = listing.description,
                        latitude      = listing.latitude,
                        longitude     = listing.longitude,
                    ))
                    db.mark_url_status(conn, url, "done")
                    done_count += 1
                    if done_count % 25 == 0:
                        _save_cp()
                    if on_item:
                        on_item(listing)
                else:
                    db.mark_url_status(conn, url, "error")
            except Exception as exc:
                print(f"[remax] HATA {url}: {exc}")
                db.mark_url_status(conn, url, "error")
            time.sleep(delay)

        _save_cp()
    finally:
        driver.quit()
        if display:
            display.stop()
        conn.close()
