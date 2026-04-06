"""
turyap_scraper.py — Turyap.com.tr için Selenium tabanlı ilan toplayıcı.

Site, ASP.NET WebForms PostBack (__doPostBack) ile sayfalama yapar.
"""
from __future__ import annotations

import re
import time
from pathlib import Path
from typing import Callable, Optional

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException, ElementClickInterceptedException, StaleElementReferenceException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from .checkpoint import load_checkpoint, save_checkpoint
from .models import CanonicalListing, utc_now_iso

# ---------------------------------------------------------------------------
# Sabitler
# ---------------------------------------------------------------------------
BASE_URL      = "https://www.turyap.com.tr/Portfoyler.aspx"
DETAIL_BASE   = "https://www.turyap.com.tr/Portfoy_Bilgileri.aspx"
PRODUCT_ID_RE = re.compile(r"ProductID=(\d+)", re.IGNORECASE)
PRICE_RE      = re.compile(r"([\d.,]+)\s*(TL|USD|EUR|₺|\$|€)", re.IGNORECASE)
LAT_RE        = re.compile(r"data-lat=\"([-\d.]+)\"")
LNG_RE        = re.compile(r"data-lng=\"([-\d.]+)\"")
COORD_JS_RE   = re.compile(r"new\s+google\.maps\.LatLng\(([-\d.]+)\s*,\s*([-\d.]+)\)")

PAGE_LOAD_WAIT = 15


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------
def create_driver(headless: bool = True) -> webdriver.Chrome:
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1280,900")
    opts.add_argument("--lang=tr-TR")
    opts.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
    prefs = {"profile.managed_default_content_settings.images": 2}
    opts.add_experimental_option("prefs", prefs)
    driver = webdriver.Chrome(options=opts)
    driver.execute_cdp_cmd(
        "Page.addScriptToEvaluateOnNewDocument",
        {"source": "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"},
    )
    return driver


# ---------------------------------------------------------------------------
# Aşama 1: URL Toplama
# ---------------------------------------------------------------------------
def collect_listing_urls(
    driver: webdriver.Chrome,
    max_pages: int = 0,
    delay: float = 2.0,
) -> list[str]:
    """Tüm ilan detay URL'lerini toplar; ProductID sorgu parametresi içeren linkler."""
    driver.get(BASE_URL)
    time.sleep(delay)

    all_urls: list[str] = []
    seen_hrefs: set[str] = set()
    page_num = 1
    zero_new_streak = 0  # arka arkaya yeni URL gelmeyen sayfa sayısı

    while True:
        # Sayfadaki tüm ilan linkleri
        try:
            WebDriverWait(driver, PAGE_LOAD_WAIT).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, f"a[href*='ProductID']"))
            )
        except TimeoutException:
            break

        links = driver.execute_script(
            """
            return Array.from(
                document.querySelectorAll("a[href*='Portfoy_Bilgileri.aspx?ProductID=']")
            ).map(a => a.href);
            """
        ) or []

        new_count = 0
        for href in links:
            if href and href not in seen_hrefs:
                seen_hrefs.add(href)
                all_urls.append(href)
                new_count += 1

        print(f"[turyap] Sayfa {page_num}: {new_count} yeni URL ({len(all_urls)} toplam)")

        if new_count == 0:
            zero_new_streak += 1
            if zero_new_streak >= 3:
                print("[turyap] 3 sayfada yeni URL yok — toplama tamamlandı.")
                break
        else:
            zero_new_streak = 0

        if max_pages > 0 and page_num >= max_pages:
            break

        # Sonraki sayfa: native Selenium click ile PostBack tetikliyoruz
        # execute_script("arguments[0].click()") Chrome 146'da PostBack tetiklemiyor;
        # btn.click() WebDriver protokolü üzerinden gerçek mouse event gönderiyor.
        moved = False
        for selector in [
            "#ContentPlaceHolder1_lbNext",
            "a[id*='lbNext']",
            "a[href*=\"__doPostBack\"][id*='Next']",
        ]:
            try:
                btn = driver.find_element(By.CSS_SELECTOR, selector)
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
                time.sleep(0.3)
                btn.click()  # native click — onclick="__doPostBack(...)" handler'ı tetikler
                time.sleep(delay)
                page_num += 1
                moved = True
                break
            except (NoSuchElementException, ElementClickInterceptedException, StaleElementReferenceException):
                continue

        if not moved:
            break

    return all_urls


# ---------------------------------------------------------------------------
# Detay Parse
# ---------------------------------------------------------------------------
def parse_detail_html(html: str) -> dict[str, str]:
    """HTML kaynağından ilan özelliklerini çıkarır."""
    features: dict[str, str] = {}

    # <li>Anahtar: <strong><span>Değer</span></strong></li>
    pattern = re.compile(
        r"<li[^>]*>\s*([^<:]+):\s*<strong[^>]*>\s*<span[^>]*>([^<]+)</span>",
        re.IGNORECASE | re.DOTALL,
    )
    for m in pattern.finditer(html):
        k = re.sub(r"\s+", " ", m.group(1)).strip().lower()
        v = re.sub(r"\s+", " ", m.group(2)).strip()
        if k and v:
            features[k] = v

    # <span class="...label...">Anahtar</span><span ...>Değer</span>
    label_val = re.compile(
        r'<span[^>]*class="[^"]*(?:label|baslik|key)[^"]*"[^>]*>([^<]+)</span>\s*'
        r'<span[^>]*>([^<]+)</span>',
        re.IGNORECASE,
    )
    for m in label_val.finditer(html):
        k = m.group(1).strip().lower()
        v = m.group(2).strip()
        if k and v and k not in features:
            features[k] = v

    return features


def _feat(features: dict[str, str], *keys: str) -> str:
    for k in keys:
        for fk, fv in features.items():
            if k in fk:
                return fv
    return ""


def scrape_detail(driver: webdriver.Chrome, url: str) -> Optional[CanonicalListing]:
    try:
        driver.get(url)
        WebDriverWait(driver, PAGE_LOAD_WAIT).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "h1, .portfoy-baslik"))
        )
    except TimeoutException:
        return None

    html = driver.page_source

    # ProductID
    pm = PRODUCT_ID_RE.search(url)
    product_id = pm.group(1) if pm else ""

    # Başlık
    title = ""
    for sel in ["h1.portfoy-baslik", "h1", ".portfoy-title h1"]:
        try:
            title = driver.find_element(By.CSS_SELECTOR, sel).text.strip()
            if title:
                break
        except NoSuchElementException:
            pass

    # Özellikler
    features = parse_detail_html(html)

    # Fiyat
    price = currency = ""
    pr = PRICE_RE.search(
        _feat(features, "fiyat", "price")
        or driver.find_element(By.CSS_SELECTOR, ".fiyat, .price, [class*='price']").text
        if _feat(features, "fiyat") else html[:3000]
    )
    if pr:
        price    = pr.group(1).replace(".", "").replace(",", "")
        currency = pr.group(2).replace("₺", "TL").replace("$", "USD").replace("€", "EUR").upper()

    # İlan No
    listing_no = _feat(features, "ilan no", "portfoy no", "listing no") or product_id

    # Konum
    location     = _feat(features, "il", "şehir", "city") or _feat(features, "konum", "location")
    district     = _feat(features, "ilçe", "district")
    neighborhood = _feat(features, "mahalle", "neighborhood")

    # Alan
    m2_brut = _feat(features, "brüt", "gross")
    m2_net  = _feat(features, "net m²", "net alan", "net")
    room_count    = _feat(features, "oda", "room")
    floor         = _feat(features, "bulunduğu kat", "kat (")
    total_floors  = _feat(features, "toplam kat", "bina kat")
    build_year    = _feat(features, "yapım yılı", "inşaat yılı", "bina yaşı", "yapı yaşı")
    heating       = _feat(features, "ısıtma", "heating")
    property_type = _feat(features, "emlak tipi", "konut tipi", "gayrimenkul tipi", "portfoy tipi")

    # İşlem tipi
    transaction_type = "bilinmiyor"
    for kw in ("satılık", "satilik", "sale"):
        if kw in html[:5000].lower():
            transaction_type = "satilik"
            break
    for kw in ("kiralık", "kiralik", "rent"):
        if kw in html[:5000].lower():
            transaction_type = "kiralik"
            break

    # Açıklama
    description = ""
    for sel in [".portfoy-aciklama", ".aciklama", "#description", ".description"]:
        try:
            description = driver.find_element(By.CSS_SELECTOR, sel).text.strip()
            if description:
                break
        except NoSuchElementException:
            pass

    # Koordinatlar
    latitude = longitude = None
    lat_m = LAT_RE.search(html) or COORD_JS_RE.search(html)
    if lat_m:
        try:
            if len(lat_m.groups()) >= 2:
                latitude  = float(lat_m.group(1))
                longitude = float(lat_m.group(2))
            else:
                lng_m = LNG_RE.search(html)
                if lng_m:
                    latitude  = float(lat_m.group(1))
                    longitude = float(lng_m.group(1))
        except ValueError:
            pass

    return CanonicalListing(
        source           = "turyap",
        url              = url,
        title            = title,
        listing_no        = listing_no,
        product_id       = product_id,
        category         = property_type or "bilinmiyor",
        transaction_type = transaction_type,
        property_type    = property_type,
        price            = price,
        currency         = currency,
        location         = location,
        district         = district,
        neighborhood     = neighborhood,
        m2_net           = m2_net,
        m2_brut          = m2_brut,
        room_count       = room_count,
        floor            = floor,
        total_floors     = total_floors,
        build_year       = build_year,
        heating          = heating,
        description      = description,
        latitude         = latitude,
        longitude        = longitude,
        scraped_at_utc   = utc_now_iso(),
    )


# ---------------------------------------------------------------------------
# Ana Scraping Fonksiyonu
# ---------------------------------------------------------------------------
def scrape_all(
    csv_path: Path,
    checkpoint_path: Path,
    max_pages: int = 0,
    delay: float = 2.0,
    headless: bool = True,
    on_item: Optional[Callable[[CanonicalListing], None]] = None,
) -> None:
    checkpoint = load_checkpoint(checkpoint_path)
    done_urls: set[str] = set(checkpoint.get("done_urls", []))
    collected: list[str] = checkpoint.get("collected", [])
    collecting_done: bool = checkpoint.get("collecting_done", False)

    driver = create_driver(headless=headless)
    try:
        # Aşama 1: URL Toplama
        if not collecting_done:
            print("[turyap] Aşama 1: ilan URL'leri toplanıyor...")
            collected = collect_listing_urls(driver, max_pages=max_pages, delay=delay)
            checkpoint["collected"]      = collected
            checkpoint["collecting_done"] = True
            save_checkpoint(checkpoint_path, checkpoint)
            print(f"[turyap] {len(collected)} URL toplandı.")

        # Aşama 2: Detay Scraping
        pending = [u for u in collected if u not in done_urls]
        print(f"[turyap] Aşama 2: {len(pending)} ilan scrape edilecek.")

        for url in pending:
            try:
                listing = scrape_detail(driver, url)
                if listing:
                    done_urls.add(url)
                    checkpoint["done_urls"] = list(done_urls)
                    save_checkpoint(checkpoint_path, checkpoint)
                    if on_item:
                        on_item(listing)
            except Exception as exc:
                print(f"[turyap] HATA {url}: {exc}")
            time.sleep(delay)

    finally:
        driver.quit()
