"""emlakjet_scraper.py

Emlakjet.com'dan satılık/kiralık konut ve işyeri ilanlarını çeker.
Liste sayfaları Selenium ile yüklenir; ilan kartları DOM'dan çıkarılır.

Kullanım:
  python emlakjet_scraper.py                                   # Tüm kategoriler
  python emlakjet_scraper.py --categories satilik_konut        # Sadece bir kategori
  python emlakjet_scraper.py --workers 2 --max-pages 50        # 50 sayfa, 2 worker
  python emlakjet_scraper.py --headless false                  # Görünür tarayıcı
  python emlakjet_scraper.py --probe                           # Test modu (5 sayfa)
"""
from __future__ import annotations

import argparse
import atexit
import csv
import json
import random
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Any, Optional

from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait

# ---------------------------------------------------------------------------
# Sabitler
# ---------------------------------------------------------------------------

BASE_URL = "https://www.emlakjet.com"

# Scrape edilecek kategoriler: isim -> base URL
CATEGORIES: dict[str, str] = {
    "satilik_konut":  BASE_URL + "/satilik-konut",
    "kiralik_konut":  BASE_URL + "/kiralik-konut",
    "satilik_isyeri": BASE_URL + "/satilik-isyeri",
    "kiralik_isyeri": BASE_URL + "/kiralik-isyeri",
    "devren_isyeri":  BASE_URL + "/devren-isyeri",
    "satilik_arsa":   BASE_URL + "/satilik-arsa",
    "kiralik_arsa":   BASE_URL + "/kiralik-arsa",
}

TRADE_TYPE_MAP: dict[str, str] = {
    "satilik_konut":  "satilik",
    "kiralik_konut":  "kiralik",
    "satilik_isyeri": "satilik",
    "kiralik_isyeri": "kiralik",
    "devren_isyeri":  "devren",
    "satilik_arsa":   "satilik",
    "kiralik_arsa":   "kiralik",
}

COLUMNS = [
    "url",
    "listing_id",
    "title",
    "location",
    "district",
    "neighborhood",
    "city",
    "lat",
    "lon",
    "category",
    "trade_type",
    "price",
    "currency",
    "prev_price",
    "room_count",
    "floor",
    "gross_sqm",
    "estate_type",
    "quick_infos",
    "image_url",
    "scraped_at_utc",
    # ── Detay sayfası alanları (scrapeDetails=True ise dolu) ───────────────────────
    "is_jet_firsat",
    "price_est_min",
    "price_est_max",
    "price_discount_pct",
    "region_avg_rental",
    "region_avg_sale",
    "region_return_years",
    "region_value_change_1y",
    "transport_nearby",
    "education_nearby",
    "market_nearby",
    "cafe_restaurant_nearby",
    "health_nearby",
]

DEFAULT_PER_PAGE = 20  # Varsayılan sayfa başı ilan sayısı
DATA_DIR = Path("data")

# ---------------------------------------------------------------------------
# Thread-local Chrome driver yönetimi
# ---------------------------------------------------------------------------

_THREAD_LOCAL = threading.local()
_DRIVER_REGISTRY: list[webdriver.Chrome] = []
_DRIVER_REGISTRY_LOCK = threading.Lock()

# Eş zamanlı açık Chrome sayısını sınırlar (None = sınırsız)
_CHROME_SEM: Optional[threading.BoundedSemaphore] = None
# Başlatma sırasında Chrome'ları dağıtmak için zaman blokları
_STARTUP_LOCK = threading.Lock()
_STARTUP_COUNTER = 0
_STARTUP_DELAY_PER_DRIVER = 0.3  # saniye


def init_chrome_semaphore(limit: int) -> None:
    """Eş zamanlı açık Chrome instance sayısını sınırla."""
    global _CHROME_SEM
    _CHROME_SEM = threading.BoundedSemaphore(limit)
    print(f"[EMLAKJET] Chrome semaforu başlatıldı: maks {limit} eş zamanlı instance")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def clean_text(value: Any) -> str:
    if not value:
        return ""
    text = re.sub(r"<[^>]+>", " ", str(value))
    text = unescape(text)
    text = text.replace("\xa0", " ").replace("\u200b", "")
    return re.sub(r"\s+", " ", text).strip()


def _atomic_write(path: Path, text: str) -> None:
    """Önce .tmp dosyasına yaz, sonra atomik rename."""
    tmp = path.with_suffix(".tmp")
    tmp.parent.mkdir(parents=True, exist_ok=True)
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


# ---------------------------------------------------------------------------
# Chrome driver
# ---------------------------------------------------------------------------

def create_driver(headless: bool = True, no_images: bool = True, proxy_url: str = "") -> webdriver.Chrome:
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
        opts.add_argument("--start-maximized")

    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-plugins")
    opts.add_argument("--disable-background-networking")
    opts.add_argument("--disable-default-apps")
    opts.add_argument("--disable-sync")
    # Renderer stabilitesi: arka plan Chrome'larının throttle edilmesini engelle
    opts.add_argument("--disable-renderer-backgrounding")
    opts.add_argument("--disable-backgrounding-occluded-windows")
    opts.add_argument("--disable-ipc-flooding-protection")
    opts.add_argument("--disable-translate")
    opts.add_argument("--hide-scrollbars")
    opts.add_argument("--mute-audio")

    if no_images:
        opts.add_experimental_option(
            "prefs",
            {"profile.managed_default_content_settings.images": 2},
        )

    if proxy_url:
        # Chrome --proxy-server URL'de kimlik bilgisi (user:pass@) desteklemez;
        # Apify proxy URL'inden sadece scheme://host:port kısmını al.
        try:
            from urllib.parse import urlparse as _up
            _p = _up(proxy_url)
            _clean = f"{_p.scheme}://{_p.hostname}:{_p.port}"
            opts.add_argument(f"--proxy-server={_clean}")
        except Exception:
            opts.add_argument(f"--proxy-server={proxy_url}")

    # Başlatmaları zaman içine yay (aynı anda 140 Chrome açılmasını engelle)
    global _STARTUP_COUNTER
    with _STARTUP_LOCK:
        delay_slot = _STARTUP_COUNTER
        _STARTUP_COUNTER += 1
    if delay_slot > 0:
        time.sleep(min(delay_slot * _STARTUP_DELAY_PER_DRIVER, 15.0))

    last_err: Optional[Exception] = None
    for attempt in range(3):
        try:
            driver = webdriver.Chrome(options=opts)
            driver.set_page_load_timeout(45)
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
            print(f"[EMLAKJET] Driver baslatma hatasi ({attempt + 1}/3): {exc}")
            time.sleep(2 + attempt)
    raise RuntimeError(f"Chrome driver baslatilamadi: {last_err}")


def _cleanup_drivers() -> None:
    with _DRIVER_REGISTRY_LOCK:
        drivers = list(_DRIVER_REGISTRY)
        _DRIVER_REGISTRY.clear()
    for drv in drivers:
        try:
            drv.quit()
        except Exception:
            pass


atexit.register(_cleanup_drivers)

# ---------------------------------------------------------------------------
# Sayfa yardımcıları
# ---------------------------------------------------------------------------

def listing_page_url(base_url: str, page: int) -> str:
    """Sayfalama URL'i oluştur. Türkçe 'sayfa' parametresi kullanır."""
    if page <= 1:
        return base_url
    return f"{base_url}?sayfa={page}"


def wait_for_listings(driver: webdriver.Chrome, timeout: int = 35) -> bool:
    """İlan kartlarının DOM'a yüklenmesini bekle.
    Gerçek DOM selektörü: div[data-id] (id^=listingItem- değil)
    """
    try:
        WebDriverWait(driver, timeout).until(
            lambda d: d.execute_script(
                "return document.querySelectorAll('[data-id]').length > 0"
            )
        )
        return True
    except TimeoutException:
        return False


def get_total_count(driver: webdriver.Chrome) -> int:
    """Sayfadaki toplam ilan sayısını çıkar."""
    try:
        count_text = driver.execute_script(r"""
            // FAQ-Page JSON-LD'den ilan sayısını bul (server-rendered)
            var scripts = document.querySelectorAll('script[type="application/ld+json"]');
            for (var i=0; i<scripts.length; i++) {
                try {
                    var d = JSON.parse(scripts[i].textContent);
                    // FAQPage tipinde, ilk sorunun cevabında fiyat bilgisi bulunuyor
                    if (d['@type'] === 'FAQPage') continue;
                } catch(e) {}
            }
            // h1 veya h2 içindeki ilan sayısı
            var els = document.querySelectorAll('h1, h2, [class*="adsCount"], [class*="adCount"], [class*="counter"]');
            for (var i=0; i<els.length; i++) {
                var t = els[i].textContent;
                var m = t.match(/([\d.]+)\s*ilan/);
                if (m) return m[1];
            }
            // Sayfa meta description'ından dene
            var meta = document.querySelector('meta[name="description"]');
            if (meta) {
                var m = meta.content.match(/([\d.]+)\s*ilan/);
                if (m) return m[1];
            }
            return null;
        """)
        if count_text:
            nums = re.findall(r"[\d.]+", str(count_text))
            if nums:
                return int(nums[0].replace(".", ""))
    except Exception:
        pass
    return 0


# ---------------------------------------------------------------------------
# DOM'dan ilan kartı çıkarma
# ---------------------------------------------------------------------------

_JS_EXTRACT_CARDS = r"""
return (function() {
    // __NEXT_DATA__ (Next.js) üzerinden koordinat ve adres bilgisi topla
    var coordMap = {};
    try {
        var nd = document.getElementById('__NEXT_DATA__');
        if (nd) {
            var ndata = JSON.parse(nd.textContent);
            var pageProps = ((ndata.props || {}).pageProps) || {};
            var items = (
                ((pageProps.searchResult || {}).items) ||
                ((pageProps.searchResult || {}).ads) ||
                pageProps.listings ||
                pageProps.ads ||
                pageProps.items ||
                []
            );
            for (var idx = 0; idx < items.length; idx++) {
                var item = items[idx];
                var itemId = String(item.id || item.listingId || item.adId || '');
                if (itemId) {
                    var coords = item.coordinates || item.location || {};
                    coordMap[itemId] = {
                        lat: String(item.latitude || item.lat || coords.latitude || coords.lat || ''),
                        lon: String(item.longitude || item.lng || item.lon || coords.longitude || coords.lon || coords.lng || ''),
                        neighborhood: String(item.neighborhood || item.mahalle || (item.address || {}).neighborhood || ''),
                        district: String(item.district || item.ilce || (item.address || {}).district || ''),
                        city: String(item.city || item.province || item.sehir || (item.address || {}).city || ''),
                        image_url: String(((item.photos || [])[0]) || item.imageUrl || item.image || item.coverPhotoUrl || '')
                    };
                }
            }
        }
    } catch(e) {}

    var containers = document.querySelectorAll('[data-id]');
    var results = [];
    for (var i = 0; i < containers.length; i++) {
        try {
            var c = containers[i];
            var a = c.querySelector('a[href*="/ilan/"]');
            if (!a) continue;

            var id = c.getAttribute('data-id') || '';
            var url = a.href || '';

            // Başlık: h3 element (class*=title)
            var h3 = a.querySelector('h3');
            var title = h3 ? h3.textContent.trim() : '';

            // Konum: span[class*=location]
            var locEl = a.querySelector('span[class*="location"]');
            var location = locEl ? locEl.textContent.trim() : '';

            // Hızlı bilgiler: "Daire | 3+1 | 1. Kat | 110 m²"
            var qiEl = a.querySelector('div[class*="quickinfo"]');
            var quickInfoText = qiEl ? qiEl.textContent.trim() : '';

            // Güncel fiyat: span[class*="styles_price__"] (previousPrice değil)
            var priceEl = a.querySelector('span[class*="styles_price__"]');
            var price = priceEl ? priceEl.textContent.trim() : '';

            // Önceki fiyat (indirimli ilanlar)
            var prevPriceEl = a.querySelector('span[class*="previousPrice"]');
            var prevPrice = prevPriceEl ? prevPriceEl.textContent.trim() : '';

            // Görsel (liste kartı img)
            var imgEl = c.querySelector('img');
            var domImage = imgEl ? (imgEl.src || imgEl.getAttribute('data-src') || '') : '';

            // coordMap'ten koordinat / adres
            var co = coordMap[id] || {};

            results.push({
                id: id,
                url: url,
                title: title,
                location: location,
                quickInfoText: quickInfoText,
                price: price,
                prevPrice: prevPrice,
                lat: co.lat || '',
                lon: co.lon || '',
                neighborhood: co.neighborhood || '',
                district: co.district || '',
                city: co.city || '',
                image_url: co.image_url || domImage
            });
        } catch(e) {}
    }
    return results;
})();
"""


def extract_card_data(driver: webdriver.Chrome) -> list[dict]:
    """Mevcut sayfadaki tüm ilan kartlarını JavaScript ile çıkar."""
    try:
        raw = driver.execute_script(_JS_EXTRACT_CARDS)
        return raw or []
    except Exception as exc:
        print(f"[EMLAKJET] JS çıkarma hatası: {exc}")
        return []


# ---------------------------------------------------------------------------
# Detay sayfası çıkarma
# ---------------------------------------------------------------------------

_JS_EXTRACT_DETAIL = r"""
return (function() {
    var res = {
        lat: '', lon: '',
        is_jet_firsat: false,
        price_est_min: '', price_est_max: '', price_discount_pct: '',
        region_avg_rental: '', region_avg_sale: '',
        region_return_years: '', region_value_change_1y: '',
        transport_nearby: '[]', education_nearby: '[]',
        market_nearby: '[]', cafe_restaurant_nearby: '[]',
        health_nearby: '[]'
    };

    // ── 1. __NEXT_DATA__ (Next.js) ────────────────────────────────────
    try {
        var nd = document.getElementById('__NEXT_DATA__');
        if (nd) {
            var ndata = JSON.parse(nd.textContent);
            var pp = ((ndata.props || {}).pageProps) || {};
            var ad = pp.ad || pp.listing || pp.adDetail || pp.detail || pp.data || {};

            // Koordinatlar
            res.lat = String(
                ad.latitude || ad.lat ||
                ((ad.geo||{}).lat) || ((ad.location||{}).lat) ||
                ((ad.coordinates||{}).lat) || ''
            );
            res.lon = String(
                ad.longitude || ad.lng || ad.lon ||
                ((ad.geo||{}).lng) || ((ad.location||{}).lng) ||
                ((ad.coordinates||{}).lng) || ((ad.coordinates||{}).lon) || ''
            );

            // JetFırsat
            res.is_jet_firsat = !!(
                ad.isJetOpportunity || ad.isJetFirsat || ad.jetFirsat ||
                ad.isAdvantageousPrice || ad.isJetChance || false
            );

            // Fiyat tahmini
            var pe = ad.priceEstimate || ad.estimatedPrice || ad.priceRange || ad.indexPrice || {};
            if (pe.min != null) {
                res.price_est_min = String(pe.min || '');
                res.price_est_max = String(pe.max || '');
                res.price_discount_pct = String(pe.discountPercent || pe.percent || pe.discount || '');
            }

            // Yakın mekanlar (__NEXT_DATA__ içinde)
            var nl = ad.nearbyLocations || ad.nearbyPlaces || ad.nearby || pp.nearbyLocations || null;
            if (nl && typeof nl === 'object') {
                var catMap = {
                    transport_nearby: ['transportation','transport','ulasim'],
                    education_nearby: ['education','egitim'],
                    market_nearby:    ['market','shopping'],
                    cafe_restaurant_nearby: ['restaurant','cafeRestaurant','cafe','restaurants'],
                    health_nearby:    ['health','saglik']
                };
                for (var key in catMap) {
                    var cats = catMap[key];
                    for (var ci=0; ci<cats.length; ci++) {
                        var itms = nl[cats[ci]];
                        if (itms && itms.length) {
                            res[key] = JSON.stringify(itms.slice(0,15).map(function(p) {
                                return {name: p.name||p.title||'', distance: String(p.distance||p.dist||'')};
                            }));
                            break;
                        }
                    }
                }
            }

            // Bölge raporu (__NEXT_DATA__ içinde)
            var rg = pp.regionStats || pp.districtStats || ad.regionStats || {};
            if (rg.avgRental || rg.averageRentalPrice) {
                res.region_avg_rental     = String(rg.avgRental || rg.averageRentalPrice || '');
                res.region_avg_sale       = String(rg.avgSale   || rg.averageSalePrice   || '');
                res.region_return_years   = String(rg.returnYears || rg.paybackPeriod    || '');
                res.region_value_change_1y= String(rg.valueChange1y || rg.priceChange    || '');
            }
        }
    } catch(e) {}

    // ── 2. DOM fallback'ler ──────────────────────────────────────────
    var bodyText = (document.body || {}).innerText || '';

    // Koordinat: data-lat/data-lng
    if (!res.lat) {
        try {
            var mapEl = document.querySelector('[data-lat],[data-latitude]');
            if (mapEl) {
                res.lat = mapEl.getAttribute('data-lat') || mapEl.getAttribute('data-latitude') || '';
                res.lon = mapEl.getAttribute('data-lng') || mapEl.getAttribute('data-longitude') || '';
            }
        } catch(e) {}
    }

    // Koordinat: script tag ıçinde "latitude": 40.xxx
    if (!res.lat) {
        try {
            var scripts = document.querySelectorAll('script');
            for (var si=0; si<scripts.length && !res.lat; si++) {
                var sc = scripts[si].textContent;
                var lm = sc.match(/"latitude"\s*:\s*(["']?)([\d.]+)\1/);
                var nm = sc.match(/"longitude"\s*:\s*(["']?)([\d.]+)\1/);
                if (lm && nm) { res.lat = lm[2]; res.lon = nm[2]; }
            }
        } catch(e) {}
    }

    // JetFırsat: DOM class
    if (!res.is_jet_firsat) {
        try {
            var jetEl = document.querySelector(
                '[class*="jetfirsat" i],[class*="jet-firsat" i],[class*="JetFirsat"],[class*="jetOpportunity" i]'
            );
            res.is_jet_firsat = !!jetEl;
        } catch(e) {}
    }

    // Fiyat tahmini: DOM metin
    if (!res.price_est_min) {
        try {
            var discM = bodyText.match(/%\s*(\d+)\s*Daha\s*Ucuz/i);
            if (discM) res.price_discount_pct = '%' + discM[1];
            var minM = bodyText.match(/Min\s+([\d.,]+\s*TL)/i);
            if (minM) res.price_est_min = minM[1].trim();
            var maxM = bodyText.match(/Max\s+([\d.,]+\s*TL)/i);
            if (maxM) res.price_est_max = maxM[1].trim();
        } catch(e) {}
    }

    // Bölge raporu: DOM metin
    if (!res.region_avg_rental) {
        try {
            var renM = bodyText.match(/Ortalama\s+Kiralık\s+Fiyat[iı]\s*[\n\r]?\s*([\d.,]+\s*TL)/i);
            if (renM) res.region_avg_rental = renM[1].trim();
            var salM = bodyText.match(/Ortalama\s+Satılık\s+Fiyat[iı]\s*[\n\r]?\s*([\d.,]+\s*TL)/i);
            if (salM) res.region_avg_sale = salM[1].trim();
            var retM = bodyText.match(/Geri\s+Dönüş\s+Süres[iı][^0-9]*(\d+)\s*Yıl/i);
            if (retM) res.region_return_years = retM[1];
            var chgM = bodyText.match(/1\s*Yıllık\s+Değer\s+Değişimi\s*[\n\r]?\s*(%?[\d.,]+%?)/i);
            if (chgM) res.region_value_change_1y = chgM[1];
        } catch(e) {}
    }

    // Yakın mekan: DOM (fallback)
    if (res.transport_nearby === '[]') {
        try {
            var POI_TITLES = {
                transport_nearby:       'Ulaşım',
                education_nearby:       'Eğitim Kurumları',
                market_nearby:          'Marketler',
                cafe_restaurant_nearby: 'Kafeler/Restoranlar',
                health_nearby:          'Sağlık Kurumları'
            };
            var allH = document.querySelectorAll('h3,h4');
            for (var pkey in POI_TITLES) {
                var ptitle = POI_TITLES[pkey];
                var targetH = null;
                for (var hi=0; hi<allH.length; hi++) {
                    if (allH[hi].textContent.trim() === ptitle) { targetH = allH[hi]; break; }
                }
                if (!targetH) continue;
                var items = [];
                var sib = targetH.nextElementSibling;
                var loopLimit = 0;
                while (sib && loopLimit++ < 40 && items.length < 10) {
                    if (sib.tagName && /^h[1-6]$/i.test(sib.tagName)) break;
                    var leaves = sib.querySelectorAll('p,span,div,li');
                    if (!leaves.length) {
                        var txt = sib.textContent.trim();
                        if (txt) {
                            var dm = txt.match(/^(.+?)\s+([\d.,]+\s*m)$/i);
                            if (dm) items.push({name: dm[1].trim(), distance: dm[2].trim()});
                            else items.push({name: txt, distance: ''});
                        }
                    } else {
                        for (var ti=0; ti<leaves.length && items.length<10; ti++) {
                            var leaf = leaves[ti];
                            if (leaf.childElementCount > 0) continue;
                            var t = leaf.textContent.trim();
                            if (!t) continue;
                            var dm2 = t.match(/^(.+?)\s+([\d.,]+\s*m)$/i);
                            if (dm2) items.push({name: dm2[1].trim(), distance: dm2[2].trim()});
                        }
                    }
                    sib = sib.nextElementSibling;
                }
                if (items.length > 0) res[pkey] = JSON.stringify(items);
            }
        } catch(e) {}
    }

    return res;
})();
"""


_DETAIL_DEFAULTS: dict = {
    "lat": "",
    "lon": "",
    "is_jet_firsat": False,
    "price_est_min": "",
    "price_est_max": "",
    "price_discount_pct": "",
    "region_avg_rental": "",
    "region_avg_sale": "",
    "region_return_years": "",
    "region_value_change_1y": "",
    "transport_nearby": "[]",
    "education_nearby": "[]",
    "market_nearby": "[]",
    "cafe_restaurant_nearby": "[]",
    "health_nearby": "[]",
}


def fetch_detail_page(
    driver: webdriver.Chrome,
    url: str,
    settle_secs: float = 2.0,
    timeout: int = 30,
) -> dict:
    """İlan detay sayfasından koordinat, jetfırsat, fiyat endeksi, bölge raporu ve POI verilerini çıkar."""
    try:
        driver.get(url)
        # Sayfa yüklenene kadar bekle (başlık)
        try:
            WebDriverWait(driver, timeout).until(
                lambda d: d.execute_script("return (document.title || '').length > 5")
            )
        except TimeoutException:
            pass
        time.sleep(settle_secs)
        result = driver.execute_script(_JS_EXTRACT_DETAIL)
        if isinstance(result, dict):
            merged = dict(_DETAIL_DEFAULTS)
            merged.update(result)
            return merged
    except Exception as exc:
        print(f"[EMLAKJET] Detay sayfası hatası ({url[:70]}): {exc}")
        # Renderer timeout sonrası driver'ı sıfırla; aksi hâlde sonraki get() de çöker
        try:
            driver.get("about:blank")
        except Exception:
            pass
    return dict(_DETAIL_DEFAULTS)


# ---------------------------------------------------------------------------
# Veri işleme
# ---------------------------------------------------------------------------

def parse_quick_info(quick_info_text: str) -> dict:
    """'Daire | 3+1 | 1. Kat | 110 m²' formatını ayrıştır."""
    room_count = ""
    floor_info = ""
    sqm = ""
    estate_type = ""

    parts = [p.strip() for p in quick_info_text.split("|") if p.strip()]
    for part in parts:
        # Oda sayısı: "3+1", "1+0", "4+1"
        if re.match(r"^\d+\+\d+$", part):
            room_count = part
        # Kat bilgisi
        elif "Kat" in part and len(part) < 30:
            floor_info = part
        # Alan: "125 m²", "1.000 m²"
        elif re.search(r"\d[\d.]*\s*m[²2]", part):
            sqm = part
        # Emlak tipi: sayı içermeyen kısa metin (ilk eşleşen)
        elif not re.search(r"\d", part) and len(part) < 30 and not estate_type:
            estate_type = part

    return {
        "room_count": room_count,
        "floor": floor_info,
        "gross_sqm": sqm,
        "estate_type": estate_type,
    }


def parse_price_str(price_str: str) -> tuple[str, str]:
    """Fiyat metninden değer ve para birimi çıkar."""
    if not price_str:
        return "", ""
    ps = price_str.strip()
    if "Fiyat Sor" in ps:
        return "Fiyat Sor", ""
    if "Kat Kar" in ps:
        return "Kat Karşılığı", ""

    currency = ""
    if "TL" in ps or "₺" in ps:
        currency = "TL"
    elif "USD" in ps or "$" in ps:
        currency = "USD"
    elif "EUR" in ps or "€" in ps:
        currency = "EUR"

    return ps.strip(), currency


def process_raw_card(raw: dict, category: str, trade_type: str) -> dict:
    """Ham kart verisini yapılandırılmış satıra dönüştür."""
    quick_info_text = raw.get("quickInfoText") or ""
    parsed = parse_quick_info(quick_info_text)
    price_str = raw.get("price") or ""
    price_val, currency = parse_price_str(price_str)

    return {
        "url": raw.get("url") or "",
        "listing_id": raw.get("id") or "",
        "title": clean_text(raw.get("title")),
        "location": clean_text(raw.get("location")),
        "district": clean_text(raw.get("district")),
        "neighborhood": clean_text(raw.get("neighborhood")),
        "city": clean_text(raw.get("city")),
        "lat": raw.get("lat") or "",
        "lon": raw.get("lon") or "",
        "category": category,
        "trade_type": trade_type,
        "price": price_val,
        "currency": currency,
        "prev_price": clean_text(raw.get("prevPrice")),
        "room_count": parsed["room_count"],
        "floor": parsed["floor"],
        "gross_sqm": parsed["gross_sqm"],
        "estate_type": parsed["estate_type"],
        "quick_infos": quick_info_text,
        "image_url": raw.get("image_url") or "",
        "scraped_at_utc": utc_now_iso(),
    }


# ---------------------------------------------------------------------------
# Sayfa yükleme ve çıkarma
# ---------------------------------------------------------------------------

def fetch_list_page(
    driver: webdriver.Chrome,
    url: str,
    category: str,
    trade_type: str,
    settle_secs: float = 3.0,
    timeout: int = 35,
) -> list[dict]:
    """Tek bir liste sayfasını yükle, ilan kartlarını çıkar ve döndür."""
    last_err: Optional[Exception] = None
    for attempt in range(1, 4):
        try:
            driver.get(url)
            found = wait_for_listings(driver, timeout=timeout)
            if not found:
                # Son sayfa geçilmiş olabilir – boş döndür
                return []
            time.sleep(settle_secs + random.uniform(0, 0.8))
            raw_cards = extract_card_data(driver)
            rows: list[dict] = []
            for raw in raw_cards:
                row = process_raw_card(raw, category, trade_type)
                if row["url"]:
                    rows.append(row)
            return rows
        except (TimeoutException, WebDriverException, Exception) as exc:
            last_err = exc
            if attempt < 3:
                wait = 4.0 * attempt + random.uniform(0, 1.5)
                print(f"[EMLAKJET:{category}] Sayfa yükleme hatası (deneme {attempt}/3): {exc} | {wait:.1f}s bekleniyor")
                time.sleep(wait)

    print(f"[EMLAKJET:{category}] HATA – sayfa alınamadı: {url} -> {last_err}")
    return []


# ---------------------------------------------------------------------------
# Sayfa aralığı toplama (tek worker)
# ---------------------------------------------------------------------------

def collect_page_range(
    category_name: str,
    base_url: str,
    trade_type: str,
    page_start: int,
    page_end: int,
    csv_path: Path,
    csv_lock: threading.Lock,
    cp_path: Path,
    cp_lock: threading.Lock,
    cp: dict,
    headless: bool,
    no_images: bool,
    delay: float,
    settle_secs: float = 3.0,
    push_callback=None,      # callable(rows: list[dict]) -> None
    cp_callback=None,        # callable(cp: dict) -> None
    proxy_getter=None,       # callable() -> str, her worker için proxy URL döndürür
    scrape_details: bool = False,  # True ise her ilan için detay sayfası ziyaret edilir
) -> int:
    """Verilen sayfa aralığını tek bir driver ile tara; satırları CSV'ye kaydet."""
    # Semafor: maks Chrome limiti aşılmışsa bekle
    if _CHROME_SEM is not None:
        _CHROME_SEM.acquire()
    proxy_url = ""
    if proxy_getter:
        try:
            proxy_url = proxy_getter()
        except Exception as _pe:
            print(f"[EMLAKJET:{category_name}] Proxy URL alınamadı: {_pe}")
    driver = create_driver(headless=headless, no_images=no_images, proxy_url=proxy_url)
    saved_count = 0

    try:
        for page in range(page_start, page_end + 1):
            # Checkpoint kontrolü
            with cp_lock:
                done_pages: list[int] = cp.get("list_done_pages", {}).get(category_name, [])
                if page in done_pages:
                    continue

            url = listing_page_url(base_url, page)
            rows = fetch_list_page(
                driver, url, category_name, trade_type,
                settle_secs=settle_secs,
            )

            if not rows:
                print(f"[EMLAKJET:{category_name}] Sayfa {page} boş – pagination sonu, duruyorum")
                break  # Sayfa boşsa listeleme bitti

            # ── Detay sayfası ziyareti (opsiyonel) ──────────────────────────
            if scrape_details and rows:
                for row in rows:
                    detail_url = row.get("url")
                    if detail_url:
                        detail = fetch_detail_page(
                            driver, detail_url,
                            settle_secs=min(settle_secs, 0.7),
                        )
                        # Koordinat boşsa liste sayfasından geleni koru
                        if not detail.get("lat") and row.get("lat"):
                            detail["lat"] = row["lat"]
                        if not detail.get("lon") and row.get("lon"):
                            detail["lon"] = row["lon"]
                        row.update(detail)
                        time.sleep(0.15 + random.uniform(0, 0.2))
                print(
                    f"[EMLAKJET:{category_name}] Detay sayfaları tamamlandı: {len(rows)} ilan"
                )

            # CSV'ye yaz
            with csv_lock:
                csv_path.parent.mkdir(parents=True, exist_ok=True)
                file_exists = csv_path.exists()
                with csv_path.open("a", newline="", encoding="utf-8-sig", errors="ignore") as fh:
                    writer = csv.DictWriter(fh, fieldnames=COLUMNS, extrasaction="ignore")
                    if not file_exists:
                        writer.writeheader()
                    writer.writerows(rows)
            saved_count += len(rows)

            # Veriyi push callback ile anlık aktar
            if push_callback and rows:
                try:
                    push_callback(rows)
                except Exception as _pce:
                    print(f"[EMLAKJET:{category_name}] push_callback hatası: {_pce}")

            # Checkpoint güncelle
            with cp_lock:
                dp = cp.setdefault("list_done_pages", {}).setdefault(category_name, [])
                if page not in dp:
                    dp.append(page)
                _atomic_write(cp_path, json.dumps(cp, ensure_ascii=False, indent=2))
                if cp_callback:
                    try:
                        cp_callback(dict(cp))
                    except Exception as _cce:
                        print(f"[EMLAKJET:{category_name}] cp_callback hatası: {_cce}")

            print(
                f"[EMLAKJET:{category_name}] Sayfa {page}/{page_end}"
                f" | {len(rows)} ilan | toplam {saved_count}"
            )
            time.sleep(delay + random.uniform(0, 0.5))

    finally:
        try:
            driver.quit()
        except Exception:
            pass
        if _CHROME_SEM is not None:
            _CHROME_SEM.release()

    return saved_count


# ---------------------------------------------------------------------------
# Kategori scrape
# ---------------------------------------------------------------------------

def scrape_category(
    category_name: str,
    base_url: str,
    workers: int,
    max_pages: int,
    headless: bool,
    no_images: bool,
    delay: float,
    output_dir: Path,
    settle_secs: float = 3.0,
    push_callback=None,         # callable(rows: list[dict]) -> None
    cp_callback=None,           # callable(cp: dict) -> None
    proxy_getter=None,          # callable() -> str, her worker için proxy URL döndürür
    scrape_details: bool = False,  # True ise her ilan için detay sayfası ziyaret edilir
) -> int:
    trade_type = TRADE_TYPE_MAP.get(category_name, "")
    csv_path = output_dir / f"emlakjet_{category_name}.csv"
    cp_path  = output_dir / f"emlakjet_{category_name}.checkpoint.json"

    # Checkpoint yükle
    cp: dict = {}
    if cp_path.exists():
        try:
            cp = json.loads(cp_path.read_text(encoding="utf-8"))
        except Exception:
            cp = {}

    print(f"\n[EMLAKJET:{category_name}] ── Başlanıyor: {base_url}")

    # Bootstrap: sayfa 1'i yükle, toplam ilan sayısını ve sayfa başı ilan sayısını öğren
    if _CHROME_SEM is not None:
        _CHROME_SEM.acquire()
    boot_proxy = ""
    if proxy_getter:
        try:
            boot_proxy = proxy_getter()
        except Exception:
            pass
    boot_driver = create_driver(headless=headless, no_images=no_images, proxy_url=boot_proxy)
    total_count = 0
    per_page = DEFAULT_PER_PAGE
    try:
        boot_driver.get(base_url)
        found = wait_for_listings(boot_driver, timeout=40)
        if not found:
            print(f"[EMLAKJET:{category_name}] Sayfa 1'de ilan yok – kategori atlanıyor")
            return 0
        time.sleep(2.5)
        total_count = get_total_count(boot_driver)
        first_page_cards = extract_card_data(boot_driver)
        if first_page_cards:
            per_page = len(first_page_cards)
    except Exception as exc:
        err_msg = str(exc)
        print(f"[EMLAKJET:{category_name}] Bootstrap hatası: {err_msg}")
        # Proxy bağlantı hatası → proxysiz yeniden dene
        if boot_proxy and "PROXY" in err_msg.upper():
            print(f"[EMLAKJET:{category_name}] Proxy başarısız → proxysiz yeniden deneniyor")
            # boot_driver'ı hemen kapat (finally beklemeden) ve semafor slotu serbest bırak
            try:
                boot_driver.quit()
            except Exception:
                pass
            if _CHROME_SEM is not None:
                _CHROME_SEM.release()
            # Fallback: proxysiz yeni driver aç
            if _CHROME_SEM is not None:
                _CHROME_SEM.acquire()
            try:
                boot_driver2 = create_driver(headless=headless, no_images=no_images, proxy_url="")
                try:
                    boot_driver2.get(base_url)
                    found = wait_for_listings(boot_driver2, timeout=40)
                    if not found:
                        return 0
                    time.sleep(2.5)
                    total_count = get_total_count(boot_driver2)
                    first_page_cards = extract_card_data(boot_driver2)
                    if first_page_cards:
                        per_page = len(first_page_cards)
                    proxy_getter = None  # kalan worker'lar da proxysiz çalışsın
                    print(f"[EMLAKJET:{category_name}] Proxysiz bootstrap başarılı")
                except Exception as exc2:
                    print(f"[EMLAKJET:{category_name}] Proxysiz deneme de başarısız: {exc2}")
                    if _CHROME_SEM is not None:
                        _CHROME_SEM.release()
                    raise RuntimeError(
                        f"Bootstrap tamamen başarısız: {exc2}"
                    ) from exc2
                finally:
                    try:
                        boot_driver2.quit()
                    except Exception:
                        pass
            finally:
                if _CHROME_SEM is not None:
                    _CHROME_SEM.release()
            # Başarılı fallback → finally bloğunu bypass et (boot_driver zaten kapatıldı)
            boot_driver = None  # type: ignore[assignment]
        else:
            raise RuntimeError(f"Bootstrap başarısız: {err_msg}") from exc
    finally:
        if boot_driver is not None:
            try:
                boot_driver.quit()
            except Exception:
                pass
            if _CHROME_SEM is not None:
                _CHROME_SEM.release()

    per_page = per_page or DEFAULT_PER_PAGE
    if total_count > 0:
        total_pages = max(1, (total_count + per_page - 1) // per_page)
    else:
        total_pages = 9999  # Bilinmiyor; boş sayfa gelince durulacak

    effective_pages = total_pages if max_pages <= 0 else min(max_pages, total_pages)

    print(
        f"[EMLAKJET:{category_name}] Toplam ilan: {total_count:,}"
        f" | Sayfa başı: {per_page} | Toplam sayfa: {total_pages}"
        f" | İşlenecek: {effective_pages}"
    )

    # Worker aralıklarını hesapla
    workers_actual = max(1, min(workers, effective_pages))
    base_size = effective_pages // workers_actual
    extra = effective_pages % workers_actual
    ranges: list[tuple[int, int]] = []
    start = 1
    for i in range(workers_actual):
        size = base_size + (1 if i < extra else 0)
        end = start + size - 1
        ranges.append((start, end))
        start = end + 1

    print(f"[EMLAKJET:{category_name}] Worker aralıkları: {ranges}")

    csv_lock = threading.Lock()
    cp_lock  = threading.Lock()
    total_saved = 0

    if len(ranges) == 1:
        total_saved = collect_page_range(
            category_name=category_name,
            base_url=base_url,
            trade_type=trade_type,
            page_start=ranges[0][0],
            page_end=ranges[0][1],
            csv_path=csv_path,
            csv_lock=csv_lock,
            cp_path=cp_path,
            cp_lock=cp_lock,
            cp=cp,
            headless=headless,
            no_images=no_images,
            delay=delay,
            settle_secs=settle_secs,
            push_callback=push_callback,
            cp_callback=cp_callback,
            proxy_getter=proxy_getter,
            scrape_details=scrape_details,
        )
    else:
        with ThreadPoolExecutor(max_workers=len(ranges)) as exe:
            futures = [
                exe.submit(
                    collect_page_range,
                    category_name=category_name,
                    base_url=base_url,
                    trade_type=trade_type,
                    page_start=s,
                    page_end=e,
                    csv_path=csv_path,
                    csv_lock=csv_lock,
                    cp_path=cp_path,
                    cp_lock=cp_lock,
                    cp=cp,
                    headless=headless,
                    no_images=no_images,
                    delay=delay,
                    settle_secs=settle_secs,
                    push_callback=push_callback,
                    cp_callback=cp_callback,
                    proxy_getter=proxy_getter,
                    scrape_details=scrape_details,
                )
                for s, e in ranges
            ]
            for fut in as_completed(futures):
                try:
                    total_saved += fut.result()
                except Exception as exc:
                    print(f"[EMLAKJET:{category_name}] Worker hatası: {exc}")

    print(
        f"\n[EMLAKJET:{category_name}] ── TAMAMLANDI:"
        f" {total_saved:,} ilan kaydedildi → {csv_path}"
    )
    return total_saved


# ---------------------------------------------------------------------------
# Probe modu: tek sayfayı test et
# ---------------------------------------------------------------------------

def probe_mode(category_name: str, pages: int, headless: bool, output_dir: Path) -> None:
    base_url = CATEGORIES[category_name]
    trade_type = TRADE_TYPE_MAP[category_name]
    csv_path = output_dir / f"emlakjet_{category_name}_probe.csv"

    driver = create_driver(headless=headless, no_images=True)
    try:
        for page in range(1, pages + 1):
            url = listing_page_url(base_url, page)
            print(f"\n[PROBE:{category_name}] Sayfa {page}: {url}")
            driver.get(url)
            found = wait_for_listings(driver, timeout=40)
            if not found:
                print(f"[PROBE:{category_name}] ilan yüklenmedi – duruyorum")
                break
            time.sleep(3.0)
            total = get_total_count(driver)
            cards = extract_card_data(driver)
            print(f"[PROBE:{category_name}] Toplam (sayfadan): {total:,} | Bu sayfada: {len(cards)} kart")
            if cards:
                sample = cards[0]
                print(f"  Örnek kart: id={sample.get('id')} url={sample.get('url','')[:80]}")
                print(f"  title={sample.get('title','')[:60]}")
                print(f"  location={sample.get('location','')}")
                print(f"  quick_infos={sample.get('quickInfoText','')}")
                print(f"  price={sample.get('price')}")

            rows = [process_raw_card(c, category_name, trade_type) for c in cards if c.get("url")]
            if rows:
                csv_path.parent.mkdir(parents=True, exist_ok=True)
                file_exists = csv_path.exists()
                with csv_path.open("a", newline="", encoding="utf-8-sig") as fh:
                    writer = csv.DictWriter(fh, fieldnames=COLUMNS, extrasaction="ignore")
                    if not file_exists:
                        writer.writeheader()
                    writer.writerows(rows)
                print(f"  → {len(rows)} satır probe CSV'ye yazıldı: {csv_path}")
    finally:
        try:
            driver.quit()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# CLI giriş noktası
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Emlakjet.com satılık/kiralık ilan scraper",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Örnekler:
  python emlakjet_scraper.py                           # Tüm kategoriler
  python emlakjet_scraper.py --probe                   # Test: satilik_konut, 3 sayfa
  python emlakjet_scraper.py --categories satilik_konut kiralik_konut
  python emlakjet_scraper.py --workers 3 --delay 1.0
  python emlakjet_scraper.py --max-pages 100 --headless false
        """,
    )
    ap.add_argument(
        "--categories", nargs="*",
        choices=list(CATEGORIES.keys()),
        help="Scrape edilecek kategoriler (varsayılan: hepsi)",
    )
    ap.add_argument(
        "--workers", type=int, default=1,
        help="Sayfa toplama worker sayısı (varsayılan: 1)",
    )
    ap.add_argument(
        "--delay", type=float, default=1.5,
        help="Worker başına sayfa bekleme süresi saniye (varsayılan: 1.5)",
    )
    ap.add_argument(
        "--max-pages", type=int, default=0,
        help="Kategori başına max sayfa (0=tümü, varsayılan: 0)",
    )
    ap.add_argument(
        "--headless", type=str, default="true",
        help="Tarayıcıyı arka planda çalıştır true/false (varsayılan: true)",
    )
    ap.add_argument(
        "--no-images", type=str, default="true",
        help="Görselleri yüklememe true/false (varsayılan: true)",
    )
    ap.add_argument(
        "--output-dir", type=str, default="data",
        help="CSV çıktı dizini (varsayılan: data)",
    )
    ap.add_argument(
        "--settle", type=float, default=3.0,
        help="Sayfa yüklendikten sonra bekleme sn (varsayılan: 3.0)",
    )
    ap.add_argument(
        "--probe", action="store_true",
        help="Test modu: sadece ilk 3 sayfayı satilik_konut için çek",
    )
    ap.add_argument(
        "--probe-category", type=str, default="satilik_konut",
        choices=list(CATEGORIES.keys()),
        help="Probe kategorisi (varsayılan: satilik_konut)",
    )
    ap.add_argument(
        "--probe-pages", type=int, default=3,
        help="Probe mod sayfa sayısı (varsayılan: 3)",
    )
    args = ap.parse_args()

    headless   = args.headless.lower() != "false"
    no_images  = args.no_images.lower() != "false"
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if args.probe:
        probe_mode(
            category_name=args.probe_category,
            pages=args.probe_pages,
            headless=headless,
            output_dir=output_dir,
        )
        return

    cats_to_run = args.categories or list(CATEGORIES.keys())
    grand_total = 0

    for cat in cats_to_run:
        base_url = CATEGORIES[cat]
        count = scrape_category(
            category_name=cat,
            base_url=base_url,
            workers=args.workers,
            max_pages=args.max_pages,
            headless=headless,
            no_images=no_images,
            delay=args.delay,
            output_dir=output_dir,
            settle_secs=args.settle,
        )
        grand_total += count

    print(f"\n[EMLAKJET] ═══════════════════════════════════════")
    print(f"[EMLAKJET] TÜM KATEGORİLER TAMAMLANDI")
    print(f"[EMLAKJET] Toplam kaydedilen ilan: {grand_total:,}")
    print(f"[EMLAKJET] Çıktı dizini: {output_dir.resolve()}")


if __name__ == "__main__":
    main()
