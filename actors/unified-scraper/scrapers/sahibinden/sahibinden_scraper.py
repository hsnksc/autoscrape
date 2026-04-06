"""sahibinden_scraper.py

Sahibinden.com liste sayfalarini BeautifulSoup ile scrape eder.
Camoufox (Firefox) + session cookies ile CF/login engelini asar.

Kullanim ornegi:
    python sahibinden_scraper.py --mode list_only --categories satilik_isyeri \
        --page-ranges 1-5 --page-workers 1 --delay 3.0 --csv data/test.csv
"""
from __future__ import annotations

import argparse
import csv
import json
import random
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from bs4 import BeautifulSoup
from urllib.parse import urlparse as _urlparse

BASE_URL = "https://www.sahibinden.com"

CATEGORIES: dict[str, str] = {
    "satilik_isyeri":        BASE_URL + "/satilik-is-yeri",
    "kiralik_isyeri":        BASE_URL + "/kiralik-is-yeri",
    "devren_satilik_isyeri": BASE_URL + "/devren-satilik-is-yeri",
    "devren_kiralik_isyeri": BASE_URL + "/devren-kiralik-is-yeri",
    "satilik":               BASE_URL + "/satilik",
    "kiralik":               BASE_URL + "/kiralik",
}

COLUMNS = [
    "url",
    "listing_id",
    "title",
    "category",
    "property_type",
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
    "floor",
    "floor_count",
    "building_age",
    "listing_date",
    "image_count",
    "scraped_at_utc",
]

# Proxy URL listesi - main.py tarafindan ayarlanir
PROXY_URLS: list[str] = []       # main.py tarafindan onceden uretilen URL listesi
_proxy_url_idx: list[int] = [0]  # dongusel index

# Session cookies - main.py tarafindan ayarlanir
# EditThisCookie JSON formatinda: [{name, value, domain, path, ...}, ...]
SESSION_COOKIES: list[dict] = []

# Camoufox handle listesi (cleanup icin)
_active_handles: list = []


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


# ---------------------------------------------------------------------------
# Browser management (Camoufox - Firefox ile CF bypass)
# ---------------------------------------------------------------------------


class _ProxyRateLimited(Exception):
    """Proxy IP rate-limit'e girdi – caller yeni IP denesin."""
    pass


class _BrowserHandle:
    """Camoufox lifecycle handle. .close() temizler."""
    def __init__(self, cm):
        self._cm = cm
    def close(self):
        try:
            self._cm.__exit__(None, None, None)
        except Exception:
            pass
        try:
            _active_handles.remove(self)
        except ValueError:
            pass


def _get_proxy_url():
    """PROXY_URLS listesinden sirali URL al."""
    if PROXY_URLS:
        url = PROXY_URLS[_proxy_url_idx[0] % len(PROXY_URLS)]
        _proxy_url_idx[0] += 1
        return url
    return None


def shutdown_browser():
    """Tum aktif browser handle'lari temizle."""
    for h in _active_handles[:]:
        try:
            h.close()
        except Exception:
            pass
    _active_handles.clear()


def _is_cf_challenge(text):
    """CF challenge sayfasi mi kontrol et (TR + EN)."""
    t = text[:5000].lower()
    return any(k in t for k in (
        "just a moment", "bir dakika", "challenge-platform",
        "cf-browser-verification", "cf-turnstile", "cf_chl_opt",
        "managed_checking_msg",
    ))


def _inject_cookies(browser_ctx):
    """SESSION_COOKIES listesindeki cerezleri browser context'e ekle."""
    if not SESSION_COOKIES:
        return 0
    pw_cookies = []
    for c in SESSION_COOKIES:
        cookie = {
            "name": c.get("name", ""),
            "value": c.get("value", ""),
            "domain": c.get("domain", ".sahibinden.com"),
            "path": c.get("path", "/"),
        }
        if c.get("secure"):
            cookie["secure"] = True
        if c.get("httpOnly"):
            cookie["httpOnly"] = True
        if c.get("sameSite"):
            ss = str(c["sameSite"]).capitalize()
            if ss in ("Strict", "Lax", "None"):
                cookie["sameSite"] = ss
        pw_cookies.append(cookie)
    if pw_cookies:
        browser_ctx.add_cookies(pw_cookies)
    return len(pw_cookies)


def create_context(proxy_url=None):
    """Camoufox ile yeni browser + page olustur. (_BrowserHandle, pw_page) doner."""
    from camoufox.sync_api import Camoufox
    proxy_opts = None
    if proxy_url:
        p = _urlparse(proxy_url)
        proxy_opts = {
            "server": f"{p.scheme}://{p.hostname}:{p.port}",
            "username": p.username or "",
            "password": p.password or "",
        }
    cm = Camoufox(
        headless=True,
        proxy=proxy_opts,
        geoip=True,
    )
    browser = cm.__enter__()
    browser_ctx = browser.new_context()
    n_cookies = _inject_cookies(browser_ctx)
    pw_page = browser_ctx.new_page()
    handle = _BrowserHandle(cm)
    _active_handles.append(handle)
    print(f"[SAHIBINDEN] Camoufox Firefox baslatildi (proxy={'evet' if proxy_opts else 'hayir'}, cerez={n_cookies})")
    return handle, pw_page


def _wait_cf_challenge(pw_page, label="", max_wait=45):
    """CF challenge sayfasinda ise cozulene kadar bekle. True=cozuldu."""
    for i in range(max_wait):
        content = pw_page.content()
        if not _is_cf_challenge(content):
            return True
        if i % 10 == 0 and i > 0:
            print(f"[SAHIBINDEN{label}] CF challenge bekleniyor... ({i}s)")
        time.sleep(1)
    return False


def warmup_session(pw_page, label=""):
    """Ana sayfayi ziyaret ederek CF challenge'i gercek tarayici ile coz."""
    try:
        pw_page.goto(f"{BASE_URL}/", wait_until="domcontentloaded", timeout=60000)
        # CF challenge varsa tarayici JS otomatik cozecek – 45s bekle
        cf_ok = _wait_cf_challenge(pw_page, label, max_wait=45)
        time.sleep(random.uniform(2.0, 4.0))
        title = pw_page.title()
        cookies = pw_page.context.cookies()
        proxy_info = "proxy" if PROXY_URLS else "direkt"
        print(f"[SAHIBINDEN{label}] Warmup ({proxy_info}): baslik={title!r} | cerez={len(cookies)} | cf_ok={cf_ok}")
        if not cf_ok:
            print(f"[SAHIBINDEN{label}] Warmup: CF challenge cozulemedi")
            return False
        title_l = title.lower()
        # CF challenge hala baslikta mi?
        if "bir dakika" in title_l or "just a moment" in title_l:
            print(f"[SAHIBINDEN{label}] Warmup: CF challenge hala aktif")
            return False
        # Sahibinden hata sayfasi / IP engeli
        if "hata" in title_l or ("not found" in title_l and "sahibinden" not in title_l):
            print(f"[SAHIBINDEN{label}] Warmup: hata sayfasi - IP engeli ({title!r})")
            return False
        if "giri" in title_l or "login" in title_l:
            body = pw_page.content()[:5000].lower()
            if "searchresults" not in body:
                print(f"[SAHIBINDEN{label}] Warmup: login sayfasi - IP engeli")
                return False
        return True
    except Exception as exc:
        print(f"[SAHIBINDEN{label}] Warmup hatasi: {exc}")
        return False


def create_working_context(label=""):
    """Farkli proxy IP'leri ile warmup dene. (context, pw_page) tuple doner."""
    last_ctx = None
    last_page = None
    for attempt in range(1, 6):
        proxy_url = _get_proxy_url()
        if last_ctx:
            try:
                last_ctx.close()
            except Exception:
                pass
        ctx, pw_page = create_context(proxy_url=proxy_url)
        last_ctx = ctx
        last_page = pw_page
        ok = warmup_session(pw_page, label)
        if ok:
            return ctx, pw_page
        proxy_info = proxy_url[:40] if proxy_url else "direkt"
        print(f"[SAHIBINDEN{label}] Deneme {attempt}/5: {proxy_info} - yeni IP deneniyor...")
        time.sleep(2)
    print(f"[SAHIBINDEN{label}] Tum warmup denemeleri basarisiz, son context ile devam...")
    return last_ctx, last_page


def _debug_html_structure(soup, html, label=""):
    """0 satir durumunda HTML yapisini logla."""
    tr_items = soup.select("tr.searchResultsItem")
    any_items = soup.select(".searchResultsItem")
    print(f"[DEBUG:{label}] tr.searchResultsItem={len(tr_items)} | .searchResultsItem={len(any_items)} | html_len={len(html)}")
    if any_items and not tr_items:
        first = any_items[0]
        print(f"[DEBUG:{label}] ilk .searchResultsItem tag={first.name}, classes={first.get('class')}")
        print(f"[DEBUG:{label}] snippet: {str(first)[:500]}")
    elif not any_items:
        # searchResultsItem string var ama element yok – JS icerisinde olabilir
        idx = html.find("searchResultsItem")
        if idx >= 0:
            snippet = html[max(0, idx - 100):idx + 200]
            print(f"[DEBUG:{label}] searchResultsItem string context: ...{snippet}...")
        # Alternatif seçiciler dene
        for sel in [".listing-item", ".classified-list-item", "[data-listing-id]",
                    ".searchResults .listing", "table.searchResults tr", ".search-result-item"]:
            found = soup.select(sel)
            if found:
                print(f"[DEBUG:{label}] alternatif '{sel}' -> {len(found)} eleman")
                print(f"[DEBUG:{label}] ilk: {str(found[0])[:300]}")
                break
        # Tum table/div yapısını özetle
        tables = soup.select("table")
        print(f"[DEBUG:{label}] table sayisi={len(tables)}")
        for t in tables[:3]:
            t_id = t.get("id", "")
            t_cls = " ".join(t.get("class", []))
            rows = t.select("tr")
            print(f"[DEBUG:{label}] table#{t_id} .{t_cls} -> {len(rows)} tr")


_dump_done = [False]  # sadece 1 kez dump yap

def _dump_page_structure(html, url=""):
    """Sayfa HTML yapisini bir kez logla – parser debug icin."""
    if _dump_done[0]:
        return
    _dump_done[0] = True
    from bs4 import BeautifulSoup as _BS
    soup = _BS(html, "lxml")
    print(f"\n{'='*60}")
    print(f"[HTML_DUMP] URL: {url}")
    print(f"[HTML_DUMP] HTML uzunluk: {len(html)}")
    print(f"[HTML_DUMP] Title: {soup.title.get_text() if soup.title else '(yok)'}")

    # searchResultsItem arama
    sr_str_count = html.count("searchResultsItem")
    tr_items = soup.select("tr.searchResultsItem")
    any_items = soup.select(".searchResultsItem")
    print(f"[HTML_DUMP] 'searchResultsItem' string: {sr_str_count}x | tr.elem: {len(tr_items)} | any.elem: {len(any_items)}")

    if tr_items:
        first = tr_items[0]
        tds = first.select("td")
        print(f"[HTML_DUMP] ilk tr: {len(tds)} td, data-id={first.get('data-id', '?')}")
        for i, td in enumerate(tds[:8]):
            cls = " ".join(td.get("class", []))
            txt = td.get_text(strip=True)[:80]
            inner_a = td.select_one("a")
            a_info = f" -> a.href={inner_a.get('href', '')[:60]}" if inner_a else ""
            print(f"[HTML_DUMP]   td[{i}] .{cls}: {txt!r}{a_info}")
        print(f"[HTML_DUMP] ilk tr raw (1500c): {str(first)[:1500]}")
    elif any_items:
        first = any_items[0]
        print(f"[HTML_DUMP] .searchResultsItem tag={first.name}, class={first.get('class')}")
        print(f"[HTML_DUMP] snippet: {str(first)[:1000]}")
    else:
        # String var ama element yok – nerede gectigini bul
        for ctx_kw in ["searchResultsItem", "searchResults", "listing-item",
                        "classified", "data-id", "resultItem"]:
            idx = html.find(ctx_kw)
            if idx >= 0:
                snippet = html[max(0, idx-80):idx+200]
                print(f"[HTML_DUMP] '{ctx_kw}' at pos {idx}: ...{snippet}...")
                break
        # Body altindaki child elementleri listele
        body = soup.body
        if body:
            children = [c for c in body.children if hasattr(c, 'name') and c.name]
            child_info = [(c.name, c.get("id", ""), " ".join(c.get("class", [])[:2])) for c in children[:15]]
            print(f"[HTML_DUMP] body children ({len(children)}): {child_info}")
        # Script icinde JSON veri var mi?
        for script in soup.select("script"):
            txt = script.string or ""
            if "totalCount" in txt or "listing" in txt.lower()[:200]:
                print(f"[HTML_DUMP] script icinde data: {txt[:500]}")
                break

    print(f"{'='*60}\n")


def fetch_page_html(
    pw_page,
    url,
    delay=2.0,
    max_retries=3,
):
    """Playwright ile URL yi yukle. JS renderingi bekle, HTML dondur."""
    for attempt in range(1, max_retries + 1):
        try:
            resp = pw_page.goto(url, wait_until="domcontentloaded", timeout=30000)
        except Exception as exc:
            print(f"[SAHIBINDEN] Navigasyon hatasi ({attempt}/{max_retries}): {exc}")
            if attempt < max_retries:
                time.sleep(5 + attempt * 3)
            continue

        sc = resp.status if resp else 0

        # CF challenge varsa tarayici otomatik cozecek – 30s bekle
        cf_ok = _wait_cf_challenge(pw_page, max_wait=30)

        # JS rendering icin ekstra bekleme – DOM elementleri olusana kadar
        try:
            pw_page.wait_for_selector(
                "tr.searchResultsItem, .searchResultsItem, [data-id]",
                timeout=10000,
            )
        except Exception:
            # Selector bulunamazsa devam et – debug icin HTML alacagiz
            pass

        # networkidle benzeri kisa bekleme
        time.sleep(1)

        html = pw_page.content()

        # CF sonrasi sayfa yeniden yuklenebilir
        if cf_ok and sc in (403, 429, 503):
            if "searchResultsItem" in html or len(html) > 20000:
                return html

        if sc in (403, 429, 503):
            cf_text = _is_cf_challenge(html)
            print(f"[SAHIBINDEN] HTTP {sc} ({attempt}/{max_retries}), cf_challenge={cf_text}, bekleniyor...")
            if attempt < max_retries:
                time.sleep(15 + attempt * 10)
            continue

        if sc not in (200, 0):
            print(f"[SAHIBINDEN] Beklenmeyen HTTP {sc} ({attempt}/{max_retries})")
            if attempt < max_retries:
                time.sleep(5 + attempt * 2)
            continue

        lower = html[:3000].lower()

        if "giri" in lower[:500] and "searchresultsitem" not in lower:
            print(f"[SAHIBINDEN] Login sayfasi tespit edildi ({attempt}/{max_retries})")
            if attempt < max_retries:
                warmup_session(pw_page)
                time.sleep(3)
                continue

        if "searchResultsItem" in html:
            return html

        # searchResultsItem yok ama sayfa yüklendi – belki farkli selector var
        if len(html) > 15000:
            return html

        title = pw_page.title() or "(bos)"
        print(
            f"[SAHIBINDEN] searchResultsItem bulunamadi ({attempt}/{max_retries}) "
            f"| baslik={title!r} | uzunluk={len(html)}"
        )

        if attempt == max_retries and len(html) > 10000:
            return html

        if attempt < max_retries:
            time.sleep(delay + 3 + attempt * 2)

    raise RuntimeError(f"Sayfa yuklenemedi ({max_retries} deneme): {url}")


# ---------------------------------------------------------------------------
# HTML parsing
# ---------------------------------------------------------------------------

def parse_price(price_text: str) -> tuple[str, str]:
    """15.000.000 TL -> (15000000, TL)"""
    if not price_text:
        return "", ""
    price_text = price_text.strip()
    currency = ""
    for cur in ["TL", "USD", "EUR", "$", "€", "£"]:
        if cur in price_text:
            currency = cur
            break
    cleaned = re.sub(r"\.", "", price_text)
    cleaned = re.sub(r"[^\d]", "", cleaned)
    if cleaned:
        return cleaned, currency
    return "", currency


def parse_attributes(attr_items: list[str]) -> dict[str, str]:
    """Parse attributeBox li text items into structured fields."""
    result: dict[str, str] = {
        "gross_sqm": "",
        "net_sqm": "",
        "room_count": "",
        "floor": "",
    }
    for item in attr_items:
        item = item.strip()
        if not item:
            continue
        if "m²" in item:
            m = re.search(r"([\d.]+)\s*m²", item)
            if m:
                val = m.group(1).replace(".", "")
                item_lower = item.lower()
                if "rüt" in item_lower or "gross" in item_lower:
                    result["gross_sqm"] = val
                elif "net" in item_lower:
                    result["net_sqm"] = val
                else:
                    if not result["gross_sqm"]:
                        result["gross_sqm"] = val
                    elif not result["net_sqm"]:
                        result["net_sqm"] = val
        elif re.match(r"^\d+\+\d+$", item):
            result["room_count"] = item
        elif item.lower() in ("stdyo", "studyo"):
            result["room_count"] = item
        elif re.search(r"kat", item, re.IGNORECASE):
            result["floor"] = item
    return result


def parse_location(loc_text: str) -> tuple[str, str, str]:
    if not loc_text:
        return "", "", ""
    parts = [p.strip() for p in loc_text.split(",")]
    city = parts[0] if len(parts) > 0 else ""
    county = parts[1] if len(parts) > 1 else ""
    district = parts[2] if len(parts) > 2 else ""
    return city, county, district


def extract_total_pages(soup: BeautifulSoup, html: str) -> int:
    """Toplam sayfa sayisini tespit et. Bulamazsa 1 dondur."""
    m = re.search(r'"totalCount"\s*:\s*(\d+)', html)
    if m:
        total_count = int(m.group(1))
        return max(1, (total_count + 19) // 20)

    m = re.search(r'Toplam\s+<b>([\d.]+)</b>', html)
    if not m:
        m = re.search(r'([\d.]+)\s*ilan\s*bulundu', html, re.IGNORECASE)
    if m:
        total_count = int(m.group(1).replace(".", ""))
        return max(1, (total_count + 19) // 20)

    for sel in [".resultCount strong", "strong.result-count", ".searchResultsHeader .resultCount"]:
        el = soup.select_one(sel)
        if el:
            m2 = re.search(r"[\d.]+", el.get_text())
            if m2:
                total_count = int(m2.group(0).replace(".", ""))
                return max(1, (total_count + 19) // 20)

    offsets = [int(x) for x in re.findall(r'pagingOffset=(\d+)', html)]
    if offsets:
        max_offset = max(offsets)
        return max(1, max_offset // 20 + 1)

    if soup.select_one('link[rel="next"]') or soup.select_one('.pageNavigator a.prevNextBut'):
        return 999

    return 1


def parse_detail_page(soup: BeautifulSoup) -> dict[str, str]:
    """Sahibinden ilan detay sayfasini parse edip eksik alanlari dondur.

    classifiedInfoList tablosundan:
      Net m², Kat, Kat Sayısı, Bina Yaşı, Emlak Tipi vb.
    userInfo / classifiedOwnerInfo bölümünden:
      advertiser_type, advertiser_name
    Görseller:
      image_count
    """
    detail: dict[str, str] = {}

    # --- classifiedInfoList tablosu ---
    info_list = soup.select_one("ul.classifiedInfoList")
    if info_list:
        for li in info_list.select("li"):
            label_el = li.select_one("strong")
            value_el = li.select_one("span")
            if not label_el or not value_el:
                continue
            label = label_el.get_text(strip=True).lower()
            value = value_el.get_text(strip=True)
            if not value or value == "-":
                continue
            if "net" in label and "m²" in label:
                m = re.search(r"([\d.]+)", value)
                if m:
                    detail["net_sqm"] = m.group(1).replace(".", "")
            elif "brüt" in label and "m²" in label:
                m = re.search(r"([\d.]+)", value)
                if m:
                    detail["gross_sqm"] = m.group(1).replace(".", "")
            elif label in ("bulunduğu kat", "bulundugu kat", "kat"):
                detail["floor"] = value
            elif "kat sayısı" in label or "kat sayisi" in label:
                detail["floor_count"] = value
            elif "bina yaşı" in label or "bina yasi" in label:
                detail["building_age"] = value
            elif "emlak tipi" in label:
                detail["property_type"] = value
            elif "oda sayısı" in label or "oda sayisi" in label:
                detail["room_count"] = value

    # Fallback: classifiedInfo tablo (eski yapı)
    if not info_list:
        for row_el in soup.select(".classifiedInfo li, .classifiedInfo tr"):
            texts = list(row_el.stripped_strings)
            if len(texts) >= 2:
                label = texts[0].lower()
                value = texts[1]
                if "net" in label and "m²" in label:
                    m = re.search(r"([\d.]+)", value)
                    if m:
                        detail["net_sqm"] = m.group(1).replace(".", "")
                elif label in ("bulunduğu kat", "bulundugu kat"):
                    detail["floor"] = value
                elif "kat sayısı" in label or "kat sayisi" in label:
                    detail["floor_count"] = value
                elif "bina yaşı" in label or "bina yasi" in label:
                    detail["building_age"] = value
                elif "emlak tipi" in label:
                    detail["property_type"] = value

    # --- Advertiser bilgileri ---
    # classifiedOwnerInfo veya userInfo bolumu
    owner_el = soup.select_one(".classifiedOwnerInfo .userName") or soup.select_one(".userInfo h5") or soup.select_one(".classifiedUserContent h5")
    if owner_el:
        detail["advertiser_name"] = owner_el.get_text(strip=True)

    # Emlakçı vs Sahibinden — badge/store göstergesi
    store_el = soup.select_one(".classifiedOwnerInfo .store-name") or soup.select_one(".classifiedOwnerInfo .real-estate-agent") or soup.select_one(".storeLink")
    if store_el:
        detail["advertiser_type"] = "Emlakçı"
        # Mağaza ismini advertiser_name olarak kullan
        store_name = store_el.get_text(strip=True)
        if store_name:
            detail["advertiser_name"] = store_name
    elif soup.select_one(".classifiedOwnerInfo .private-owner") or soup.select_one(".ownerBadge"):
        detail["advertiser_type"] = "Sahibinden"
    else:
        # Fallback: sayfa iceriginde "Emlak Ofisi" string'i var mi
        owner_section = soup.select_one(".classifiedOwnerInfo")
        if owner_section:
            owner_text = owner_section.get_text(strip=True).lower()
            if "emlak" in owner_text or "gayrimenkul" in owner_text:
                detail["advertiser_type"] = "Emlakçı"
            else:
                detail["advertiser_type"] = "Sahibinden"

    # --- Image count ---
    # Galeri thumbnails
    thumbs = soup.select(".classifiedDetailPhotos .thmbItem, .classifiedGallery img, #thumbListUl li")
    if thumbs:
        detail["image_count"] = str(len(thumbs))
    else:
        # Alternatif: fotoğraf sayısı badge'i
        photo_badge = soup.select_one(".classifiedDetailPhotos .photoCount, .photoCountBadge")
        if photo_badge:
            m = re.search(r"(\d+)", photo_badge.get_text(strip=True))
            if m:
                detail["image_count"] = m.group(1)

    return detail


# İlk detay sayfası dump kontrolü
_detail_dump_done = [False]


def _dump_detail_structure(html: str, url: str = ""):
    """İlk detay sayfası HTML yapısını 1 kez logla — parser debug için."""
    if _detail_dump_done[0]:
        return
    _detail_dump_done[0] = True
    soup = BeautifulSoup(html, "lxml")
    print(f"\n{'='*60}")
    print(f"[DETAIL_DUMP] URL: {url}")
    print(f"[DETAIL_DUMP] HTML uzunluk: {len(html)}")
    print(f"[DETAIL_DUMP] Title: {soup.title.get_text() if soup.title else '(yok)'}")

    # classifiedInfoList
    info_list = soup.select_one("ul.classifiedInfoList")
    if info_list:
        items = info_list.select("li")
        print(f"[DETAIL_DUMP] classifiedInfoList: {len(items)} li")
        for li in items[:10]:
            texts = list(li.stripped_strings)
            print(f"[DETAIL_DUMP]   li: {texts}")
    else:
        # Alternatif yapılar
        for sel in [".classifiedInfo", ".classified-detail-info", "[class*='classifiedInfo']"]:
            found = soup.select(sel)
            if found:
                print(f"[DETAIL_DUMP] '{sel}' -> {len(found)} eleman")
                print(f"[DETAIL_DUMP] snippet: {str(found[0])[:500]}")
                break
        else:
            print("[DETAIL_DUMP] classifiedInfoList / classifiedInfo bulunamadi")

    # Owner info
    owner = soup.select_one(".classifiedOwnerInfo")
    if owner:
        print(f"[DETAIL_DUMP] ownerInfo: {owner.get_text(strip=True)[:200]}")
    else:
        print("[DETAIL_DUMP] classifiedOwnerInfo bulunamadi")

    # Photo count
    thumbs = soup.select(".classifiedDetailPhotos .thmbItem, .classifiedGallery img, #thumbListUl li")
    print(f"[DETAIL_DUMP] photo thumbs: {len(thumbs)}")

    print(f"{'='*60}\n")


def fetch_detail_html(pw_page, url: str, delay: float = 2.0, max_retries: int = 3) -> str:
    """Detay sayfası HTML'ini al. CF challenge varsa bekle."""
    for attempt in range(1, max_retries + 1):
        try:
            resp = pw_page.goto(url, wait_until="domcontentloaded", timeout=30000)
        except Exception as exc:
            print(f"[DETAIL] Navigasyon hatasi ({attempt}/{max_retries}): {exc}")
            if attempt < max_retries:
                time.sleep(5 + attempt * 3)
            continue

        sc = resp.status if resp else 0
        cf_ok = _wait_cf_challenge(pw_page, max_wait=30)

        # Detay sayfası içeriğinin yüklenmesini bekle
        try:
            pw_page.wait_for_selector(
                ".classifiedInfoList, .classifiedInfo, .classifiedDetailTitle, h1",
                timeout=10000,
            )
        except Exception:
            pass

        time.sleep(1)
        html = pw_page.content()

        if sc == 429:
            print(f"[DETAIL] HTTP 429 – rate limit ({attempt}/{max_retries}), bekleniyor...")
            if attempt < max_retries:
                time.sleep(20 + attempt * 15)
            continue

        if sc in (403, 503):
            if len(html) > 20000:
                return html
            print(f"[DETAIL] HTTP {sc} ({attempt}/{max_retries}), bekleniyor...")
            if attempt < max_retries:
                time.sleep(10 + attempt * 5)
            continue

        if sc not in (200, 0):
            print(f"[DETAIL] Beklenmeyen HTTP {sc} ({attempt}/{max_retries})")
            if attempt < max_retries:
                time.sleep(5 + attempt * 2)
            continue

        # İlk detay sayfasının yapısını logla
        _dump_detail_structure(html, url)
        return html

    # Tüm denemeler 429 ise: callers yeni IP denesin
    raise _ProxyRateLimited(f"Rate limit asildi ({max_retries} deneme): {url}")


def parse_list_rows(soup: BeautifulSoup, category_name: str) -> list[dict]:
    """tr.searchResultsItem satirlarini parse et ve dict listesi dondur.

    2026 HTML yapisi:
      td[0] .searchResultsLargeThumbnail  (thumbnail link -> /ilan/...)
      td[1] .searchResultsTitleValue       (a.classifiedTitle -> /ilan/...)
      td[2] .searchResultsAttributeValue   (brüt m²)
      td[3] .searchResultsAttributeValue   (oda sayisi)
      td[4] .searchResultsPriceValue       (fiyat)
      td[5] .searchResultsDateValue        (tarih)
      td[6] .searchResultsLocationValue    (il / ilçe)
      td[7] .ignore-me
    """
    rows_data: list[dict] = []
    rows = soup.select("tr.searchResultsItem")

    for row in rows:
        listing_id = row.get("data-id", "")

        # --- URL + Title ---
        title_a = row.select_one("a.classifiedTitle")
        if not title_a:
            # Fallback: thumbnail link
            title_a = row.select_one("td.searchResultsLargeThumbnail a[href^='/ilan/']")
        href = title_a.get("href", "") if title_a else ""
        title = title_a.get("title", "") if title_a else ""
        if not title and title_a:
            title = title_a.get_text(strip=True)
        url = (BASE_URL + href) if href.startswith("/") else href

        # --- Price ---
        price_el = row.select_one("td.searchResultsPriceValue")
        price_raw = price_el.get_text(strip=True) if price_el else ""
        price, currency = parse_price(price_raw)

        # --- Location (il/ilçe/mahalle) ---
        loc_el = row.select_one("td.searchResultsLocationValue")
        if loc_el:
            # İç elementler br ile ayrılmış olabilir – stripped_strings kullan
            loc_parts = list(loc_el.stripped_strings)
        else:
            loc_parts = []
        city = loc_parts[0] if len(loc_parts) > 0 else ""
        county = loc_parts[1] if len(loc_parts) > 1 else ""
        district = loc_parts[2] if len(loc_parts) > 2 else ""

        # --- Date ---
        date_el = row.select_one("td.searchResultsDateValue")
        if date_el:
            date_parts = list(date_el.stripped_strings)
            listing_date = " ".join(date_parts)
        else:
            listing_date = ""

        # --- Attributes (brüt m², oda sayısı) ---
        attr_tds = row.select("td.searchResultsAttributeValue")
        gross_sqm = ""
        room_count = ""
        for atd in attr_tds:
            val = atd.get_text(strip=True)
            if not val:
                continue
            if re.match(r"^[\d.]+$", val):
                # Saf rakam – brüt m²
                if not gross_sqm:
                    gross_sqm = val.replace(".", "")
                else:
                    # İkinci saf rakam (net m² vb.) – şimdilik atla
                    pass
            elif re.match(r"^\d+\+\d+$", val) or val.lower() in ("stüdyo", "stdyo", "studyo"):
                room_count = val
            elif "m²" in val:
                m = re.search(r"([\d.]+)", val)
                if m and not gross_sqm:
                    gross_sqm = m.group(1).replace(".", "")

        rows_data.append({
            "url": url,
            "listing_id": listing_id,
            "title": title,
            "category": category_name,
            "property_type": "",
            "advertiser_type": "",
            "advertiser_name": "",
            "city": city,
            "county": county,
            "district": district,
            "price": price,
            "currency": currency,
            "gross_sqm": gross_sqm,
            "net_sqm": "",
            "room_count": room_count,
            "floor": "",
            "floor_count": "",
            "building_age": "",
            "listing_date": listing_date,
            "image_count": "",
            "scraped_at_utc": utc_now_iso(),
        })

    return rows_data


# ---------------------------------------------------------------------------
# URL helpers
# ---------------------------------------------------------------------------

def listing_page_url(base_url: str, page: int) -> str:
    """Page 1 -> base_url, Page N -> base_url?pagingOffset=(N-1)*20"""
    if page <= 1:
        return base_url
    offset = (page - 1) * 20
    sep = "&" if "?" in base_url else "?"
    return f"{base_url}{sep}pagingOffset={offset}"


# ---------------------------------------------------------------------------
# Checkpoint / CSV helpers
# ---------------------------------------------------------------------------

def load_checkpoint(path: Path) -> dict:
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def save_checkpoint(path: Path, checkpoint: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(checkpoint, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def append_csv_row(path: Path, row: dict, lock: threading.Lock) -> None:
    with lock:
        path.parent.mkdir(parents=True, exist_ok=True)
        file_exists = path.exists()
        with path.open("a", newline="", encoding="utf-8-sig") as fh:
            writer = csv.DictWriter(fh, fieldnames=COLUMNS, extrasaction="ignore")
            if not file_exists:
                writer.writeheader()
            writer.writerow({c: row.get(c, "") for c in COLUMNS})


# ---------------------------------------------------------------------------
# Page-range utilities
# ---------------------------------------------------------------------------

def parse_page_ranges(raw: str | None) -> list[tuple[int, int]]:
    if not raw:
        return []
    ranges: list[tuple[int, int]] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" not in part:
            raise SystemExit(f"Gecersiz page range: {part!r}")
        start_s, end_s = part.split("-", 1)
        start = int(start_s.strip())
        end = int(end_s.strip())
        if start <= 0 or end < start:
            raise SystemExit(f"Gecersiz page range: {part!r}")
        ranges.append((start, end))
    return ranges


def build_page_ranges(total_pages: int, workers: int) -> list[tuple[int, int]]:
    if total_pages <= 0:
        return [(1, 1)]
    workers = max(1, min(workers, total_pages))
    base = total_pages // workers
    extra = total_pages % workers
    ranges: list[tuple[int, int]] = []
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
) -> list[tuple[int, int]]:
    clamped: list[tuple[int, int]] = []
    for start, end in ranges:
        if start > total_pages:
            continue
        clamped.append((start, min(end, total_pages)))
    return clamped


# ---------------------------------------------------------------------------
# Main scraping logic
# ---------------------------------------------------------------------------

def detect_total_pages(
    pw_page,
    category_name: str,
    base_url: str,
    delay: float,
) -> int:
    """Ilk sayfa yukleyerek toplam sayfa sayisini tespit et."""
    url = listing_page_url(base_url, 1)
    print(f"[SAHIBINDEN:{category_name}] Toplam sayfa tespiti: {url}")
    html = fetch_page_html(pw_page, url, delay=delay)
    soup = BeautifulSoup(html, "lxml")
    total = extract_total_pages(soup, html)
    print(f"[SAHIBINDEN:{category_name}] Tespit edilen toplam sayfa: {total}")
    return total


def scrape_list_only(
    categories: list[str],
    csv_path: Path,
    page_workers: int,
    delay: float,
    headless: bool,  # Playwright her zaman headless
    page_ranges: list[tuple[int, int]],
) -> None:
    checkpoint_path = csv_path.with_suffix("").with_suffix(".checkpoint.json")
    checkpoint = load_checkpoint(checkpoint_path)
    list_done_pages: dict[str, list[int]] = checkpoint.get("list_done_pages", {})
    meta: dict = checkpoint.get("meta", {})
    csv_lock = threading.Lock()
    page_lock = threading.Lock()

    for category_name in categories:
        if category_name not in CATEGORIES:
            raise SystemExit(
                f"Bilinmeyen kategori: {category_name!r}. "
                f"Gecerli kategoriler: {', '.join(CATEGORIES)}"
            )

        base_url = CATEGORIES[category_name]

        # Toplam sayfa tespiti
        bootstrap_ctx, bootstrap_page = create_working_context(f":{category_name}")
        total_pages = detect_total_pages(bootstrap_page, category_name, base_url, delay)
        try:
            bootstrap_ctx.close()
        except Exception:
            pass

        meta[category_name] = {
            "total_pages": total_pages,
            "mode": "list_only",
            "updated_at_utc": utc_now_iso(),
        }
        checkpoint["meta"] = meta
        checkpoint["list_done_pages"] = list_done_pages
        save_checkpoint(checkpoint_path, checkpoint)

        done_pages: set[int] = {int(x) for x in list_done_pages.get(category_name, [])}

        if page_ranges:
            ranges = clamp_page_ranges(page_ranges, total_pages)
        else:
            ranges = build_page_ranges(total_pages, page_workers)

        print(
            f"[SAHIBINDEN:{category_name}] LIST_ONLY "
            f"| Toplam sayfa: {total_pages} "
            f"| Page workers: {page_workers}"
        )
        print(f"[SAHIBINDEN:{category_name}] Page worker araliklari: {ranges}")

        def run_range(
            start: int,
            end: int,
            _cat: str = category_name,
            _base: str = base_url,
        ) -> int:
            rng_ctx, pw_page = create_working_context(f":{_cat}")
            written = 0
            for page in range(start, end + 1):
                with page_lock:
                    if page in done_pages:
                        continue
                url = listing_page_url(_base, page)
                try:
                    html = fetch_page_html(pw_page, url, delay=delay)
                except Exception as exc:
                    print(f"[SAHIBINDEN:{_cat}] Sayfa {page} atlandi: {exc}")
                    time.sleep(delay + 2 + random.uniform(0, 1.0))
                    continue

                soup = BeautifulSoup(html, "lxml")
                rows = parse_list_rows(soup, _cat)

                # 0 satir – retry
                if not rows:
                    print(f"[SAHIBINDEN:{_cat}] Sayfa {page}: 0 satir, 5s beklenip tekrar deneniyor...")
                    time.sleep(5)
                    try:
                        html = fetch_page_html(pw_page, url, delay=delay)
                        soup = BeautifulSoup(html, "lxml")
                        rows = parse_list_rows(soup, _cat)
                    except Exception:
                        pass

                fresh = 0
                for row in rows:
                    if not row["url"]:
                        continue
                    append_csv_row(csv_path, row, csv_lock)
                    fresh += 1
                    written += 1

                if rows and fresh == 0:
                    print(f"[SAHIBINDEN:{_cat}] {len(rows)} row bulundu ama hepsi url-bos!")

                with page_lock:
                    done_pages.add(page)
                    list_done_pages[_cat] = sorted(done_pages)
                    checkpoint["list_done_pages"] = list_done_pages
                    checkpoint["meta"] = meta
                    save_checkpoint(checkpoint_path, checkpoint)

                print(
                    f"[SAHIBINDEN:{_cat}] Sayfa {page}/{total_pages} "
                    f"| {fresh} satir yazildi"
                )
                time.sleep(delay + random.uniform(0, 0.5))
            try:
                rng_ctx.close()
            except Exception:
                pass
            return written

        effective_workers = min(page_workers, len(ranges)) if ranges else 1
        with ThreadPoolExecutor(max_workers=effective_workers or 1) as exe:
            futures = [exe.submit(run_range, start, end) for start, end in ranges]
            total_written = sum(fut.result() for fut in as_completed(futures))

        print(f"[SAHIBINDEN:{category_name}] LIST_ONLY tamamlandi | {total_written} satir yazildi")


def scrape_with_details(
    categories: list[str],
    csv_path: Path,
    page_workers: int,
    delay: float,
    headless: bool,
    page_ranges: list[tuple[int, int]],
) -> None:
    """İki aşamalı scraping: Önce liste sayfalarından URL topla, sonra detay çek.

    Aşama 1: scrape_list_only ile tüm liste sayfalarını tara, URL'leri CSV'ye yaz.
    Aşama 2: CSV'deki her URL için detay sayfasını ziyaret et, eksik alanları doldur.
    """
    # --- Aşama 1: Liste sayfalarından URL toplama ---
    print("[SAHIBINDEN] ===== ASAMA 1: Liste sayfalari taraniyor =====")
    scrape_list_only(
        categories=categories,
        csv_path=csv_path,
        page_workers=page_workers,
        delay=delay,
        headless=headless,
        page_ranges=page_ranges,
    )

    # Liste fazindan sonra kisa dinlenme – rate-limit etkisini azaltir
    _PHASE_PAUSE = 60
    print(f"[SAHIBINDEN] Liste -> Detay arasinda {_PHASE_PAUSE}s bekleniyor...")
    time.sleep(_PHASE_PAUSE)

    # --- Aşama 2: Detay sayfalarını scrape et ---
    print("[SAHIBINDEN] ===== ASAMA 2: Detay sayfalari taraniyor =====")

    if not csv_path.exists():
        print("[SAHIBINDEN] CSV dosyasi bulunamadi, detay asamasi atlaniyor.")
        return

    # CSV'deki tüm satırları oku
    with csv_path.open("r", encoding="utf-8-sig") as fh:
        rows = list(csv.DictReader(fh))

    if not rows:
        print("[SAHIBINDEN] CSV bos, detay asamasi atlaniyor.")
        return

    total_rows = len(rows)
    print(f"[SAHIBINDEN] Toplam {total_rows} ilan icin detay cekilecek")

    # Checkpoint: detay sayfasi tamamlanan URL'ler
    checkpoint_path = csv_path.with_suffix("").with_suffix(".detail_checkpoint.json")
    detail_chk = load_checkpoint(checkpoint_path)
    done_urls: set[str] = set(detail_chk.get("done_urls", []))
    print(f"[SAHIBINDEN] Daha once tamamlanmis: {len(done_urls)} detay")

    # Filtrenin gerekliligi: eksik alanı olan satırlar
    pending_indices: list[int] = []
    for i, row in enumerate(rows):
        url = row.get("url", "")
        if not url or url in done_urls:
            continue
        # Detay alanlarından en az biri boşsa çekilmeli
        if not row.get("net_sqm") or not row.get("advertiser_name") or not row.get("floor"):
            pending_indices.append(i)

    if not pending_indices:
        print("[SAHIBINDEN] Tum detaylar zaten mevcut, atlaniyor.")
        return

    print(f"[SAHIBINDEN] {len(pending_indices)} ilan icin detay cekilecek")

    _ROTATE_EVERY = 20  # Her 20 detayda bir proaktif IP rotasyonu

    def _new_detail_ctx():
        """Warmup olmadan direkt context acilir – detail icin homepage'e gitmeye gerek yok."""
        proxy_url = _get_proxy_url()
        c, p = create_context(proxy_url=proxy_url)
        _p = proxy_url[:50] if proxy_url else "direkt"
        print(f"[DETAIL] Yeni context (no-warmup) | proxy={_p}")
        return c, p

    # Browser context olustur (warmup yok)
    ctx, pw_page = _new_detail_ctx()

    def _rotate_ctx(label_suffix=""):
        nonlocal ctx, pw_page
        try:
            ctx.close()
        except Exception:
            pass
        ctx, pw_page = _new_detail_ctx()

    enriched_count = 0
    for seq, idx in enumerate(pending_indices, 1):
        # Proaktif IP rotasyonu
        if seq > 1 and (seq - 1) % _ROTATE_EVERY == 0:
            print(f"[DETAIL] Periyodik IP rotasyonu (her {_ROTATE_EVERY} ilanda bir)...")
            _rotate_ctx()

        row = rows[idx]
        url = row.get("url", "")
        if not url:
            continue

        # Detay çekme – rate limit'te 1 kez IP rotasyonu yap
        html = None
        for ip_try in range(2):
            try:
                html = fetch_detail_html(pw_page, url, delay=delay)
                break
            except _ProxyRateLimited:
                if ip_try == 0:
                    print(f"[DETAIL] [{seq}/{len(pending_indices)}] Rate limit – yeni IP deneniyor...")
                    _rotate_ctx(f"_ratelimit{seq}")
                else:
                    print(f"[DETAIL] [{seq}/{len(pending_indices)}] Rate limit devam ediyor, atlaniyor: {url[:60]}")
            except Exception as exc:
                print(f"[DETAIL] [{seq}/{len(pending_indices)}] HATA {url[:60]}... -> {exc}")
                break

        if html is None:
            time.sleep(delay + random.uniform(0.5, 1.5))
            continue

        try:
            soup = BeautifulSoup(html, "lxml")
            detail = parse_detail_page(soup)

            # Mevcut satırdaki boş alanları doldur
            for key, value in detail.items():
                if value and not row.get(key):
                    rows[idx][key] = value

            enriched_count += 1
            done_urls.add(url)

            # Checkpoint + CSV kaydet (her 10 detayda bir)
            if enriched_count % 10 == 0:
                detail_chk["done_urls"] = sorted(done_urls)
                save_checkpoint(checkpoint_path, detail_chk)
                csv_path.parent.mkdir(parents=True, exist_ok=True)
                with csv_path.open("w", newline="", encoding="utf-8-sig") as _fh:
                    _writer = csv.DictWriter(_fh, fieldnames=COLUMNS, extrasaction="ignore")
                    _writer.writeheader()
                    for _row in rows:
                        _writer.writerow({c: _row.get(c, "") for c in COLUMNS})
                print(f"[DETAIL] Ara kayit: {enriched_count} detay CSV'ye yazildi")

            print(
                f"[DETAIL] [{seq}/{len(pending_indices)}] OK "
                f"| {url[:60]}... | alanlar: {list(detail.keys())}"
            )
        except Exception as exc:
            print(f"[DETAIL] [{seq}/{len(pending_indices)}] PARSE HATA {url[:60]}... -> {exc}")

        time.sleep(delay + random.uniform(0.5, 1.5))

    # Browser kapat
    try:
        ctx.close()
    except Exception:
        pass

    # Final checkpoint
    detail_chk["done_urls"] = sorted(done_urls)
    save_checkpoint(checkpoint_path, detail_chk)

    # Zenginleştirilmiş CSV'yi yeniden yaz
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({c: row.get(c, "") for c in COLUMNS})

    print(f"[SAHIBINDEN] DETAY tamamlandi | {enriched_count}/{len(pending_indices)} ilan zenginlestirildi")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Sahibinden.com satilik/kiralik scraper (Playwright CF bypass)"
    )
    parser.add_argument(
        "--mode",
        choices=["list_only", "list_and_detail"],
        default="list_and_detail",
        help="list_only: sadece liste | list_and_detail: liste + detay",
    )
    parser.add_argument(
        "--categories",
        default="satilik,kiralik",
        help="Virgulle ayrilmis kategori listesi",
    )
    parser.add_argument(
        "--csv",
        default="data/sahibinden_listings.csv",
        help="CSV cikti yolu",
    )
    parser.add_argument(
        "--page-workers",
        type=int,
        default=1,
        help="Liste sayfasi worker sayisi",
    )
    parser.add_argument(
        "--page-ranges",
        default="",
        help="Ornek: 1-100 veya 1-50,51-100",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=3.0,
        help="Istekler arasi taban bekleme saniyesi",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        default=False,
        help="Headless mod (varsayilan: True)",
    )
    parser.add_argument(
        "--cookies",
        default="",
        help="EditThisCookie JSON formatinda cerez dosyasi yolu (orn: cookies.json)",
    )
    args = parser.parse_args()

    # Cerez dosyasi yukle
    if args.cookies:
        cookies_path = Path(args.cookies)
        if cookies_path.exists():
            with open(cookies_path, encoding="utf-8") as f:
                loaded = json.load(f)
            if isinstance(loaded, list):
                SESSION_COOKIES.extend(loaded)
                print(f"[SAHIBINDEN] {len(loaded)} cerez yuklendi: {cookies_path}")
            else:
                print(f"[SAHIBINDEN] UYARI: {cookies_path} gecerli JSON listesi degil, atlanacak.")
        else:
            print(f"[SAHIBINDEN] UYARI: Cerez dosyasi bulunamadi: {cookies_path}")

    categories = [x.strip() for x in args.categories.split(",") if x.strip()]
    page_ranges = parse_page_ranges(args.page_ranges)

    scrape_fn = scrape_with_details if args.mode == "list_and_detail" else scrape_list_only
    scrape_fn(
        categories=categories,
        csv_path=Path(args.csv),
        page_workers=args.page_workers,
        delay=args.delay,
        headless=args.headless,
        page_ranges=page_ranges,
    )


if __name__ == "__main__":
    main()