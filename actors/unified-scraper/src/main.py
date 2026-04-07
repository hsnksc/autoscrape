"""
main.py — Unified Turkish Real Estate Scraper — Apify Actor giriş noktası.

Input:
  urls              : list[str]  — ilan detay URL'leri
  webhookUrl        : str        — sonuçların POST edileceği AutoScrape URL
  jobId             : str        — AutoScrape job ID (webhook payload'a eklenir)
  sahibindenCookies : str        — Sahibinden CF bypass çerezleri (JSON string)
  concurrency       : int        — eş zamanlı worker (varsayılan: 3)
  requestDelay      : float      — worker başlangıçları arası gecikme (varsayılan: 1.5s)

Akış:
  URL → domain tespiti → uygun scraper → normalize → Apify Dataset push
  Tümü tamamlanınca → webhookUrl'e POST (jobId + listings)
"""
from __future__ import annotations

import asyncio
import json
import sys
import urllib.request
from pathlib import Path
from urllib.parse import urlparse

# ── Kaynak scraper dizinleri artık gerekmez (requests tabanlı) ──────────────
_SRC = Path(__file__).parent
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from apify import Actor  # noqa: E402

import emlakjet_detail    # noqa: E402
import hepsiemlak_detail  # noqa: E402
import sahibinden_detail  # noqa: E402
import generic_detail     # noqa: E402
import shb_detail         # noqa: E402
import remax_detail       # noqa: E402
from normalize import to_autoscrape_schema  # noqa: E402


# ---------------------------------------------------------------------------
# Domain → scraper routing
# ---------------------------------------------------------------------------

def _domain(url: str) -> str:
    try:
        return (urlparse(url).hostname or "").replace("www.", "").lower()
    except Exception:
        return ""


def _get_scraper(url: str):
    d = _domain(url)
    if "emlakjet.com" in d:
        return "emlakjet"
    if "hepsiemlak.com" in d:
        return "hepsiemlak"
    if "sahibinden.com" in d:
        return "sahibinden"
    if any(x in d for x in ("cb.com.tr", "century21.com.tr", "era.com.tr")):
        return "shb"
    if "remax.com.tr" in d:
        return "remax"
    return "generic"


def _is_hepsiemlak_search_page(url: str) -> bool:
    """Hepsiemlak arama/liste sayfası mı? (detay sayfası değil)"""
    from urllib.parse import urlparse
    path = urlparse(url).path
    return "hepsiemlak.com" in url and "/ilan/" not in path


def _expand_hepsiemlak_search(url: str, proxy_url: str | None) -> list[str]:
    """
    Hepsiemlak arama URL'inden ilan detay URL'lerini çıkar.
    Nuxt.js SPA olduğu için HTML parse yerine Playwright DOM evaluate kullan.
    """
    from playwright_fetch import fetch_links_sync
    # SPA sonrası DOM'dan /ilan/ içeren a[href] linklerini al
    raw_links = fetch_links_sync(
        url,
        link_selector="a[href*='/ilan/']",
        proxy_url=proxy_url,
        timeout_ms=50_000,
    )
    print(f"[HEPSIEMLAK-DOM] {url[:60]} → {len(raw_links)} raw link")
    # Temizle: sadece hepsiemlak /ilan/ linkleri
    found = []
    seen = set()
    for href in raw_links:
        if not href:
            continue
        # Absolute URL oluştur
        if href.startswith("/"):
            href = "https://www.hepsiemlak.com" + href
        # Navigasyon/kategori linklerini atla, ilana git
        if "/ilan/" in href and href not in seen:
            # Sadece detay URL'leri: /ilan/xxx-NNN formatı
            from urllib.parse import urlparse as _up
            path = _up(href).path
            parts = path.strip("/").split("/")
            # /ilan/slug-ID123 veya daha derin path — en az bir segment /ilan/'dan sonra
            if len(parts) >= 2 and parts[0] == "ilan":
                seen.add(href)
                found.append(href)
    print(f"[HEPSIEMLAK] Arama sayfası {url[:60]} → {len(found)} ilan URL")
    return found


def _is_sahibinden_search_page(url: str) -> bool:
    """Sahibinden arama/liste sayfası mı? (ilan detay sayfası değil)"""
    from urllib.parse import urlparse
    path = urlparse(url).path
    if "sahibinden.com" not in url:
        return False
    # Detay sayfası: /ilan/... ile başlar
    if "/ilan/" in path:
        return False
    # Arama sayfaları: /satilik-daire/, /kiralik-daire/, /emlak- ile başlar vb.
    return True


def _expand_sahibinden_search(url: str, proxy_url: str | None) -> list[str]:
    """
    Sahibinden arama URL'inden ilan detay URL'lerini çıkar (Playwright DOM).
    Sahibinden da SPA benzeri yapıda; HTML parse yerine DOM evaluate kullan.
    """
    from playwright_fetch import fetch_links_sync
    raw_links = fetch_links_sync(
        url,
        link_selector="a[href*='/ilan/']",
        proxy_url=proxy_url,
        timeout_ms=50_000,
    )
    print(f"[SAHIBINDEN-DOM] {url[:60]} → {len(raw_links)} raw link")
    found = []
    seen = set()
    for href in raw_links:
        if not href:
            continue
        if href.startswith("/"):
            href = "https://www.sahibinden.com" + href
        if "/ilan/" in href and href not in seen:
            seen.add(href)
            found.append(href)
    print(f"[SAHIBINDEN] Arama sayfası {url[:60]} → {len(found)} ilan URL")
    return found


# ---------------------------------------------------------------------------
# Tekil URL scrape (thread'de çalışır)
# ---------------------------------------------------------------------------

def _scrape_sync(url: str, scraper_name: str, cookies: list, proxy_url: str | None = None) -> dict | None:
    """Bloklayan scrape işlemi — asyncio.to_thread ile çalıştırılır."""
    try:
        if scraper_name == "emlakjet":
            raw = emlakjet_detail.scrape_url(url)
        elif scraper_name == "hepsiemlak":
            raw = hepsiemlak_detail.scrape_url(url, proxy_url=proxy_url)
        elif scraper_name == "sahibinden":
            raw = sahibinden_detail.scrape_url(url, cookies=cookies, proxy_url=proxy_url)
        elif scraper_name == "shb":
            raw = shb_detail.scrape_url(url)
        elif scraper_name == "remax":
            raw = remax_detail.scrape_url(url, proxy_url=proxy_url)
        else:
            raw = generic_detail.scrape_url(url)

        if raw:
            return to_autoscrape_schema(raw, url)
    except Exception as exc:
        print(f"[UNIFIED] Scrape hatası ({scraper_name}, {url[:60]}): {exc}")
    return None


# ---------------------------------------------------------------------------
# Webhook POST
# ---------------------------------------------------------------------------

def _post_webhook(webhook_url: str, job_id: str, listings: list) -> None:
    payload = json.dumps({
        "jobId":    job_id,
        "listings": listings,
        "total":    len(listings),
    }).encode("utf-8")
    req = urllib.request.Request(
        webhook_url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        print(f"[UNIFIED] Webhook yanıtı: {resp.status} → {webhook_url}")


# ---------------------------------------------------------------------------
# Async main
# ---------------------------------------------------------------------------

async def main() -> None:
    async with Actor:
        inp = await Actor.get_input() or {}

        urls: list[str]    = inp.get("urls") or []
        webhook_url: str   = inp.get("webhookUrl") or ""
        job_id: str        = inp.get("jobId") or ""
        cookies_json: str  = inp.get("sahibindenCookies") or ""
        concurrency: int   = max(1, int(inp.get("concurrency") or 3))
        delay: float       = float(inp.get("requestDelay") or 1.5)

        # Sahibinden çerezlerini parse et
        cookies: list = []
        if cookies_json:
            try:
                cookies = json.loads(cookies_json)
            except Exception:
                Actor.log.warning("sahibindenCookies JSON parse hatası — çerez devre dışı")

        # Residential proxy konfigürasyonu (hepsiemlak, sahibinden, remax için)
        use_proxy: bool = inp.get("useProxy", True)
        proxy_config = None
        if use_proxy:
            try:
                proxy_config = await Actor.create_proxy_configuration(
                    groups=["RESIDENTIAL"],
                    country_code="TR",
                )
                Actor.log.info("Residential proxy (TR) aktif")
            except Exception as exc:
                Actor.log.warning(f"Proxy yapılandırma hatası, proxy devre dışı: {exc}")

        # Proxy gerektiren domain'ler
        _PROXY_DOMAINS = {"hepsiemlak.com", "sahibinden.com", "remax.com.tr"}

        # Hepsiemlak arama sayfalarını genişlet (arama URL → detay URL'leri)
        expanded_urls: list[str] = []
        for url in urls:
            if _is_hepsiemlak_search_page(url):
                proxy_url_hp = None
                if proxy_config:
                    try:
                        proxy_url_hp = await proxy_config.new_url()
                    except Exception:
                        pass
                detail_urls = await asyncio.to_thread(
                    _expand_hepsiemlak_search, url, proxy_url_hp
                )
                expanded_urls.extend(detail_urls)
            elif _is_sahibinden_search_page(url):
                proxy_url_shb = None
                if proxy_config:
                    try:
                        proxy_url_shb = await proxy_config.new_url()
                    except Exception:
                        pass
                detail_urls = await asyncio.to_thread(
                    _expand_sahibinden_search, url, proxy_url_shb
                )
                expanded_urls.extend(detail_urls)
            else:
                expanded_urls.append(url)

        # Dedupe
        seen_exp = set()
        urls = [u for u in expanded_urls if not (u in seen_exp or seen_exp.add(u))]
        Actor.log.info(f"URL genişletme sonrası: {len(urls)} URL")

        Actor.log.info(
            f"Başlıyor | {len(urls)} URL | concurrency={concurrency} | delay={delay}s"
        )

        sem = asyncio.Semaphore(concurrency)

        async def _scrape_one(url: str) -> dict | None:
            async with sem:
                name = _get_scraper(url)
                # Proxy gerektiren siteler için yeni proxy URL al
                proxy_url: str | None = None
                if proxy_config and any(d in url for d in _PROXY_DOMAINS):
                    try:
                        proxy_url = await proxy_config.new_url()
                    except Exception:
                        pass
                result = await asyncio.to_thread(_scrape_sync, url, name, cookies, proxy_url)
                await asyncio.sleep(delay)
                return result

        tasks = [_scrape_one(url) for url in urls]
        raw_results = await asyncio.gather(*tasks, return_exceptions=True)

        _INVALID_TITLES = ("410", "sayfa artık mevcut değil", "not found", "page not found")

        listings = []
        for r in raw_results:
            if isinstance(r, dict):
                title_lower = (r.get("title") or "").lower()
                if any(t in title_lower for t in _INVALID_TITLES):
                    Actor.log.info(f"Geçersiz ilan atlandı: {r.get('url','')[:60]}")
                    continue
                listings.append(r)
            elif isinstance(r, Exception):
                Actor.log.warning(f"Görev hatası: {r}")

        Actor.log.info(f"Tamamlandı: {len(listings)}/{len(urls)} ilan başarıyla scrape edildi")

        if listings:
            await Actor.push_data(listings)

        # Webhook POST
        if webhook_url:
            try:
                await asyncio.to_thread(_post_webhook, webhook_url, job_id, listings)
            except Exception as exc:
                Actor.log.error(f"Webhook gönderilemedi: {exc}")
        else:
            Actor.log.warning("webhookUrl belirtilmedi — sonuçlar sadece Apify Dataset'e yazıldı")


if __name__ == "__main__":
    asyncio.run(main())
