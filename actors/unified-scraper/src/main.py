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
    return "generic"


# ---------------------------------------------------------------------------
# Tekil URL scrape (thread'de çalışır)
# ---------------------------------------------------------------------------

def _scrape_sync(url: str, scraper_name: str, cookies: list) -> dict | None:
    """Bloklayan scrape işlemi — asyncio.to_thread ile çalıştırılır."""
    try:
        if scraper_name == "emlakjet":
            raw = emlakjet_detail.scrape_url(url)
        elif scraper_name == "hepsiemlak":
            raw = hepsiemlak_detail.scrape_url(url)
        elif scraper_name == "sahibinden":
            raw = sahibinden_detail.scrape_url(url, cookies=cookies)
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

        Actor.log.info(
            f"Başlıyor | {len(urls)} URL | concurrency={concurrency} | delay={delay}s"
        )

        sem = asyncio.Semaphore(concurrency)

        async def _scrape_one(url: str) -> dict | None:
            async with sem:
                name = _get_scraper(url)
                result = await asyncio.to_thread(_scrape_sync, url, name, cookies)
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
