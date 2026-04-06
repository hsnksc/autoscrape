"""
main.py — Apify Actor giriş noktası.

Desteklenen kaynaklar:
  cb, century21, era, realtyworld, remax, turyap

Input parametreleri (.actor/input_schema.json ile eşleşmeli):
  sources       : list[str]  — Çalıştırılacak kaynaklar (varsayılan: hepsi)
  maxPages      : int        — Sayfa limiti, 0 = sınırsız
  requestDelay  : float      — İstekler arası bekleme (saniye)
  headless      : bool       — Selenium headless modu
"""
from __future__ import annotations

import asyncio
import json
import threading
from pathlib import Path

from apify import Actor
from crawlee.events import Event

from .checkpoint import load_checkpoint, save_checkpoint
from .models import CanonicalListing


class _GracefulAbort(BaseException):
    """Apify 'aborting' sinyali alındığında scraper döngüsünü temiz kapatmak için kullanılır."""

# ---------------------------------------------------------------------------
# Kaynak → Scraper eşlemesi (import döngüsünü önlemek için geç import)
# ---------------------------------------------------------------------------
_ALL_SOURCES = ["cb", "century21", "era", "realtyworld", "remax", "turyap"]

# Geçici depolama dizini (Apify içinde /tmp okunabilir+yazılabilir)
_DATA_DIR = Path("/tmp/shb-portal-sync")


def _data_path(filename: str) -> Path:
    _DATA_DIR.mkdir(parents=True, exist_ok=True)
    return _DATA_DIR / filename


# ---------------------------------------------------------------------------
# Apify KV Store: checkpoint anahtarları ve yardımcı fonksiyon
# (runs arası checkpoint kalıcılığı için — /tmp her yeni run'da temizlenir)
# ---------------------------------------------------------------------------
_KV_STORE_NAME = "turkish-scraper-checkpoints"
_CP_KEYS = [
    "cb_latest.checkpoint.json",
    "century21_latest.checkpoint.json",
    "era_latest.checkpoint.json",
    "realtyworld_latest.checkpoint.json",
    "remax_latest.checkpoint.json",
    "turyap_latest.checkpoint.json",
]


async def _sync_to_kv(kvs) -> None:
    """Mevcut /tmp checkpoint dosyalarını Apify KV Store'a senkronize eder."""
    for key in _CP_KEYS:
        path = _data_path(key)
        if path.exists():
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                await kvs.set_value(key, data)
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Her kaynak için scrape_all çağrıları
# ---------------------------------------------------------------------------
def _run_cb(max_pages: int, delay: float, concurrency: int, on_item):
    from .cb_scraper import scrape_all
    scrape_all(
        csv_path        = _data_path("cb_latest.csv"),
        checkpoint_path = _data_path("cb_latest.checkpoint.json"),
        max_pages       = max_pages,
        delay           = delay,
        concurrency     = concurrency,
        on_item         = on_item,
    )


def _run_century21(max_pages: int, delay: float, concurrency: int, on_item):
    from .century21_scraper import scrape_all
    scrape_all(
        csv_path        = _data_path("century21_latest.csv"),
        checkpoint_path = _data_path("century21_latest.checkpoint.json"),
        max_pages       = max_pages,
        delay           = delay,
        concurrency     = concurrency,
        on_item         = on_item,
    )


def _run_era(max_pages: int, delay: float, concurrency: int, on_item):
    from .era_scraper import scrape_all
    scrape_all(
        csv_path        = _data_path("era_latest.csv"),
        checkpoint_path = _data_path("era_latest.checkpoint.json"),
        max_pages       = max_pages,
        delay           = delay,
        concurrency     = concurrency,
        on_item         = on_item,
    )


def _run_realtyworld(max_pages: int, delay: float, concurrency: int, on_item):
    from .realtyworld_scraper import scrape_all
    scrape_all(
        csv_path        = _data_path("realtyworld_latest.csv"),
        checkpoint_path = _data_path("realtyworld_latest.checkpoint.json"),
        max_pages       = max_pages,
        delay           = delay,
        concurrency     = concurrency,
        on_item         = on_item,
    )


def _run_remax(max_pages: int, delay: float, headless: bool, on_item, cookies_json: str = "", proxy_url: str | None = None):
    from .remax_scraper import scrape_all
    scrape_all(
        db_path         = _data_path("remax_latest.sqlite3"),
        checkpoint_path = _data_path("remax_latest.checkpoint.json"),
        max_pages       = max_pages,
        delay           = delay,
        headless        = headless,
        cookies_json    = cookies_json,
        proxy_url       = proxy_url,
        on_item         = on_item,
    )


def _run_turyap(max_pages: int, delay: float, headless: bool, on_item):
    from .turyap_scraper import scrape_all
    scrape_all(
        csv_path        = _data_path("turyap_latest.csv"),
        checkpoint_path = _data_path("turyap_latest.checkpoint.json"),
        max_pages       = max_pages,
        delay           = delay,
        headless        = headless,
        on_item         = on_item,
    )


# ---------------------------------------------------------------------------
# Actor main
# ---------------------------------------------------------------------------
async def main() -> None:
    async with Actor:
        inp = await Actor.get_input() or {}

        sources: list[str]  = inp.get("sources", _ALL_SOURCES) or _ALL_SOURCES
        max_pages: int       = int(inp.get("maxPages", 0))
        delay: float         = float(inp.get("requestDelay", 1.5))
        headless: bool       = bool(inp.get("headless", True))
        concurrency: int     = int(inp.get("concurrency", 4))

        # Remax için proxy URL — Cloudflare bypassı için residential proxy gerekli
        remax_proxy_url: str | None = None
        _raw_proxy = inp.get("proxyConfiguration")
        if _raw_proxy:
            try:
                _proxy_cfg = await Actor.create_proxy_configuration(actor_proxy_input=_raw_proxy)
                if _proxy_cfg:
                    remax_proxy_url = await _proxy_cfg.new_url(session_id="remax_cf_bypass")
                    Actor.log.info(f"[remax] Proxy yapılandırıldı: {str(remax_proxy_url)[:50]}...")
            except Exception as _pe:
                Actor.log.warning(f"[remax] Proxy yapılandırma hatası: {_pe}")

        # KV Store açılıyor ve önceki run'dan checkpoint'ler /tmp'ye yükleniyor
        try:
            kvs = await Actor.open_key_value_store(name=_KV_STORE_NAME)
            Actor.log.info(f"Named KV Store '{_KV_STORE_NAME}' açıldı.")
        except Exception as _kvs_err:
            Actor.log.warning(
                f"Named KV Store açılamadı ({_kvs_err}), default store kullanılıyor. "
                "Checkpoint'ler run'lar arası kalıcı OLMAYACAK."
            )
            kvs = await Actor.open_key_value_store()
        cp_loaded = 0
        for _cp_key in _CP_KEYS:
            _cp_data = await kvs.get_value(_cp_key)
            if isinstance(_cp_data, dict) and _cp_data:
                _cp_path = _data_path(_cp_key)
                _cp_path.write_text(
                    json.dumps(_cp_data, ensure_ascii=False, indent=2), encoding="utf-8"
                )
                cp_loaded += 1
        if cp_loaded:
            Actor.log.info(
                f"KV Store'dan {cp_loaded} checkpoint yüklendi — kaldığı yerden devam ediliyor."
            )

        # Geçersiz kaynak adlarını filtrele
        sources = [s.lower().strip() for s in sources if s.lower().strip() in _ALL_SOURCES]
        if not sources:
            Actor.log.warning("Hiçbir geçerli kaynak bulunamadı, tüm kaynaklar çalıştırılıyor.")
            sources = _ALL_SOURCES

        Actor.log.info(f"Çalıştırılacak kaynaklar: {sources}")
        Actor.log.info(
            f"maxPages={max_pages}, requestDelay={delay}s, headless={headless}, concurrency={concurrency}"
        )

        # ----------------------------------------------------------------
        # Graceful abort
        # ----------------------------------------------------------------
        _abort_event = threading.Event()

        async def _on_aborting() -> None:
            Actor.log.warning(
                "Abort sinyali alındı — checkpoint KV Store'a kaydedildikten sonra duruluyor…"
            )
            _abort_event.set()
            await _sync_to_kv(kvs)

        Actor.on(Event.ABORTING, _on_aborting)

        # Periyodik KV Store senkronizasyonu (60 saniyede bir /tmp → KV Store)
        async def _periodic_kv_sync() -> None:
            try:
                while True:
                    await asyncio.sleep(60)
                    await _sync_to_kv(kvs)
            except asyncio.CancelledError:
                pass

        _sync_task = asyncio.create_task(_periodic_kv_sync())

        # Asyncio event loop referansı — worker thread'lerinden push_data çağırabilmek için
        loop = asyncio.get_running_loop()

        async def push(listing: CanonicalListing) -> None:
            row = {
                "source":           listing.source,
                "url":              listing.url,
                "title":            listing.title,
                "listing_no":       listing.listing_no,
                "product_id":       listing.product_id,
                "category":         listing.category,
                "transaction_type": listing.transaction_type,
                "property_type":    listing.property_type,
                "price":            listing.price,
                "currency":         listing.currency,
                "location":         listing.location,
                "district":         listing.district,
                "neighborhood":     listing.neighborhood,
                "m2_net":           listing.m2_net,
                "m2_brut":          listing.m2_brut,
                "room_count":       listing.room_count,
                "floor":            listing.floor,
                "total_floors":     listing.total_floors,
                "build_year":       listing.build_year,
                "heating":          listing.heating,
                "description":      listing.description,
                "latitude":         listing.latitude,
                "longitude":        listing.longitude,
                "scraped_at_utc":   listing.scraped_at_utc,
            }
            await Actor.push_data(row)

        # Thread-safe köprü: herhangi bir worker thread'inden çağrılabilir
        def sync_on_item(listing: CanonicalListing) -> None:
            asyncio.run_coroutine_threadsafe(push(listing), loop).result()
            if _abort_event.is_set():
                raise _GracefulAbort

        # Her kaynak kendi thread'inde çalışan sarmalayıcı
        def sync_run_source(source: str) -> None:
            Actor.log.info(f"--- {source.upper()} başlıyor ---")
            try:
                if source == "cb":
                    _run_cb(max_pages, delay, concurrency, sync_on_item)
                elif source == "century21":
                    _run_century21(max_pages, delay, concurrency, sync_on_item)
                elif source == "era":
                    _run_era(max_pages, delay, concurrency, sync_on_item)
                elif source == "realtyworld":
                    _run_realtyworld(max_pages, delay, concurrency, sync_on_item)
                elif source == "remax":
                    remax_cookies = inp.get("remaxCookies", "")
                    _run_remax(max_pages, delay, headless, sync_on_item, cookies_json=remax_cookies, proxy_url=remax_proxy_url)
                elif source == "turyap":
                    _run_turyap(max_pages, delay, headless, sync_on_item)
                Actor.log.info(f"--- {source.upper()} tamamlandı ---")
            except _GracefulAbort:
                Actor.log.warning(f"Graceful abort: {source.upper()} durduruldu.")
            except Exception as exc:
                Actor.log.error(f"{source} hatası: {exc}", exc_info=True)

        # Tüm kaynaklar paralel çalışır — her biri kendi thread'inde
        # HTTP kaynaklar: farklı domain, RAM etkisi minimal
        # Selenium kaynaklar: her biri ayrı Chrome instance (~400MB) — 8GB limiti içinde sorunsuz
        http_sources    = [s for s in sources if s not in ("remax", "turyap")]
        browser_sources = [s for s in sources if s in ("remax", "turyap")]

        all_parallel = http_sources + browser_sources
        if all_parallel:
            Actor.log.info(f"Tüm kaynaklar paralel çalıştırılıyor: {all_parallel}")
            await asyncio.gather(*(asyncio.to_thread(sync_run_source, s) for s in all_parallel))

        _sync_task.cancel()
        await _sync_to_kv(kvs)
        Actor.log.info("Tüm kaynaklar tamamlandı veya durduruldu.")


if __name__ == "__main__":
    asyncio.run(main())
