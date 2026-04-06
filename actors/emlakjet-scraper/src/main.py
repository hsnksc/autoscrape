"""
Emlakjet Apify Actor - Ana giriş noktası.

Apify input parametrelerini okur, scraper'ı çalıştırır ve
sonuçları Apify Dataset'e aktarır. Checkpoint KV Store'a kaydedilir;
kesintide yeni run aynı kategorileri kaldığı yerden devam ettirir.

Input parametreleri (.actor/INPUT_SCHEMA.json ile tanımlanmıştır):
  categories      - Scrape edilecek kategoriler (varsayılan: tümü)
  maxPages        - Kategori başına max sayfa, 0=sınırsız (varsayılan: 0)
  workers         - Worker sayısı (varsayılan: 8)
  delay           - İstekler arası bekleme saniyesi (varsayılan: 0.8)
  settle          - Sayfa yükleme sonrası bekleme (varsayılan: 1.5)
  clearCheckpoint - Önceki checkpoint'i sil ve baştan başla (varsayılan: false)
  maxConcurrentChrome - Aynı anda açık Chrome sayısı üst sınırı (varsayılan: 50)
  useProxy        - Apify Residential proxy kullan (varsayılan: false)
"""
from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from apify import Actor  # noqa: E402
import emlakjet_scraper as scraper  # noqa: E402

OUTPUT_DIR = Path("/tmp/emlakjet_output")
CP_STORE_NAME = "emlakjet-checkpoints"  # Kalıcı named KV store


async def main() -> None:
    async with Actor:
        actor_input = await Actor.get_input() or {}

        categories           = actor_input.get("categories") or list(scraper.CATEGORIES.keys())
        max_pages            = int(actor_input.get("maxPages", 0))
        workers              = int(actor_input.get("workers", 8))
        delay                = float(actor_input.get("delay", 0.8))
        settle               = float(actor_input.get("settle", 1.5))
        max_concurrent_chrome = int(actor_input.get("maxConcurrentChrome", 30))
        use_proxy            = bool(actor_input.get("useProxy", False))
        clear_cp             = bool(actor_input.get("clearCheckpoint", False))
        scrape_details       = bool(actor_input.get("scrapeDetails", False))

        # scrapeDetails=true ise her worker liste + detay sayfalarını ziyaret eder.
        # CPU sınırı: Apify 32GB ~ 8 vCPU. Tüm worker'ların aynı anda detay JS çalıştırması
        # renderer timeout'a yol açar. Güvenli üst sınır = min(workers × kategoriler, 20).
        if scrape_details:
            # Detay sayfaları JS-heavy; CPU doygunluğunu önlemek için Chrome sayısını sınırla.
            # Apify 32GB ~8 vCPU → 10 eş zamanlı Chrome güvenli üst sınır.
            safe_limit = min(workers * len(categories), 10)
            if max_concurrent_chrome != safe_limit:
                Actor.log.info(
                    f"scrapeDetails=true → chrome limiti {max_concurrent_chrome} → {safe_limit}"
                    f" (workers={workers} × kategoriler={len(categories)}, maks 10)"
                )
            max_concurrent_chrome = safe_limit

        total_chrome = len(categories) * workers
        Actor.log.info(
            f"Başlıyor | kategoriler={categories} "
            f"| max_sayfa={max_pages} | worker/kategori={workers} "
            f"| toplam_max_chrome={total_chrome} | chrome_limit={max_concurrent_chrome} "
            f"| proxy={use_proxy} | detay={scrape_details}"
        )

        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

        # ── Chrome semafor: aynı anda açık Chrome sayısını sınırla ───────────
        scraper.init_chrome_semaphore(max_concurrent_chrome)

        # ── Kalıcı KV store: checkpoint yükle / kaydet ──────────────────────
        kv = await Actor.open_key_value_store(name=CP_STORE_NAME)

        if clear_cp:
            Actor.log.info("clearCheckpoint=true → tüm checkpoint'ler siliniyor")
            for cat in scraper.CATEGORIES:
                await kv.set_value(f"cp_{cat}", None)

        # Her kategorinin checkpoint'ini /tmp'ye restore et
        for cat in categories:
            saved = await kv.get_value(f"cp_{cat}")
            if saved:
                cp_path = OUTPUT_DIR / f"emlakjet_{cat}.checkpoint.json"
                cp_path.write_text(
                    json.dumps(saved, ensure_ascii=False, indent=2), encoding="utf-8"
                )
                done_count = len(
                    (saved.get("list_done_pages") or {}).get(cat, [])
                )
                Actor.log.info(
                    f"{cat}: checkpoint yüklendi ({done_count} sayfa tamamlanmış)"
                )

        # ── Proxy yapılandırması ────────────────────────────────────────────
        proxy_cfg = None
        if use_proxy:
            try:
                proxy_cfg = await Actor.create_proxy_configuration(
                    groups=["RESIDENTIAL"],
                    country_code="TR",
                )
                Actor.log.info("Residential proxy (TR) aktif")
            except Exception as exc:
                Actor.log.warning(f"Proxy oluşturulamadı: {exc} – proxysiz devam ediliyor")

        # ── Thread-safe callback'ler ─────────────────────────────────────────
        loop = asyncio.get_running_loop()

        def push_data_sync(rows: list[dict]) -> None:
            """Scraper thread'inden çağrılır; satırları Dataset'e anlık gönderir."""
            if not rows:
                return
            try:
                future = asyncio.run_coroutine_threadsafe(
                    Actor.push_data(rows), loop
                )
                future.result(timeout=60)
            except Exception as exc:
                print(f"[EMLAKJET] Dataset push hatası: {exc}")

        def make_cp_save(cat: str):
            def _save(cp_data: dict) -> None:
                asyncio.run_coroutine_threadsafe(kv.set_value(f"cp_{cat}", cp_data), loop)
            return _save

        def make_proxy_getter(cfg):
            """Her çağrıda yeni bir proxy URL üretir (thread-safe)."""
            def _get() -> str:
                try:
                    fut = asyncio.run_coroutine_threadsafe(cfg.new_url(), loop)
                    return fut.result(timeout=15)
                except Exception as _pex:
                    print(f"[EMLAKJET] Proxy URL hatası: {_pex}")
                    return ""
            return _get

        proxy_getter = make_proxy_getter(proxy_cfg) if proxy_cfg else None

        # ── Her kategoriyi paralel başlat ────────────────────────────────────
        async def _scrape_cat(cat: str) -> None:
            base_url = scraper.CATEGORIES.get(cat)
            if not base_url:
                Actor.log.warning(f"Bilinmeyen kategori: {cat!r}, atlanıyor")
                return
            cp_save = make_cp_save(cat)
            try:
                await asyncio.to_thread(
                    scraper.scrape_category,
                    category_name=cat,
                    base_url=base_url,
                    workers=workers,
                    max_pages=max_pages,
                    headless=True,
                    no_images=True,
                    delay=delay,
                    output_dir=OUTPUT_DIR,
                    settle_secs=settle,
                    push_callback=push_data_sync,
                    cp_callback=cp_save,
                    proxy_getter=proxy_getter,
                    scrape_details=scrape_details,
                )
            except Exception as exc:
                # Hata durumunda checkpoint'i SILME — sonraki run kaldığı yerden devam eder
                Actor.log.error(f"{cat}: HATA – {exc}")
                return
            # Yalnızca başarılı tamamlanmada checkpoint'i temizle
            await kv.set_value(f"cp_{cat}", None)
            Actor.log.info(f"{cat}: tamamlandı, checkpoint temizlendi")

        await asyncio.gather(*[_scrape_cat(cat) for cat in categories])

        Actor.log.info("Tüm kategoriler tamamlandı. Veriler Dataset'e aktarıldı.")


if __name__ == "__main__":
    asyncio.run(main())

