"""
Hepsiemlak Apify Actor - Ana giriş noktası.

Apify input parametrelerini okur, scraper'ı çalıştırır ve
sonuçları Apify Dataset'e aktarır.

Input parametreleri (.actor/INPUT_SCHEMA.json ile tanımlanmıştır):
  categories    - Scrape edilecek kategoriler (varsayılan: ["satilik","kiralik"])
  mode          - "list_only" veya "full" (varsayılan: "list_only")
  maxPages      - Kategori başına max sayfa, 0=sınırsız (varsayılan: 0)
  pageWorkers   - Sayfa worker sayısı (varsayılan: 2)
  detailWorkers - Detay worker sayısı, sadece full modda (varsayılan: 2)
  delay         - İstekler arası bekleme saniyesi (varsayılan: 1.5)
"""
from __future__ import annotations

import asyncio
import csv
import sys
from pathlib import Path

# Scraper modülünün bulunduğu dizini Python path'ine ekle
sys.path.insert(0, str(Path(__file__).parent))

from apify import Actor  # noqa: E402
import hepsiemlak_scraper as scraper  # noqa: E402

OUTPUT_CSV = Path("/tmp/hepsiemlak_output.csv")
CHECKPOINT_PATH = Path("/tmp/hepsiemlak_output.checkpoint.json")


async def main() -> None:
    async with Actor:
        actor_input = await Actor.get_input() or {}

        categories    = actor_input.get("categories") or ["satilik", "kiralik"]
        mode          = actor_input.get("mode", "list_only")
        max_pages     = int(actor_input.get("maxPages", 0))
        page_workers  = int(actor_input.get("pageWorkers", 4))
        detail_workers = int(actor_input.get("detailWorkers", 2))
        delay         = float(actor_input.get("delay", 1.5))

        Actor.log.info(
            f"Başlıyor | kategoriler={categories} | mod={mode} "
            f"| max_sayfa={max_pages} | sayfa_worker={page_workers}"
        )

        # Proxy ortam degiskenleri – cakisma olup olmadigini gormek icin
        import os as _os
        for _ev in ("http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY", "APIFY_PROXY_PASSWORD"):
            _val = _os.environ.get(_ev)
            if _val:
                # Parolalari maskele, sadece varligi/uzunlugu goster
                Actor.log.info(f"ENV {_ev} mevcut (len={len(_val)})")

        # Proxy yapılandırması
        proxy_cfg_input = actor_input.get("proxyConfiguration") or {}
        if proxy_cfg_input:
            try:
                proxy_cfg = await Actor.create_proxy_configuration(
                    actor_proxy_input=proxy_cfg_input
                )
                scraper.PROXY_URL = await proxy_cfg.new_url()
                # Güvenlik: şifreyi loglamadan yapıyı göster
                from urllib.parse import urlparse as _up
                _p = _up(scraper.PROXY_URL)
                Actor.log.info(
                    f"Apify proxy aktif: {_p.scheme}://{_p.hostname}:{_p.port} "
                    f"(username_len={len(_p.username or '')}, has_password={bool(_p.password)})"
                )
            except Exception as exc:
                Actor.log.warning(f"Proxy ayarlanamadı, proxysiz devam: {exc}")
                scraper.PROXY_URL = None
        else:
            scraper.PROXY_URL = None

        # Incremental push: actor durdurulsa bile o ana kadar yazilan veriler Dataset'e gider
        _pushed_rows: list[int] = [0]

        async def _incremental_push() -> None:
            """Her 60 saniyede CSV'deki yeni satirlari Dataset'e push eder."""
            while True:
                await asyncio.sleep(60)
                if not OUTPUT_CSV.exists():
                    continue
                try:
                    with OUTPUT_CSV.open("r", encoding="utf-8-sig") as fh:
                        rows = list(csv.DictReader(fh))
                    new_rows = rows[_pushed_rows[0]:]
                    if new_rows:
                        await Actor.push_data(new_rows)
                        _pushed_rows[0] += len(new_rows)
                        Actor.log.info(
                            f"Ara kayit: {len(new_rows)} satir gonderildi "
                            f"(toplam: {_pushed_rows[0]})"
                        )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    Actor.log.warning(f"Ara kayit hatasi: {exc}")

        push_task = asyncio.create_task(_incremental_push())

        try:
            # Scraper senkron/blocking → ayrı thread'de çalıştır
            if mode == "list_only":
                await asyncio.to_thread(
                    scraper.scrape_list_only,
                    categories=categories,
                    csv_path=OUTPUT_CSV,
                    max_pages=max_pages,
                    page_workers=page_workers,
                    delay=delay,
                    headless=False,
                    no_images=True,
                    page_starts=[],
                    page_ranges=[],
                )
            else:
                await asyncio.to_thread(
                    scraper.scrape_all,
                    categories=categories,
                    csv_path=OUTPUT_CSV,
                    max_pages=max_pages,
                    page_workers=page_workers,
                    detail_workers=detail_workers,
                    delay=delay,
                    headless=False,
                    no_images=True,
                    page_starts=[],
                    worker_cfg_path=None,
                )
        finally:
            # Arka plan push gorevini durdur
            push_task.cancel()
            try:
                await push_task
            except asyncio.CancelledError:
                pass

            # Son push – henuz gonderilmemis satirlari gonder
            if OUTPUT_CSV.exists():
                try:
                    with OUTPUT_CSV.open("r", encoding="utf-8-sig") as fh:
                        rows = list(csv.DictReader(fh))
                    new_rows = rows[_pushed_rows[0]:]
                    if new_rows:
                        await Actor.push_data(new_rows)
                        Actor.log.info(
                            f"Son kayit: {len(new_rows)} satir gonderildi "
                            f"(genel toplam: {_pushed_rows[0] + len(new_rows)})"
                        )
                    else:
                        Actor.log.info(f"Tum kayitlar zaten gonderildi (toplam: {_pushed_rows[0]})")
                except Exception as exc:
                    Actor.log.warning(f"Son kayit hatasi: {exc}")
            else:
                Actor.log.warning("Cikti CSV olusturulamadi – kayit bulunamadi.")

            # Debug HTML varsa dataset'e push et
            debug_html_path = Path("/tmp/hepsiemlak_debug_page.html")
            if debug_html_path.exists():
                try:
                    debug_html = debug_html_path.read_text(encoding="utf-8", errors="replace")
                    Actor.log.warning(
                        f"DEBUG: Kaydedilen sayfa HTML {len(debug_html)} karakter"
                    )
                    await Actor.push_data([{
                        "_debug": True,
                        "url": "hepsiemlak_debug",
                        "html_preview": debug_html[:3000],
                        "html_length": len(debug_html),
                    }])
                except Exception as exc:
                    Actor.log.warning(f"Debug HTML okunamadi: {exc}")


if __name__ == "__main__":
    asyncio.run(main())
