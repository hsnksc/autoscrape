"""
Sahibinden Apify Actor - Ana giriş noktası.

Apify input parametrelerini okur, scraper'ı çalıştırır ve
sonuçları Apify Dataset'e aktarır.

UYARI: Sahibinden.com Cloudflare koruması kullanmaktadır.
Headless=True ile erişim kısıtlanabilir. Apify'ın residential proxy'leri
ile birlikte kullanılması başarı oranını artırır.

Input parametreleri (.actor/INPUT_SCHEMA.json ile tanımlanmıştır):
  categories  - Scrape edilecek kategoriler (varsayılan: ["satilik","kiralik"])
  pageWorkers - Worker sayısı, CF nedeniyle 1 önerilir (varsayılan: 1)
  delay       - İstekler arası bekleme, en az 2.0 önerilir (varsayılan: 3.0)
  pageRanges  - Sayfa aralığı, ör: "1-50" veya "1-25,26-50" (varsayılan: "")
"""
from __future__ import annotations

import asyncio
import csv
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from apify import Actor  # noqa: E402
import sahibinden_scraper as scraper  # noqa: E402

OUTPUT_CSV = Path("/tmp/sahibinden_output.csv")


async def main() -> None:
    async with Actor:
        actor_input = await Actor.get_input() or {}

        categories      = actor_input.get("categories") or ["satilik", "kiralik"]
        page_workers    = int(actor_input.get("pageWorkers", 1))
        delay           = float(actor_input.get("delay", 3.0))
        page_ranges_str = str(actor_input.get("pageRanges", "") or "")
        scrape_details  = bool(actor_input.get("scrapeDetails", True))

        page_ranges = scraper.parse_page_ranges(page_ranges_str) if page_ranges_str.strip() else []

        Actor.log.info(
            f"Başlıyor | kategoriler={categories} "
            f"| sayfa_worker={page_workers} | gecikme={delay:.1f}s"
            f"| detay_cek={scrape_details}"
        )
        Actor.log.info("Camoufox (Firefox) ile CF bypass aktif.")

        # Session cookies
        raw_cookies = actor_input.get("sessionCookies") or ""
        if isinstance(raw_cookies, str) and raw_cookies.strip():
            try:
                cookies_list = json.loads(raw_cookies)
                if isinstance(cookies_list, list):
                    scraper.SESSION_COOKIES = cookies_list
                    Actor.log.info(f"Session cookies yuklendi: {len(cookies_list)} cerez")
                else:
                    Actor.log.warning("sessionCookies JSON bir liste degil, atlanacak.")
            except json.JSONDecodeError as exc:
                Actor.log.warning(f"sessionCookies JSON parse hatasi: {exc}")
        elif isinstance(raw_cookies, list):
            scraper.SESSION_COOKIES = raw_cookies
            Actor.log.info(f"Session cookies yuklendi: {len(raw_cookies)} cerez")
        else:
            Actor.log.warning(
                "sessionCookies bos! Sahibinden CF/login engelini asmak icin "
                "EditThisCookie ile export edilmis cerezler gereklidir."
            )

        # Proxy yapılandırması
        proxy_cfg_input = actor_input.get("proxyConfiguration") or {}
        if proxy_cfg_input:
            try:
                proxy_cfg = await Actor.create_proxy_configuration(
                    actor_proxy_input=proxy_cfg_input,
                    country_code="TR",
                )
                # Her session icin farkli TR IP → 20 URL onceden uret
                proxy_urls = []
                for _ in range(20):
                    proxy_urls.append(await proxy_cfg.new_url())
                scraper.PROXY_URLS = proxy_urls
                from urllib.parse import urlparse as _up
                _p = _up(proxy_urls[0])
                Actor.log.info(
                    f"Apify proxy aktif (TR, {len(proxy_urls)} URL): "
                    f"{_p.scheme}://{_p.hostname}:{_p.port}"
                )
            except Exception as exc:
                Actor.log.warning(f"Proxy ayarlanamadı, proxysiz devam: {exc}")
                scraper.PROXY_URLS = []
        else:
            scraper.PROXY_URLS = []

        try:
            scrape_fn = scraper.scrape_with_details if scrape_details else scraper.scrape_list_only
            await asyncio.to_thread(
                scrape_fn,
                categories=categories,
                csv_path=OUTPUT_CSV,
                page_workers=page_workers,
                delay=delay,
                headless=False,
                page_ranges=page_ranges,
            )
        finally:
            scraper.shutdown_browser()

        if OUTPUT_CSV.exists():
            with OUTPUT_CSV.open("r", encoding="utf-8-sig") as fh:
                rows = list(csv.DictReader(fh))
            Actor.log.info(f"{len(rows)} kayıt Dataset'e aktarılıyor...")
            await Actor.push_data(rows)
        else:
            Actor.log.warning(
                "Çıktı CSV oluşturulamadı – Cloudflare engeli veya boş kategori olabilir."
            )




if __name__ == "__main__":
    asyncio.run(main())
