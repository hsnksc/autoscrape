"""
playwright_fetch.py — Cloudflare korumalı siteler için browser tabanlı HTML fetch.

hepsiemlak.com ve sahibinden.com içindir.
Playwright + playwright-stealth ile Cloudflare JS challenge bypass.
"""
from __future__ import annotations

import asyncio
from urllib.parse import urlparse
from typing import Optional


def _parse_proxy(proxy_url: str) -> dict:
    """
    Apify proxy URL'sini Playwright proxy config dict'ine dönüştür.
    Apify formatı: http://user:password@proxy.apify.com:8000
    Playwright bekler: {"server": "http://host:port", "username": "...", "password": "..."}
    """
    try:
        parsed = urlparse(proxy_url)
        server = f"{parsed.scheme}://{parsed.hostname}:{parsed.port}"
        config: dict = {"server": server}
        if parsed.username:
            config["username"] = parsed.username
        if parsed.password:
            config["password"] = parsed.password
        return config
    except Exception:
        return {"server": proxy_url}


async def _launch_browser(proxy_url: Optional[str] = None):
    """Browser + context + stealth döndür."""
    from playwright.async_api import async_playwright
    p_ctx = async_playwright()
    p = await p_ctx.__aenter__()

    launch_kwargs: dict = {
        "headless": True,
        "args": [
            "--no-sandbox", "--disable-setuid-sandbox",
            "--disable-dev-shm-usage", "--disable-gpu",
            "--disable-blink-features=AutomationControlled",
        ],
    }
    if proxy_url:
        launch_kwargs["proxy"] = _parse_proxy(proxy_url)

    browser = await p.chromium.launch(**launch_kwargs)
    context = await browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        extra_http_headers={
            "Accept-Language": "tr-TR,tr;q=0.9,en;q=0.8",
            "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
            "sec-ch-ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
        },
        locale="tr-TR",
        viewport={"width": 1280, "height": 800},
        timezone_id="Europe/Istanbul",
    )
    page = await context.new_page()

    try:
        from playwright_stealth import stealth_async
        await stealth_async(page)
    except ImportError:
        await page.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )

    return p_ctx, p, browser, page


async def fetch_with_playwright(
    url: str,
    proxy_url: Optional[str] = None,
    timeout_ms: int = 40_000,
) -> str:
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        print("[PLAYWRIGHT] playwright kurulu degil")
        return ""

    try:
        p_ctx, p, browser, page = await _launch_browser(proxy_url)
        try:
            response = await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            if response:
                print(f"[PLAYWRIGHT] Status {response.status}: {url[:60]}")
                if response.status in (403, 429, 503):
                    return ""
        except Exception as exc:
            print(f"[PLAYWRIGHT] goto hatasi ({url[:60]}): {exc}")

        try:
            await page.wait_for_load_state("networkidle", timeout=15_000)
        except Exception:
            pass
        await page.wait_for_timeout(2_000)

        html = await page.content()
        await browser.close()
        await p_ctx.__aexit__(None, None, None)

        if len(html) < 1000:
            print(f"[PLAYWRIGHT] Icerik cok kisa ({len(html)} byte): {url[:60]}")
            return ""
        return html

    except Exception as exc:
        print(f"[PLAYWRIGHT] Hata ({url[:60]}): {exc}")
        return ""


async def fetch_links_from_page(
    url: str,
    link_selector: str = "a[href]",
    proxy_url: Optional[str] = None,
    timeout_ms: int = 45_000,
) -> list[str]:
    """
    Playwright ile sayfadaki linkleri JavaScript evaluate ile top.
    SPA sayfalar (Nuxt/React) icin HTML parse yerine DOM kullan.
    """
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        return []

    try:
        p_ctx, p, browser, page = await _launch_browser(proxy_url)
        try:
            response = await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            if response:
                print(f"[PLAYWRIGHT-LINKS] Status {response.status}: {url[:60]}")
                if response.status in (403, 429, 503):
                    return []
        except Exception as exc:
            print(f"[PLAYWRIGHT-LINKS] goto hatasi ({url[:60]}): {exc}")
            return []

        # SPA renderinin bitmesini bekle
        try:
            await page.wait_for_load_state("networkidle", timeout=20_000)
        except Exception:
            pass
        await page.wait_for_timeout(3_000)

        # JavaScript ile DOM'dan linkleri al
        hrefs: list = await page.evaluate(
            """(selector) => {
                const links = document.querySelectorAll(selector);
                return Array.from(links).map(a => a.href || a.getAttribute('href')).filter(Boolean);
            }""",
            link_selector,
        )

        await browser.close()
        await p_ctx.__aexit__(None, None, None)
        return hrefs

    except Exception as exc:
        print(f"[PLAYWRIGHT-LINKS] Hata ({url[:60]}): {exc}")
        return []


def fetch_sync(url: str, proxy_url: Optional[str] = None, timeout_ms: int = 40_000) -> str:
    """Senkron wrapper."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(fetch_with_playwright(url, proxy_url, timeout_ms))
    finally:
        loop.close()


def fetch_links_sync(url: str, link_selector: str = "a[href]", proxy_url: Optional[str] = None, timeout_ms: int = 45_000) -> list[str]:
    """fetch_links_from_page senkron wrapper."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(fetch_links_from_page(url, link_selector, proxy_url, timeout_ms))
    finally:
        loop.close()
