"""
Ortak HTTP yardımcıları - tüm scraper'lar bu modülü kullanır.
urllib.request tabanlı (dış bağımlılık yok).
"""
from __future__ import annotations

import random
import re
import time
from html import unescape
from typing import Optional
from urllib.request import Request, urlopen
from urllib.parse import urljoin

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def fetch_html(url: str, retries: int = 3, delay: float = 2.0) -> str:
    """URL'yi indir, başarısız olursa `retries` kez tekrar dene."""
    for attempt in range(retries):
        try:
            req = Request(url, headers={"User-Agent": USER_AGENT})
            with urlopen(req, timeout=30) as resp:
                return resp.read().decode("utf-8", errors="ignore")
        except Exception as exc:
            if attempt < retries - 1:
                time.sleep(delay * (attempt + 1))
            else:
                raise RuntimeError(f"HTML alınamadı: {url} -> {exc}") from exc
    return ""


def clean_text(html: str) -> str:
    """HTML tag'lerini kaldır, entity'leri çöz."""
    return unescape(re.sub(r"<[^>]+>", " ", html)).replace("\xa0", " ").strip()
