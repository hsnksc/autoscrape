"""
scraper_base.py — Ortak scraper yardımcıları ve iş parçacıklı detay döngüsü.

ThreadedDetailLoop:
  - URL listesini batch'ler halinde işler (varsayılan: 20 URL/batch)
  - Batch'ler arasında worker_cfg.json okuyarak worker sayısını dinamik günceller
  - Thread-safe CSV append (Lock)
  - Thread-safe checkpoint (atomik yazma: tmp → rename)
  - Her worker kendi delay'ini bekler (toplam istek hızı = workers / delay req/s)
"""
from __future__ import annotations

import csv
import json
import os
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Callable, Optional

WORKER_CFG_FILENAME = "worker_cfg.json"
BATCH_SIZE = 20          # Her batch'te işlenecek URL sayısı
CHECKPOINT_EVERY = 20   # Bu kadar URL işleyince checkpoint kaydet


def read_worker_cfg(cfg_path: Path, source: str, default: int) -> int:
    """worker_cfg.json'dan kaynak için worker sayısını oku. Hata olursa default döndür."""
    try:
        if cfg_path.exists():
            data = json.loads(cfg_path.read_text(encoding="utf-8"))
            val = data.get(source, default)
            return max(1, int(val))
    except Exception:
        pass
    return default


def _atomic_write(path: Path, text: str) -> None:
    """Önce .tmp dosyasına yaz, sonra yeniden adlandır (atomik)."""
    tmp = path.with_suffix(".tmp")
    tmp.parent.mkdir(parents=True, exist_ok=True)
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


class ThreadedDetailLoop:
    """
    Paralel HTTP getirme ile URL listesini işler.

    Kullanım:
        loop = ThreadedDetailLoop(
            source="cb",
            csv_path=Path("data/cb.csv"),
            done_urls=checkpoint["done_urls"],
            cp_path=Path("data/cb.checkpoint.json"),
            cp=checkpoint,
            columns=COLUMNS,
            workers=2,
            delay=1.0,
            worker_cfg_path=Path("data/latest/worker_cfg.json"),
        )
        loop.run(remaining_urls, fetch_and_parse_fn)
    """

    def __init__(
        self,
        source: str,
        csv_path: Path,
        done_urls: list[str],
        cp_path: Path,
        cp: dict,
        columns: list[str],
        workers: int = 1,
        delay: float = 1.0,
        worker_cfg_path: Optional[Path] = None,
    ) -> None:
        self.source = source
        self.csv_path = csv_path
        self.done_urls = done_urls
        self.cp_path = cp_path
        self.cp = cp
        self.columns = columns
        self.workers = workers
        self.delay = delay
        self.worker_cfg_path = worker_cfg_path

        self._csv_lock = threading.Lock()
        self._cp_lock = threading.Lock()
        self._processed = 0  # başarıyla işlenen sayısı

    # ------------------------------------------------------------------
    # İç yardımcılar
    # ------------------------------------------------------------------

    def _check_cfg_update(self) -> None:
        """worker_cfg.json'dan güncel worker sayısını oku ve güncelle."""
        if not self.worker_cfg_path:
            return
        new_w = read_worker_cfg(self.worker_cfg_path, self.source, self.workers)
        if new_w != self.workers:
            print(f"  [{self.source.upper()}] Worker güncellendi: {self.workers} → {new_w}")
            self.workers = new_w

    def _save_checkpoint(self) -> None:
        """Checkpoint'i atomik olarak kaydet (cp_lock içinde çağrılmalı)."""
        try:
            _atomic_write(
                self.cp_path,
                json.dumps(self.cp, ensure_ascii=False, indent=2),
            )
        except Exception as e:
            print(f"  [{self.source.upper()}] Checkpoint hatası: {e}")

    def _process_one(
        self,
        url: str,
        global_idx: int,
        total: int,
        fetch_and_parse: Callable[[str], Optional[dict]],
    ) -> bool:
        """Tek URL'yi işle; thread güvenlidir."""
        time.sleep(self.delay + random.uniform(0, 0.35))
        try:
            row = fetch_and_parse(url)
        except Exception as exc:
            print(f"  [{self.source.upper()}] [{global_idx}/{total}] HATA {url}: {exc}")
            return False

        if not row:
            print(f"  [{self.source.upper()}] [{global_idx}/{total}] BOŞDÖNDÜ {url}")
            return False

        # Thread-safe CSV append
        with self._csv_lock:
            self.csv_path.parent.mkdir(parents=True, exist_ok=True)
            file_exists = self.csv_path.exists()
            with self.csv_path.open("a", newline="", encoding="utf-8-sig", errors="ignore") as fh:
                writer = csv.DictWriter(fh, fieldnames=self.columns, extrasaction="ignore")
                if not file_exists:
                    writer.writeheader()
                writer.writerow(row)

        # Thread-safe checkpoint update
        with self._cp_lock:
            self.done_urls.append(url)
            self.cp["done_urls"] = list(self.done_urls)
            self._processed += 1
            if self._processed % CHECKPOINT_EVERY == 0:
                self._save_checkpoint()

        print(f"  [{self.source.upper()}] [{global_idx}/{total}] OK {url}")
        return True

    # ------------------------------------------------------------------
    # Ana döngü
    # ------------------------------------------------------------------

    def run(
        self,
        remaining: list[str],
        fetch_and_parse: Callable[[str], Optional[dict]],
    ) -> None:
        """
        Tüm URL listesini BATCH_SIZE'lık gruplar halinde thread'lerle işle.

        Her batch başında worker_cfg.json okunur; worker sayısı değişmişse
        bir sonraki batch yeni worker sayısıyla çalışır.
        """
        done_set = set(self.done_urls)
        todo = [u for u in remaining if u not in done_set]
        total = len(todo)

        if total == 0:
            print(f"  [{self.source.upper()}] İşlenecek URL kalmadı.")
            return

        print(f"  [{self.source.upper()}] {total} URL işlenecek | worker={self.workers} | delay={self.delay}s")

        offset = 0
        global_start = len(done_set)  # done_urls'deki mevcut offset

        while offset < total:
            # --- Batch başı: worker config güncelle ---
            self._check_cfg_update()

            batch = todo[offset: offset + BATCH_SIZE]
            batch_workers = min(self.workers, len(batch))

            with ThreadPoolExecutor(max_workers=batch_workers) as exe:
                futs = {
                    exe.submit(
                        self._process_one,
                        url,
                        global_start + offset + i + 1,
                        global_start + total,
                        fetch_and_parse,
                    ): url
                    for i, url in enumerate(batch)
                }
                for _ in as_completed(futs):
                    pass  # sonuçlar _process_one içinde loglanıyor

            offset += len(batch)

        # Son checkpoint
        with self._cp_lock:
            self._save_checkpoint()

        print(
            f"  [{self.source.upper()}] Detay döngüsü tamamlandı. "
            f"{self._processed} yeni kayıt işlendi."
        )
