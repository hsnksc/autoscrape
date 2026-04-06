# Unified Turkish Real Estate Scraper
# Build context: actors/ (dockerContextDir: ".." ayarıyla bir üst dizin)
#
# Hem Selenium Chrome (emlakjet, hepsiemlak) hem Camoufox Firefox (sahibinden)
# desteklendiği için apify/actor-python-selenium base image kullanılır;
# üzerine camoufox ve diğer bağımlılıklar eklenir.

FROM apify/actor-python-selenium:3.12

WORKDIR /usr/src/app

# ── Bağımlılık dosyalarını topla ve birleştir ───────────────────────────────
COPY emlakjet-scraper/requirements.txt   /tmp/req_emlakjet.txt
COPY hepsiemlak-scraper/requirements.txt /tmp/req_hepsiemlak.txt
COPY sahibinden-scraper/requirements.txt /tmp/req_sahibinden.txt
COPY unified-scraper/requirements.txt    /tmp/req_unified.txt

# Tekrar edenleri filtrele, tek pip çağrısında yükle
RUN sort -u \
        /tmp/req_emlakjet.txt \
        /tmp/req_hepsiemlak.txt \
        /tmp/req_sahibinden.txt \
        /tmp/req_unified.txt \
    > /tmp/req_all.txt \
 && pip install --no-cache-dir -r /tmp/req_all.txt

# ── Camoufox patched Firefox binary indir ──────────────────────────────────
RUN python -m camoufox fetch

# ── Scraper kaynak kodlarını kopyala ───────────────────────────────────────
COPY emlakjet-scraper/src/   /usr/src/app/scrapers/emlakjet/
COPY hepsiemlak-scraper/src/ /usr/src/app/scrapers/hepsiemlak/
COPY sahibinden-scraper/src/ /usr/src/app/scrapers/sahibinden/

# ── Unified actor kaynak kodunu kopyala ────────────────────────────────────
COPY unified-scraper/src/ ./src/

CMD ["python3", "src/main.py"]
