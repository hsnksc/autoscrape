# Unified Turkish Real Estate Scraper — self-contained
# Scraper kaynaklari unified-scraper/scrapers/ icinde bulunur.

FROM apify/actor-python-selenium:3.12

WORKDIR /usr/src/app

# Bagimlilik yukle
COPY requirements.txt /tmp/req_unified.txt
RUN pip install --no-cache-dir -r /tmp/req_unified.txt

# Camoufox patched Firefox binary indir
RUN python -m camoufox fetch

# Scraper kaynak kodlarini kopyala
COPY scrapers/emlakjet/   /usr/src/app/scrapers/emlakjet/
COPY scrapers/hepsiemlak/ /usr/src/app/scrapers/hepsiemlak/
COPY scrapers/sahibinden/ /usr/src/app/scrapers/sahibinden/

# Unified actor kaynak kodunu kopyala
COPY src/ ./src/

CMD ["python3", "src/main.py"]
