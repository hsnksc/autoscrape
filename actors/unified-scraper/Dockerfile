# Unified Turkish Real Estate Scraper — Playwright + requests tabanlı

FROM apify/actor-python-playwright:3.12

WORKDIR /usr/src/app

# Bagimlilik yukle (playwright SDK zaten base image'de mevcut)
COPY requirements.txt /tmp/req_unified.txt
RUN pip install --no-cache-dir -r /tmp/req_unified.txt

# Unified actor kaynak kodunu kopyala
COPY src/ ./src/

CMD ["python3", "src/main.py"]
