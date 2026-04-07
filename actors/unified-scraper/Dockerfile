# Unified Turkish Real Estate Scraper — requests tabanlı, Selenium yok

FROM python:3.12-slim

WORKDIR /usr/src/app

# Bagimlilik yukle
COPY requirements.txt /tmp/req_unified.txt
RUN pip install --no-cache-dir -r /tmp/req_unified.txt

# Unified actor kaynak kodunu kopyala
COPY src/ ./src/

CMD ["python3", "src/main.py"]
