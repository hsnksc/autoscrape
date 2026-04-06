# Hepsiemlak scraper için Apify Docker image
FROM apify/actor-python-selenium:3.12

# Çalışma dizini
WORKDIR /usr/src/app

# Bağımlılıkları yükle
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Kaynak dosyaları kopyala
COPY src/ ./src/

# Actor'ı çalıştır
CMD ["python3", "src/main.py"]
