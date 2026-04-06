# Sahibinden scraper için Apify Docker image
# Camoufox (Firefox tabanli) ile CF bypass
FROM apify/actor-python-playwright:3.12

# Çalışma dizini
WORKDIR /usr/src/app

# Bağımlılıkları yükle
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Camoufox patched Firefox binary indir
RUN python -m camoufox fetch

# Kaynak dosyaları kopyala
COPY src/ ./src/

# Actor'ı çalıştır
CMD ["python3", "src/main.py"]
