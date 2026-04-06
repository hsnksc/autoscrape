# Apify Python Actor base image — Python 3.11
FROM apify/actor-python:3.11

# Google Chrome'u resmi apt deposundan kur (libgconf-2-4 Debian 12'de yok)
RUN apt-get update -qq && \
    apt-get install -y --no-install-recommends wget gnupg2 ca-certificates && \
    wget -q -O /tmp/google.asc https://dl.google.com/linux/linux_signing_key.pub && \
    gpg --dearmor -o /usr/share/keyrings/google-chrome.gpg /tmp/google.asc && \
    echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-chrome.gpg] \
http://dl.google.com/linux/chrome/deb/ stable main" \
        > /etc/apt/sources.list.d/google-chrome.list && \
    apt-get update -qq && \
    apt-get install -y --no-install-recommends google-chrome-stable xvfb unzip && \
    apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/google.asc

# Chrome ile birebir eşleşen ChromeDriver'ı build-time'da yükle
RUN CHROME_VER=$(google-chrome --version | grep -oP '\d+\.\d+\.\d+\.\d+') && \
    echo "ChromeDriver indiriliyor: ${CHROME_VER}" && \
    wget -q "https://storage.googleapis.com/chrome-for-testing-public/${CHROME_VER}/linux64/chromedriver-linux64.zip" \
         -O /tmp/chromedriver.zip && \
    unzip -j /tmp/chromedriver.zip "*/chromedriver" -d /usr/local/bin/ && \
    rm /tmp/chromedriver.zip && \
    chmod +x /usr/local/bin/chromedriver && \
    echo "ChromeDriver kuruldu: $(chromedriver --version)"

# Uygulama bağımlılıkları
# ChromeDriver: Selenium Manager (>=4.6) tarafından otomatik indirilir
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
COPY .actor/ ./.actor/

# Apify Actor giriş noktası
CMD ["python", "-m", "src.main"]
