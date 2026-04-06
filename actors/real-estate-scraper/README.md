# Turkish Real Estate Portal Scraper

Türkiye'nin 6 büyük emlak portalından konut ve ticari ilan verilerini toplayan **Apify Actor**.

| Portal | Yöntem | Kategoriler |
|---|---|---|
| [CB.com.tr](https://www.cb.com.tr) | HTTP/urllib | Konut Satılık, Kiralık, Devren |
| [Century21.com.tr](https://www.century21.com.tr) | HTTP/urllib | Konut Satılık, Kiralık |
| [ERA.com.tr](https://www.era.com.tr) | HTTP/urllib | Konut Satılık, Kiralık |
| [RealtyWorld.com.tr](https://www.realtyworld.com.tr) | HTTP/urllib | Tüm kategoriler |
| [Remax.com.tr](https://www.remax.com.tr) | Selenium | Konut/Ticari Satılık, Kiralık, Devren |
| [Turyap.com.tr](https://www.turyap.com.tr) | Selenium | Tüm kategoriler |

---

## Özellikler

- Her portaldan **CanonicalListing** formatında normalize veri
- JSON checkpoint sistemi → çökme/durdurma sonrası **kaldığı yerden devam**
- Remax için SQLite tabanlı 2-aşamalı scraping (URL toplama → detay çekme)
- Apify Dataset'e otomatik push
- Tüm kaynaklar bağımsız çalışır, istediğiniz kombinasyonu seçebilirsiniz

---

## Input Parametreleri

| Alan | Tip | Varsayılan | Açıklama |
|---|---|---|---|
| `sources` | `string[]` | tüm kaynaklar | Çalıştırılacak portal listesi |
| `maxPages` | `integer` | `0` (sınırsız) | Kategori sayfası limiti |
| `requestDelay` | `number` | `1.5` | İstekler arası bekleme (saniye) |
| `headless` | `boolean` | `true` | Selenium headless modu |

---

## Output Şeması

Her ilan aşağıdaki alanlarla Apify Dataset'e yazılır:

```json
{
  "source": "remax",
  "url": "https://www.remax.com.tr/ilan/...",
  "title": "3+1 Satılık Daire",
  "listing_no": "12345",
  "product_id": "12345",
  "category": "konut_satilik",
  "transaction_type": "satilik",
  "property_type": "Daire",
  "price": "2500000",
  "currency": "TL",
  "location": "İstanbul",
  "district": "Kadıköy",
  "neighborhood": "Moda",
  "m2_net": "120",
  "m2_brut": "140",
  "room_count": "3+1",
  "floor": "4",
  "total_floors": "7",
  "build_year": "2015",
  "heating": "Kombi",
  "description": "...",
  "latitude": 40.9854,
  "longitude": 29.0345,
  "scraped_at_utc": "2024-01-15T10:30:00+00:00"
}
```

---

## Yerel Geliştirme

### Gereksinimler

- Python 3.11+
- Google Chrome + ChromeDriver (PATH'te)

### Kurulum

```bash
git clone https://github.com/YOUR_USERNAME/turkish-real-estate-scraper.git
cd turkish-real-estate-scraper
pip install -r requirements.txt
```

### Çalıştırma (Apify CLI ile)

```bash
pip install apify-cli
apify run
```

### Çalıştırma (doğrudan Python)

```bash
python -m src.main
```

---

## Apify'e Yükleme

### Yöntem 1: GitHub üzerinden (Önerilen)

1. Bu repoyu GitHub'a push edin
2. [Apify Console](https://console.apify.com) → **Actors** → **Create new Actor**
3. **Link Git repository** seçin
4. GitHub repo URL'nizi girin
5. **Build** butonuna basın

### Yöntem 2: Apify CLI

```bash
apify login
apify push
```

---

## Proje Yapısı

```
.
├── .actor/
│   ├── actor.json          # Actor metadata
│   └── input_schema.json   # Input parametreleri
├── src/
│   ├── __init__.py
│   ├── main.py             # Apify Actor giriş noktası
│   ├── models.py           # CanonicalListing dataclass
│   ├── http_utils.py       # HTTP yardımcıları
│   ├── checkpoint.py       # JSON checkpoint sistemi
│   ├── cb_scraper.py
│   ├── century21_scraper.py
│   ├── era_scraper.py
│   ├── realtyworld_scraper.py
│   ├── remax_db.py         # Remax SQLite yardımcıları
│   └── remax_scraper.py
│   └── turyap_scraper.py
├── Dockerfile
├── requirements.txt
└── README.md
```

---

## Lisans

Özel kullanım. Yeniden dağıtım yasaktır.
