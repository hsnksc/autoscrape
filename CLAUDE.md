## AutoScrape — Exa AI + Apify Tabanlı Türk Emlak Arama Motoru

Proje: **E:/autoscrape**
AutoScrape bir **microservice**'dir. Başka bir "main backend" tarafından çağrılır.

### Mimari

```
Main Backend ──POST /api/search──► AutoScrape Orchestrator (Express :3001)
                                              │
                              Google Maps Geocoding API
                           (koordinat → mahalle/yakın çevre)
                                              │
                                       Exa AI API
                               (neural search, koordinat bölgesi sorgusu)
                                              │
                              ┌───────────────┴────────────────┐
                         Fiyatlı ilanlar                  Sadece link olanlar
                         (Exa'dan tam veri)             (çeşitli sitelerden URL)
                                              │
                                        Apify Scraper
                                  (linkleri scrape edip bilgi çıkarır)
                                              │
                                   Apify → Webhook → AutoScrape
                                              │
                              Exa fiyatlı ilanlar + Apify sonuçları birleştirilir
                                              │
Main Backend ◄─────────────── Tek response (tüm ilanlar, normalize JSON) ──────┘
```

### İşlem Akışı

1. Main backend, `POST /api/search` ile koordinat + oda + fiyat gönderir
2. AutoScrape, koordinatı Google Maps Geocoding API ile mahalle/yakın çevreye çevirir
3. Dönüştürülen konum bilgisiyle Exa AI'ya neural search sorgusu gönderilir
4. Exa sonuçları iki gruba ayrılır:
   - **Fiyatlı/tam ilanlar**: Metin içinden fiyat ve detay parse edilebiliyor
   - **Sadece link olanlar**: İçerik yok ya da yetersiz, farklı sitelerden URL
5. Sadece-link olanlar **Apify scraper**'a gönderilir (ilgili Apify aktörü çağrılır)
6. Apify linkleri scrape eder; bitince AutoScrape'e **webhook** ile POST atar
7. AutoScrape her iki sonucu (Exa tam ilanlar + Apify sonuçları) birleştirir ve normalize eder
8. Birleşik sonuç **tek seferde** main backend'e döndürülür

### Proje Dosyaları

```
autoscrape/
├── src/orchestrator/
│   ├── index.js            Express + WebSocket + async search handler
│   ├── exa-searcher.js     Exa AI client + HTML text parser + link/fiyatlı ayırımı
│   ├── apify-scraper.js    Apify aktör tetikleyici + webhook dinleyici (yapılacak)
│   ├── geocoder.js         Google Maps Geocoding API — koordinat → mahalle (yapılacak)
│   ├── cache.js            Redis cache (job status + listing cache)
│   └── types.js            JobStatus enum (searching/scraping/completed/failed)
├── client/
│   ├── App.jsx             React app: polling, state, results
│   ├── components/
│   │   ├── SearchForm.jsx  Konum, oda, fiyat formu
│   │   ├── StatusBar.jsx   Durum göstergesi (loading spinner)
│   │   └── ResultsList.jsx İlan kartları (görsel, fiyat, meta)
│   ├── index.html
│   ├── main.jsx
│   ├── index.css
│   ├── vite.config.js      Proxy: /api → :3001, /ws → :3001
│   └── package.json
├── .env                    EXA_API_KEY, APIFY_TOKEN, GOOGLE_MAPS_API_KEY burada
├── .env.example            Template
├── .gitignore
├── package.json            express, ws, ioredis, uuid, pino, cors
├── docker-compose.yml      Redis + backend
├── Dockerfile
└── README.md
```

### API Endpoint'leri

**POST `/api/search`** ← main backend buraya çağırır
```json
{ "lat": 40.9833, "lng": 29.0333, "rooms": "2+1", "minPrice": 2000000, "maxPrice": 5000000 }
```
Response: `{ "jobId": "uuid", "status": "searching" }`

**GET `/api/job/:jobId`**
Response: `{ "jobId", "status", "result": [...], "updatedAt" }`

**POST `/api/webhook/apify`** ← Apify tamamlayınca buraya POST atar
Body: Apify dataset sonuçları

**WebSocket `/ws`**
Connect → send `{"subscribe": "jobId"}` → receive `{ "jobId", "data": [...] }`

### Normalized İlan Schema

`url, domain, title, price, currency, city, district, rooms, netM2, grossM2, floor, buildingAge, isCreditEligible, hasElevator, hasParking, furnished, description, images[], publishedDate, score, highlights, summary, source`

### Geocoder Detayları (yapılacak)

- Google Maps Geocoding API (`https://maps.googleapis.com/maps/api/geocode/json`)
- Girdi: `lat`, `lng` koordinatları
- Çıktı: mahalle adı + ilçe adı (örn. `"Moda, Kadıköy"`)
- Bu bilgi Exa sorgusuna eklenir

### Exa Searcher Detayları

- Endpoint: `POST https://api.exa.ai/search`
- Auth: `x-api-key` header
- `type: "neural"`, `numResults: 50`
- `contents: { text: true, highlights: { numSentences: 6 }, summary: true }`
- `includeDomains`: sahibinden.com, hepsiemlak.com, emlakjet.com, zingat.com, hurriyetemlak.com
- Query geocoder çıktısından oluşturulur: `"Moda Kadıköy 2+1 2.000.000-5.000.000 TL ilan"`
- Sonuçlar iki gruba ayrılır:
  - **Fiyatlı/tam ilanlar**: `text` içinden fiyat parse edilebilenler → doğrudan normalize edilir
  - **Sadece link olanlar**: `text` yok ya da fiyat parse edilemiyor → Apify'a gönderilir

### Apify Scraper Detayları (yapılacak)

- Apify token ile `POST https://api.apify.com/v2/acts/{actorId}/runs` çağrılır
- Input: `{ "startUrls": [ { "url": "..." }, ... ] }`
- Apify scrape eder; tamamlayınca AutoScrape'in `/api/webhook/apify` endpoint'ine POST atar
- Webhook body'sinden scrape edilen listing verileri parse edilir ve normalize edilir

### Cache (Redis)

- Job status: `job:{jobId}` → hset, TTL 1h
- Job result: `job:{jobId}:result` → TTL 1h
- Listing cache: `listing:{normalizedUrl}` → TTL 24h
- Apify bekleme state: `job:{jobId}:exa_results` → Apify gelene kadar Exa sonuçları burada bekler

### Çalıştırma

```bash
# 1. npm install
# 2. .env dosyasında EXA_API_KEY, APIFY_TOKEN, GOOGLE_MAPS_API_KEY ayarla
# 3. Redis çalışıyor olmalı (docker compose up -d redis veya local)
# 4. npm run dev           → Backend :3001
# 5. cd client && npm run dev  → Frontend :3000
```

## Proje Analizi ve Rapor

- Proje: `AutoScrape`
- Amaç: Main backend'den gelen koordinata göre Türk emlak sitelerinden toplu ilan araması yapmak; Exa'dan tam gelen ilanlar ile Apify üzerinden kazınan ilanları birleştirerek normalize edilmiş tek bir JSON döndürmek.
- Mimari: Express microservice → Google Maps Geocoding → Exa AI → [fiyatlı + sadece link] → Apify (webhook) → merge → main backend'e tek response.

### Ana bileşenler (mevcut durum)

- `src/orchestrator/index.js`
  - `POST /api/search` ile job oluşturuyor.
  - `runSearch` içinde Exa araması yapıyor, sonuçları normalize edip URL bazlı dedupe ediyor.
  - `GET /api/job/:jobId` aracılığıyla job durumu ve sonuçları döndürüyor.
  - `/ws` WebSocket sunucusu ile abonelere sonuç yayınlayabiliyor.
  - **Eksik**: Google Maps Geocoding entegrasyonu, Apify tetikleme, webhook endpoint.

- `src/orchestrator/exa-searcher.js`
  - Exa AI `https://api.exa.ai/search` endpoint'ini kullanıyor.
  - `type: 'neural'`, `numResults: 50` ve `contents: { text: true, highlights, summary }` gönderiyor.
  - Metinden `price`, `rooms`, `netM2`, `grossM2`, `district`, `city`, `floor`, `buildingAge`, `isCreditEligible`, `hasElevator`, `hasParking`, `furnished` gibi alanları parse ediyor.
  - **Eksik**: Fiyatlı / sadece-link ayrımı yapılmıyor; tüm sonuçlar aynı şekilde işleniyor.

- `src/orchestrator/cache.js`
  - Redis anahtarları: `job:{jobId}`, `job:{jobId}:result`, `listing:{url}`
  - **Hata**: `getJob()` içinde `JobStatus` kullanılıyor ama import edilmemiş.

- `client/`
  - `App.jsx` aramayı başlatır, `jobId` ile 3 saniyede bir polling yapar.
  - `StatusBar.jsx` statü etiketleri eski/fazla akışa ait; backend durumlarıyla eşleşmiyor.
  - `package.json` içinde `devDependencies` iki kez tanımlanmış.

### Yapılacaklar (öncelik sırasıyla)

1. `src/orchestrator/geocoder.js` — Google Maps Geocoding API entegrasyonu
2. `exa-searcher.js` — Exa sonuçlarını fiyatlı / sadece-link olarak ikiye ayır
3. `src/orchestrator/apify-scraper.js` — Apify aktör tetikleme
4. `index.js` — `POST /api/webhook/apify` endpoint'i ekle; Exa + Apify sonuçlarını birleştir
5. `cache.js` — `JobStatus` import eksikliğini gider; Apify bekleme state'i ekle
6. `client/components/StatusBar.jsx` — durumları backend ile hizala
7. `package.json` — tekrar eden `devDependencies` temizle
