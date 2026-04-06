# Hepsiemlak Scraper — Apify Actor

Hepsiemlak.com'dan satılık/kiralık konut ve işyeri ilanlarını Selenium + Nuxt state üzerinden çeker.

## Özellikler
- **list_only modu**: Liste sayfasındaki Nuxt JSON state'ten hızlıca veri çeker
- **full modu**: Her ilanın detay sayfasını da açarak eksiksiz veri toplar
- Paralel worker desteği
- Checkpoint ile kesintiden devam
- Apify Dataset'e otomatik çıktı

## Kullanım (Apify)

Actor'ı Apify platformunda çalıştırın. Input parametreleri:

| Parametre | Tip | Varsayılan | Açıklama |
|-----------|-----|------------|----------|
| `categories` | array | `["satilik","kiralik"]` | Scrape edilecek kategoriler |
| `mode` | string | `"list_only"` | `list_only` veya `full` |
| `maxPages` | integer | `0` (sınırsız) | Kategori başına max sayfa |
| `pageWorkers` | integer | `2` | Paralel sayfa worker sayısı |
| `detailWorkers` | integer | `2` | Detay sayfası worker sayısı |
| `delay` | number | `1.5` | İstekler arası bekleme (sn) |

## Çıktı Alanları

`url`, `listing_id`, `title`, `category`, `property_type`, `seller_type`, `advertiser_type`, `advertiser_name`, `city`, `county`, `district`, `price`, `currency`, `gross_sqm`, `net_sqm`, `room_count`, `bathroom_count`, `floor`, `floor_count`, `building_age`, `heating`, `credit_status`, `deed_status`, `furnished`, `usage_status`, `trade_status`, `image_count`, `has_video`, `latitude`, `longitude`, `description`, `scraped_at_utc`

## Yerel Geliştirme

```bash
pip install -r requirements.txt
python src/main.py
```

## Apify'a Deploy

```bash
apify login
apify push
```
