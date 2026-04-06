# Emlakjet Scraper — Apify Actor

Emlakjet.com'dan satılık/kiralık konut, işyeri ve arsa ilanlarını Selenium ile çeker.

## Özellikler
- 7 farklı kategori desteği (satılık/kiralık konut, işyeri, devren, arsa)
- Paralel worker desteği
- Checkpoint ile kesintiden devam
- Apify Dataset'e otomatik çıktı

## Kategoriler
- `satilik_konut`, `kiralik_konut`
- `satilik_isyeri`, `kiralik_isyeri`, `devren_isyeri`
- `satilik_arsa`, `kiralik_arsa`

## Kullanım (Apify)

| Parametre | Tip | Varsayılan | Açıklama |
|-----------|-----|------------|----------|
| `categories` | array | tümü | Scrape edilecek kategoriler |
| `maxPages` | integer | `0` (sınırsız) | Kategori başına max sayfa |
| `workers` | integer | `1` | Worker sayısı |
| `delay` | number | `1.5` | İstekler arası bekleme (sn) |
| `settle` | number | `3.0` | DOM yerleşme bekleme (sn) |

## Çıktı Alanları

`url`, `listing_id`, `title`, `location`, `category`, `trade_type`, `price`, `currency`, `room_count`, `floor`, `gross_sqm`, `estate_type`, `quick_infos`, `scraped_at_utc`

## Apify'a Deploy

```bash
apify login
apify push
```
