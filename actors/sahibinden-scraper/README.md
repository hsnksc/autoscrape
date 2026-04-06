# Sahibinden Scraper — Apify Actor

Sahibinden.com'dan satılık/kiralık ilan listelerini Cloudflare bypass tekniği (undetected_chromedriver) ile çeker.

## ⚠️ Önemli Uyarı

Sahibinden.com agresif Cloudflare koruması kullanmaktadır. Bu actor:
- **undetected_chromedriver** kullanır (WebDriver tespitini engeller)
- Apify **residential proxy** kullanılması başarı oranını artırır
- Headless modda bazı engellerle karşılaşılabilir

## Özellikler
- Cloudflare challenge otomatik bekleyerek geçer
- Checkpoint ile kesintiden devam
- Apify Dataset'e otomatik çıktı

## Kategoriler
- `satilik`, `kiralik`
- `satilik_isyeri`, `kiralik_isyeri`
- `devren_satilik_isyeri`, `devren_kiralik_isyeri`

## Kullanım (Apify)

| Parametre | Tip | Varsayılan | Açıklama |
|-----------|-----|------------|----------|
| `categories` | array | `["satilik","kiralik"]` | Scrape edilecek kategoriler |
| `pageWorkers` | integer | `1` | Worker sayısı (1 önerilir) |
| `delay` | number | `3.0` | İstekler arası bekleme (min 2.0 önerilir) |
| `pageRanges` | string | `""` | Sayfa aralığı, ör: `1-50` veya `1-25,26-50` |

## Çıktı Alanları

`url`, `listing_id`, `title`, `category`, `property_type`, `advertiser_type`, `advertiser_name`, `city`, `county`, `district`, `price`, `currency`, `gross_sqm`, `net_sqm`, `room_count`, `floor`, `floor_count`, `building_age`, `listing_date`, `image_count`, `scraped_at_utc`

## Apify'a Deploy

```bash
apify login
apify push
```
