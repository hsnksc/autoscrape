/**
 * zyte-scraper.js — Zyte API ile emlak ilanı scrape
 *
 * API: POST https://api.zyte.com/v1/extract
 * Auth: Basic (apiKey:)
 * Body: { url, browserHtml: true } veya { url, httpResponseBody: true }
 */

import * as cheerio from 'cheerio';

const ZYTE_API = 'https://api.zyte.com/v1/extract';

// JS rendering gereken domain'ler (SPA / heavy JS)
const BROWSER_DOMAINS = ['hepsiemlak.com', 'sahibinden.com', 'remax.com.tr'];

// Concurrency limiti
const MAX_CONCURRENCY = 5;

export class ZyteScraper {
  #apiKey;
  #logger;

  constructor() {
    this.#apiKey = process.env.ZYTE_API_KEY;
    this.#logger = null;
  }

  setLogger(logger) {
    this.#logger = logger;
  }

  /**
   * URL listesini Zyte API ile scrape eder.
   * @param {string[]} urls
   * @returns {Promise<object[]>} normalized listing array
   */
  async scrapeAll(urls, locationHint = {}) {
    if (!this.#apiKey) throw new Error('ZYTE_API_KEY eksik');
    if (!urls.length) return [];

    const startTime = Date.now();
    this.#logger?.info({ urlCount: urls.length }, 'Zyte scrape başlıyor');

    const results = [];
    for (let i = 0; i < urls.length; i += MAX_CONCURRENCY) {
      const batch = urls.slice(i, i + MAX_CONCURRENCY);
      const batchResults = await Promise.allSettled(
        batch.map((url) => this.#scrapeOne(url, locationHint)),
      );
      for (const r of batchResults) {
        if (r.status === 'fulfilled' && r.value) {
          results.push(r.value);
        }
      }
    }

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    this.#logger?.info({ count: results.length, total: urls.length, elapsed: `${elapsed}s` }, 'Zyte scrape tamamlandı');
    return results;
  }

  /**
   * Tek bir URL'yi Zyte API ile fetch + parse eder.
   */
  async #scrapeOne(url, locationHint = {}) {
    try {
      const domain = this.#domain(url);
      const needsBrowser = BROWSER_DOMAINS.some((d) => domain.includes(d));

      const body = {
        url,
        geolocation: 'TR',
      };

      if (needsBrowser) {
        body.browserHtml = true;
      } else {
        body.httpResponseBody = true;
      }

      const authHeader = 'Basic ' + Buffer.from(this.#apiKey + ':').toString('base64');

      const res = await fetch(ZYTE_API, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: authHeader,
          'Accept-Encoding': 'gzip, deflate',
        },
        body: JSON.stringify(body),
        signal: AbortSignal.timeout(45_000),
      });

      if (!res.ok) {
        const errText = await res.text().catch(() => '');
        this.#logger?.warn({ url: url.slice(0, 60), status: res.status, err: errText.slice(0, 100) }, 'Zyte HTTP hatası');
        return null;
      }

      const data = await res.json();

      let html;
      if (data.browserHtml) {
        html = data.browserHtml;
      } else if (data.httpResponseBody) {
        // base64 decode
        html = Buffer.from(data.httpResponseBody, 'base64').toString('utf-8');
      } else {
        this.#logger?.warn({ url: url.slice(0, 60) }, 'Zyte: HTML içerik yok');
        return null;
      }

      if (html.length < 500) {
        this.#logger?.warn({ url: url.slice(0, 60), len: html.length }, 'Zyte içerik çok kısa');
        return null;
      }

      return this.#parseHtml(html, url, domain, locationHint);
    } catch (err) {
      this.#logger?.warn({ url: url.slice(0, 60), err: err.message }, 'Zyte scrape hatası');
      return null;
    }
  }

  /**
   * HTML'den cheerio ile ilan verisini çıkarır.
   */
  #parseHtml(html, url, domain, locationHint = {}) {
    const $ = cheerio.load(html);
    const text = $('body').text();

    const title = $('h1').first().text().trim()
      || $('title').text().trim()
      || '';

    const price = this.#parsePriceFromHtml($, text);
    const rooms = this.#extractRooms(text);

    if (!title && !price) return null;

    // Görseller
    const images = [];
    $('img[src*="emlak"], img[src*="ilan"], img[data-src], .gallery img, .swiper img, meta[property="og:image"]').each((_, el) => {
      const src = $(el).attr('src') || $(el).attr('data-src') || $(el).attr('content') || '';
      if (src && src.startsWith('http') && !src.includes('logo') && images.length < 10) {
        images.push(src);
      }
    });

    // Konum: HTML'den güçlü sinyal varsa onu kullan, yoksa geocoder hint'i
    const parsed = this.#parseLocation($, text, url);
    // Güçlü sinyal: h1 veya title'dan geliyorsa (URL parse'dan değil) — ama her halükarda
    // hint city metin içinde geçiyorsa hint'i önceliklendir
    const hintCity = locationHint.city || '';
    const cityFromHtml = parsed.city || '';
    const city = (hintCity && text.includes(hintCity)) ? hintCity : (cityFromHtml || hintCity);
    const district = parsed.district || locationHint.district || '';

    return {
      url,
      domain,
      title: title.slice(0, 200),
      price,
      currency: 'TRY',
      city,
      district,
      rooms,
      grossM2: this.#extractM2(text, 'brut'),
      netM2: this.#extractM2(text, 'net'),
      floor: this.#extractFloor(text),
      buildingAge: this.#extractBuildingAge(text),
      isCreditEligible: /kredi.*uygun/i.test(text),
      hasElevator: /asans[öo]r/i.test(text),
      hasParking: /otopark|garaj/i.test(text),
      furnished: /e[şs]yal[ıi]/i.test(text),
      description: this.#extractDescription($),
      images,
      publishedDate: null,
      score: 0,
      highlights: [],
      summary: '',
      source: 'zyte',
    };
  }

  // ── Parsing helpers ──

  #domain(url) {
    try {
      return new URL(url).hostname.replace('www.', '');
    } catch {
      return '';
    }
  }

  /**
   * Türk ve İngiliz sayı formatlarını doğru parse eder.
   * 28.500 → 28500, 28,500 → 28500, 4.500.000 → 4500000, 28.500,50 → 28500.5
   */
  #parseNumericPrice(raw) {
    const s = raw.trim();
    const hasDot = s.includes('.');
    const hasComma = s.includes(',');

    if (hasDot && hasComma) {
      // Türk formatı: 4.500.000,50 — noktalar binlik, virgül ondalık
      return parseFloat(s.replace(/\./g, '').replace(',', '.'));
    }
    if (hasDot) {
      // Son parça 3 basamaklıysa nokta binlik ayraç: 28.500 → 28500
      const parts = s.split('.');
      return parts[parts.length - 1].length === 3
        ? parseFloat(s.replace(/\./g, ''))
        : parseFloat(s);
    }
    if (hasComma) {
      // Son parça 3 basamaklıysa virgül binlik ayraç: 28,500 → 28500
      const parts = s.split(',');
      return parts[parts.length - 1].length === 3
        ? parseFloat(s.replace(/,/g, ''))
        : parseFloat(s.replace(',', '.'));
    }
    return parseFloat(s);
  }

  #extractPrice(text) {
    if (!text) return null;
    const str = typeof text === 'number' ? String(text) : text;

    // 1. Para sembolü ile: "28.500 TL", "28,500 TL", "4.500.000 ₺"
    const m = str.match(/([\.\d,]+)\s*(?:TL|₺)/i);
    if (m) {
      const val = this.#parseNumericPrice(m[1]);
      if (val >= 1000) return val;
    }

    // 2. Saf sayısal (CSS element'ten geliyorsa TL etiketi olmayabilir)
    const mNum = str.match(/^[\s]*([\.\d,]+)[\s]*$/);
    if (mNum) {
      const val = this.#parseNumericPrice(mNum[1]);
      if (val >= 5000) return val;
    }

    return null;
  }

  #parsePriceFromHtml($, text) {
    const selectors = [
      '.price', '[class*="price"]', '[class*="fiyat"]',
      '.listing-price', '.detail-price',
    ];
    for (const sel of selectors) {
      const el = $(sel).first();
      if (el.length) {
        const price = this.#extractPrice(el.text());
        if (price) return price;
      }
    }
    return this.#extractPrice(text);
  }

  #extractRooms(text) {
    const m = text.match(/(\d+)\s*\+\s*(\d+)/);
    return m ? `${m[1]}+${m[2]}` : '';
  }

  #extractM2(text, type) {
    const patterns = type === 'brut'
      ? [/br[üu]t[\s:]*?(\d+)/i, /(\d+)\s*m[²2].*br[üu]t/i]
      : [/net[\s:]*?(\d+)/i, /(\d+)\s*m[²2].*net/i];
    for (const p of patterns) {
      const m = text.match(p);
      if (m) return parseInt(m[1]);
    }
    return null;
  }

  #extractFloor(text) {
    const m = text.match(/(\d+)\.\s*kat/i);
    return m ? m[1] : '';
  }

  #extractBuildingAge(text) {
    const m = text.match(/bina\s*ya[sş][ıi]\s*[:]*\s*(\d+[-–]?\d*)/i);
    return m ? m[1] : '';
  }

  #parseLocation($, text, url) {
    const CITIES = [
      'İstanbul', 'Ankara', 'İzmir', 'Bursa', 'Antalya', 'Adana', 'Konya',
      'Gaziantep', 'Kocaeli', 'Mersin', 'Eskişehir', 'Kayseri', 'Trabzon',
      'Samsun', 'Alanya', 'Bodrum', 'Fethiye', 'Muğla', 'Denizli',
      'Balıkesir', 'Manisa', 'Sakarya', 'Tekirdağ', 'Hatay', 'Malatya',
    ];

    const findCity = (str) => CITIES.find((c) => str.includes(c)) || '';

    let city = '';
    let district = '';

    // 1. H1 başlığı (en güvenilir — ilan başlığı genelde şehir içerir)
    city = findCity($('h1').first().text());

    // 2. Title tag
    if (!city) city = findCity($('title').text());

    // 3. Meta description
    if (!city) {
      const meta = $('meta[name="description"], meta[property="og:description"]').first().attr('content') || '';
      city = findCity(meta);
    }

    // 4. Breadcrumb (spesifik selector — tüm <a> değil)
    if (!city) {
      const bcText = $('[class*="breadcrumb"], [class*="Breadcrumb"], nav ol, nav ul, [class*="location"]').first().text();
      city = findCity(bcText);
    }

    // 5. URL path (TR karakter normalize edilmiş hali)
    if (!city) {
      try {
        const pathname = new URL(url).pathname.toLowerCase();
        city = CITIES.find((c) => {
          const normalized = c.toLowerCase()
            .replace(/İ/g, 'i').replace(/ı/g, 'i').replace(/ş/g, 's')
            .replace(/ğ/g, 'g').replace(/ü/g, 'u').replace(/ö/g, 'o').replace(/ç/g, 'c');
          return pathname.includes(normalized);
        }) || '';
      } catch {}
    }

    // İlçe: şehir adının hemen arkasındaki büyük harfli kelime
    if (city) {
      const h1Text = $('h1').first().text();
      const searchIn = h1Text || text.substring(0, 500);
      const m = searchIn.match(new RegExp(city + '[\\s,/-]+([A-ZÇĞİÖŞÜ][a-zçğıöşü]+)'));
      if (m) district = m[1];
    }

    return { city, district };
  }

  #extractDescription($) {
    const selectors = [
      '.description', '[class*="description"]', '[class*="aciklama"]',
      '.detail-text', '.listing-description',
    ];
    for (const sel of selectors) {
      const el = $(sel).first();
      if (el.length) {
        const text = el.text().trim();
        if (text.length > 30) return text.slice(0, 500);
      }
    }
    return '';
  }
}
