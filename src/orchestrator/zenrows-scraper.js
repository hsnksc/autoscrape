/**
 * zenrows-scraper.js — ZenRows Universal Scraper API ile emlak ilanı scrape
 *
 * Apify'a alternatif: ZenRows API ile JS rendering + premium proxy kullanarak
 * hepsiemlak, sahibinden, emlakjet vb. sitelerden ilan verisi çeker.
 *
 * API: GET https://api.zenrows.com/v1/?url=TARGET&apikey=KEY&js_render=true&premium_proxy=true
 */

import * as cheerio from 'cheerio';

const ZENROWS_API = 'https://api.zenrows.com/v1/';

// JS rendering + premium proxy gereken domain'ler (Cloudflare korumalı)
const PREMIUM_DOMAINS = ['hepsiemlak.com', 'sahibinden.com', 'remax.com.tr'];

// Concurrency limiti (plan'a göre ayarla)
const MAX_CONCURRENCY = 5;

export class ZenRowsScraper {
  #apiKey;
  #logger;

  constructor() {
    this.#apiKey = process.env.ZENROWS_API_KEY;
    this.#logger = null;
  }

  setLogger(logger) {
    this.#logger = logger;
  }

  /**
   * URL listesini ZenRows ile scrape eder.
   * @param {string[]} urls
   * @returns {Promise<object[]>} normalized listing array
   */
  async scrapeAll(urls) {
    if (!this.#apiKey) throw new Error('ZENROWS_API_KEY eksik');
    if (!urls.length) return [];

    const startTime = Date.now();
    this.#logger?.info({ urlCount: urls.length }, 'ZenRows scrape başlıyor');

    // Concurrency-limited parallel fetch
    const results = [];
    for (let i = 0; i < urls.length; i += MAX_CONCURRENCY) {
      const batch = urls.slice(i, i + MAX_CONCURRENCY);
      const batchResults = await Promise.allSettled(
        batch.map((url) => this.#scrapeOne(url)),
      );
      for (const r of batchResults) {
        if (r.status === 'fulfilled' && r.value) {
          results.push(r.value);
        }
      }
    }

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    this.#logger?.info({ count: results.length, total: urls.length, elapsed: `${elapsed}s` }, 'ZenRows scrape tamamlandı');
    return results;
  }

  /**
   * Tek bir URL'yi ZenRows ile fetch + parse eder.
   */
  async #scrapeOne(url) {
    try {
      const domain = this.#domain(url);
      const needsPremium = PREMIUM_DOMAINS.some((d) => domain.includes(d));

      const params = new URLSearchParams({
        url,
        apikey: this.#apiKey,
        autoparse: 'true',
      });

      if (needsPremium) {
        params.set('js_render', 'true');
        params.set('premium_proxy', 'true');
        params.set('proxy_country', 'tr');
        params.set('wait', '3000');
      }

      const res = await fetch(`${ZENROWS_API}?${params}`, {
        signal: AbortSignal.timeout(30_000),
      });

      if (!res.ok) {
        this.#logger?.warn({ url: url.slice(0, 60), status: res.status }, 'ZenRows HTTP hatası');
        return null;
      }

      const contentType = res.headers.get('content-type') || '';
      const body = await res.text();

      // autoparse JSON döndüyse
      if (contentType.includes('application/json')) {
        try {
          const parsed = JSON.parse(body);
          return this.#normalizeAutoparse(parsed, url, domain);
        } catch {
          // JSON parse fail → HTML olarak devam
        }
      }

      // HTML parse
      if (body.length < 500) {
        this.#logger?.warn({ url: url.slice(0, 60), len: body.length }, 'ZenRows içerik çok kısa');
        return null;
      }

      return this.#parseHtml(body, url, domain);
    } catch (err) {
      this.#logger?.warn({ url: url.slice(0, 60), err: err.message }, 'ZenRows scrape hatası');
      return null;
    }
  }

  /**
   * ZenRows autoparse JSON çıktısını normalize eder.
   */
  #normalizeAutoparse(data, url, domain) {
    // autoparse çeşitli formatlar döndürebilir
    const title = data.title || data.name || '';
    const price = this.#extractPrice(data.price || data.amount || '');

    if (!title && !price) return null;

    return {
      url,
      domain,
      title: title.slice(0, 200),
      price,
      currency: 'TRY',
      city: data.city || '',
      district: data.district || data.location || '',
      rooms: this.#extractRooms(title + ' ' + (data.description || '')),
      grossM2: this.#extractM2(data.description || title, 'brut'),
      netM2: this.#extractM2(data.description || title, 'net'),
      floor: '',
      buildingAge: '',
      isCreditEligible: false,
      hasElevator: false,
      hasParking: false,
      furnished: false,
      description: (data.description || '').slice(0, 500),
      images: Array.isArray(data.images) ? data.images.slice(0, 10) : [],
      publishedDate: data.date || null,
      score: 0,
      highlights: [],
      summary: '',
      source: 'zenrows',
    };
  }

  /**
   * HTML'den cheerio ile ilan verisini çıkarır.
   */
  #parseHtml(html, url, domain) {
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

    // Konum
    const { city, district } = this.#parseLocation($, text, url);

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
      source: 'zenrows',
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

  #extractPrice(text) {
    if (!text) return null;
    const str = typeof text === 'number' ? String(text) : text;
    const m = str.match(/([\d.,]+)\s*(?:TL|₺|tl)/i) || str.match(/([\d.]+)/);
    if (!m) return null;
    const val = parseFloat(m[1].replace(/\./g, '').replace(',', '.'));
    return val > 1000 ? val : null;
  }

  #parsePriceFromHtml($, text) {
    // Önce fiyat içeren elementleri ara
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
    // Fallback: text'ten parse
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
    let city = '';
    let district = '';

    // Breadcrumb'dan
    const breadcrumbs = $('a').map((_, el) => $(el).text().trim()).get();
    const cities = ['İstanbul', 'Ankara', 'İzmir', 'Bursa', 'Antalya'];
    for (const bc of breadcrumbs) {
      for (const c of cities) {
        if (bc.includes(c)) { city = c; break; }
      }
    }

    // URL'den
    try {
      const parts = new URL(url).pathname.toLowerCase().split(/[-/]/);
      if (parts.includes('istanbul')) city = city || 'İstanbul';
      if (parts.includes('ankara')) city = city || 'Ankara';
    } catch {}

    // Text'ten ilçe
    const districtMatch = text.match(/(?:İstanbul|Ankara|İzmir)\s*[-–,]\s*([A-ZÇĞİÖŞÜ][a-zçğıöşü]+)/);
    if (districtMatch) district = districtMatch[1];

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
