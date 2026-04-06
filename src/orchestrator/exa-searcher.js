/**
 * Exa Search API client for Turkish real estate
 *
 * Exa does neural search AND content extraction in a single call.
 * No Apify / Playwright needed — Exa already gives us the full page text.
 *
 * Docs: https://docs.exa.ai/reference/search
 */

const EXA_SEARCH = 'https://api.exa.ai/search';
const EXA_CONTENTS = 'https://api.exa.ai/contents';

const EMARKA_DOMAINS = [
  'sahibinden.com',
  'hepsiemlak.com',
  'emlakjet.com',
  'zingat.com',
  'hurriyetemlak.com',
];

export class ExaSearcher {
  #logger;
  #apiKey;

  constructor() {
    this.#apiKey = process.env.EXA_API_KEY;
    this.#logger = null;
  }

  setLogger(logger) {
    this.#logger = logger;
  }

  /**
   * Build a natural-language query from structured criteria.
   */
  #buildQuery({ location, rooms, minPrice, maxPrice }) {
    const parts = [location];
    if (rooms) parts.push(rooms);
    if (minPrice || maxPrice) {
      const priceText = `${minPrice ? minPrice.toLocaleString('tr-TR') : ''}${maxPrice ? '-' + maxPrice.toLocaleString('tr-TR') : ''}`;
      parts.push(priceText + ' TL');
    }
    parts.push('ilan');
    return parts.filter(Boolean).join(' ');
  }

  /**
   * Search + fetch contents in a single Exa call.
   * Returns normalized listing objects.
   */
  async search({ location, rooms, minPrice, maxPrice }) {
    if (!this.#apiKey) throw new Error('EXA_API_KEY is not set');

    const query = this.#buildQuery({ location, rooms, minPrice, maxPrice });
    this.#logger?.info({ query }, 'Exa search');

    const body = {
      query,
      type: 'neural',
      useAutoprompt: false,
      numResults: 50,
      contents: {
        text: true,
        highlights: { numSentences: 6 },
        summary: true,
      },
      includeDomains: EMARKA_DOMAINS,
    };

    const res = await fetch(EXA_SEARCH, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': this.#apiKey,
      },
      body: JSON.stringify(body),
    });

    if (!res.ok) {
      const err = await res.text();
      throw new Error(`Exa ${res.status}: ${err}`);
    }

    const data = await res.json();
    this.#logger?.info({ count: data.results?.length }, 'Exa response');

    const listings = this.#normalize(data.results || []);
    return this.#splitResults(listings);
  }

  /**
   * Fiyat parse edilebilenler → priced, edilemeyenler → linksOnly.
   */
  #splitResults(listings) {
    const priced = listings.filter((l) => l.price !== null && l.price > 0);
    const linksOnly = listings.filter((l) => l.price === null || l.price <= 0);
    return { priced, linksOnly };
  }

  /**
   * Fetch full page contents for specific URLs (contents endpoint).
   */
  async fetchUrls(urls) {
    if (!this.#apiKey || urls.length === 0) return [];

    const res = await fetch(EXA_CONTENTS, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': this.#apiKey,
      },
      body: JSON.stringify({
        ids: urls,
        contents: { text: true, highlights: {} },
      }),
    });

    if (!res.ok) return [];

    const data = await res.json();
    return this.#normalize(data.results || []);
  }

  /**
   * Normalize Exa results into a standard emlak schema.
   */
  #normalize(rawResults) {
    return rawResults.map((r) => {
      const text = r.text || '';
      const domain = this.#domain(r.url);

      return {
        url: r.url,
        domain,
        title: this.#parseTitle(r, text),
        price: this.#parsePrice(text),
        currency: 'TRY',

        city: this.#parseCity(text, r.url),
        district: this.#parseDistrict(text),
        rooms: this.#parseRooms(text),
        grossM2: this.#parseM2(text, 'brut'),
        netM2: this.#parseM2(text, 'net'),
        buildingAge: this.#extract(text, /bina yasi[:\s]+([^\n]+)/i),
        floor: this.#extract(text, /kat[:\s]+([^\n]+)/i),

        isCreditEligible: /kredi.*uygun/i.test(text),
        hasElevator: /asansor|asansör/i.test(text) && /var/i.test(text),
        hasParking: /otopark|park/i.test(text),
        furnished: /esyali|eşyalı/i.test(text),

        description: this.#pickDescription(text),
        images: [], // Exa returns images[] too — can add if needed
        publishedDate: r.publishedDate || null,

        score: r.score || 0,
        highlights: r.highlights || [],
        summary: r.summary || '',
        source: 'exa',
      };
    });
  }

  // ── Parsing helpers ──

  #domain(url) {
    try {
      return new URL(url).hostname.replace('www.', '');
    } catch {
      return '';
    }
  }

  #parseTitle(r, text) {
    return r.title || text.split('\n').find((l) => l.trim().length > 10)?.slice(0, 120) || '';
  }

  #parsePrice(text) {
    // Match Turkish formatted prices like 4.500.000 TL
    const m = text.match(/([\d.]+)\s*TL/i);
    if (!m) return null;
    return parseFloat(m[1].replace(/\./g, ''));
  }

  #parseCity(text, url) {
    const cities = ['İstanbul', 'Ankara', 'İzmir', 'Bursa', 'Antalya', 'Adana'];
    for (const c of cities) {
      if (text.includes(c)) return c;
    }
    try {
      // Try to extract from URL path
      const parts = new URL(url).pathname.split('/').filter(Boolean);
      for (const p of parts) {
        if (cities.some((c) => c.toLowerCase().includes(p.toLowerCase()))) return p;
      }
    } catch {}
    return '';
  }

  #parseDistrict(text) {
    const m = text.match(/([A-ZÇĞİÖŞÜ][a-zçğıöşü]+)\s*\/\s*([A-ZÇĞİÖŞÜ][a-zçğıöşü]+)/);
    return m ? m[2] : '';
  }

  #parseRooms(text) {
    const m = text.match(/(\d+)\+(\d+)/);
    return m ? `${m[1]}+${m[2]}` : '';
  }

  #parseM2(text, type) {
    const pattern = type === 'brut'
      ? /brut[:\s]+m²?[:\s]*(\d+)/i
      : /net[:\s]+m²?[:\s]*(\d+)/i;
    const m = text.match(pattern);
    return m ? parseInt(m[1]) : null;
  }

  #extract(text, pattern) {
    const m = text.match(pattern);
    return m ? m[1].trim() : '';
  }

  #pickDescription(text) {
    // Find the longest paragraph
    const paragraphs = text.split(/\n{2,}/).filter((p) => p.trim().length > 50);
    return paragraphs.sort((a, b) => b.length - a.length)[0]?.slice(0, 500) || '';
  }
}
