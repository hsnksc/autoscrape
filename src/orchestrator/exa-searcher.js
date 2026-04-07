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

// Ana portaller — tek büyük sorguda iyi sonuç veriyorlar
const PORTAL_DOMAINS = [
  'emlakjet.com',
  'zingat.com',
];

// Per-domain sorgular — her biri için 10'ar sonuç (portal sorgusunda gölge düşüyor)
const BRAND_DOMAINS = [
  'hepsiemlak.com',
  'hurriyetemlak.com',
  'era.com.tr',
  'century21.com.tr',
  'remax.com.tr',
  'turyap.com.tr',
  'realtyworld.com.tr',
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
   * Konum parçalarından arama sorgu metni oluşturur.
   * Örn: "Gebizli Muratpaşa Antalya 2+1 1.000.000-5.000.000 TL satılık daire"
   */
  #buildQuery({ locationStr, rooms, minPrice, maxPrice }) {
    const parts = [locationStr, 'satılık daire'];
    if (rooms) parts.push(rooms);
    if (minPrice || maxPrice) {
      const priceText = `${minPrice ? minPrice.toLocaleString('tr-TR') : ''}${maxPrice ? '-' + maxPrice.toLocaleString('tr-TR') : ''} TL`;
      parts.push(priceText);
    }
    parts.push('ilan');
    return parts.filter(Boolean).join(' ');
  }

  /**
   * Search + fetch contents.
   * @param {object} params
   * @param {string}   params.location       — eski compat (geocoder'ın birleşik string'i)
   * @param {string}   params.mahalle        — Google'dan gelen mahalle adı
   * @param {string}   params.ilce           — ilçe adı
   * @param {string}   params.il             — il adı
   * @param {string[]} params.nearbyMahalleler — komşu mahalle adları (merkez dahil)
   * @param {string[]} params.nearbyIlceler  — komşu ilçe adları
   * @param {string}   params.rooms
   * @param {number}   params.minPrice
   * @param {number}   params.maxPrice
   * @param {string}   params.geocodedCity   — backward compat
   */
  async search({ location, mahalle, ilce, il, nearbyMahalleler = [], nearbyIlceler = [], rooms, minPrice, maxPrice, geocodedCity = '' }) {
    if (!this.#apiKey) throw new Error('EXA_API_KEY is not set');

    const city = il || geocodedCity;

    // Aranacak konum listesini oluştur (benzersiz mahalle + ilçe kombinasyonları)
    // Önce en spesifik (mahalle+ilçe+il), sonra diğer yakın mahalleler
    const locationStrings = this.#buildLocationList({ mahalle, ilce, il, nearbyMahalleler, nearbyIlceler, location });
    this.#logger?.info({ locationStrings }, 'Exa konum listesi');

    // Her konum için portal + brand sorgusu çalıştır (paralel gruplar)
    const allListingsMap = new Map(); // URL → listing (dedupe için)

    await Promise.all(
      locationStrings.map(async (locStr) => {
        const query = this.#buildQuery({ locationStr: locStr, rooms, minPrice, maxPrice });
        this.#logger?.info({ query }, 'Exa sorgu');

        const portalBody = {
          query,
          type: 'neural',
          useAutoprompt: false,
          numResults: 30,
          contents: {
            text: true,
            highlights: { numSentences: 6 },
            summary: true,
          },
          includeDomains: PORTAL_DOMAINS,
        };

        const brandBodies = BRAND_DOMAINS.map((domain) => ({
          query,
          type: 'neural',
          useAutoprompt: false,
          numResults: 8,
          contents: { text: false },
          includeDomains: [domain],
        }));

        const [portalRes, ...brandResArr] = await Promise.all([
          this.#exaPost(portalBody).catch(() => null),
          ...brandBodies.map((b) => this.#exaPost(b).catch(() => null)),
        ]);

        const listings = [
          ...this.#normalize(portalRes?.results || [], city),
          ...brandResArr.filter(Boolean).flatMap((r) => this.#normalize(r?.results || [], city)),
        ].filter((l) => this.#isValidListingUrl(l.url));

        for (const l of listings) {
          if (!allListingsMap.has(l.url)) allListingsMap.set(l.url, l);
        }
      }),
    );

    const unique = [...allListingsMap.values()];
    this.#logger?.info({ total: unique.length, locations: locationStrings.length }, 'Exa toplam sonuç');
    return this.#splitResults(unique);
  }

  /**
   * Aranacak konum string listesini oluşturur.
   * Merkez mahalle en başta olmak üzere, farklı komşu mahalleler eklenir.
   * Maksimum 3 farklı konum sorgusuna izin verilir (API maliyeti kontrolü).
   */
  #buildLocationList({ mahalle, ilce, il, nearbyMahalleler, nearbyIlceler, location }) {
    const MAX_LOCATIONS = 3;
    const result = [];

    if (ilce && il) {
      if (mahalle) {
        // Ana konum: mahalle + ilçe + il
        result.push(`${mahalle} ${ilce} ${il}`);
        // Komşu mahalleler (ana mahalleden farklı olanlar, aynı ilçede)
        for (const m of nearbyMahalleler) {
          if (result.length >= MAX_LOCATIONS) break;
          if (m !== mahalle) result.push(`${m} ${ilce} ${il}`);
        }
      } else {
        // Mahalle bilinmiyorsa doğrudan ilçe + il
        result.push(`${ilce} ${il}`);
      }
      // Yakın farklı ilçeler varsa onları da ekle
      for (const i of (nearbyIlceler || [])) {
        if (result.length >= MAX_LOCATIONS) break;
        if (i !== ilce) result.push(`${i} ${il}`);
      }
    } else if (il) {
      result.push(il);
    } else {
      result.push(location || '');
    }

    return result.filter(Boolean);
  }

  /**
   * Internal helper: POST to Exa search endpoint.
   */
  async #exaPost(body) {
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
    return res.json();
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
   * URL'nin geçerli bir ilan detay sayfası olup olmadığını kontrol eder.
   * CDN URL'leri, kategori sayfaları ve alakasız içerikler filtreler.
   */
  #isValidListingUrl(url) {
    if (!url) return false;
    try {
      const u = new URL(url);
      const host = u.hostname;
      const path = u.pathname;

      // CDN/image domain'leri filtrele
      if (/image\d*\.|cdn\.|static\.|media\./.test(host)) return false;

      // Sahibinden: ZenRows ile scrape edilecek — Exa'dan URL'leri al
      // (artık filtrelenmeyecek, URL routing'de ZenRows'a yönlendirilecek)

      // Hepsiemlak: tüm URL'lere izin ver — Apify search sayfalarını detail URL'lere genişletir
      if (host.includes('hepsiemlak.com')) {
        return true;
      }

      return true;
    } catch {
      return false;
    }
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
  #normalize(rawResults, geocodedCity = '') {
    return rawResults.map((r) => {
      const text = r.text || '';
      const domain = this.#domain(r.url);

      return {
        url: r.url,
        domain,
        title: this.#parseTitle(r, text),
        price: this.#parsePrice(text),
        currency: 'TRY',

        city: this.#parseCity(text, r.url, geocodedCity),
        district: this.#parseDistrict(text),
        rooms: this.#parseRooms(text),
        grossM2: this.#parseM2(text, 'brut'),
        netM2: this.#parseM2(text, 'net'),
        buildingAge: this.#parseBuildingAge(text),
        floor: this.#parseFloor(text),

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
    // 28.500 TL, 28,500 TL, 4.500.000 TL, 28.500,00 TL formatlarını destekler
    const m = text.match(/([\d.,]+)\s*TL/i);
    if (!m) return null;
    const raw = m[1];
    const hasDot = raw.includes('.');
    const hasComma = raw.includes(',');
    let val;
    if (hasDot && hasComma) {
      val = parseFloat(raw.replace(/\./g, '').replace(',', '.'));
    } else if (hasDot) {
      const parts = raw.split('.');
      val = parts[parts.length - 1].length === 3
        ? parseFloat(raw.replace(/\./g, ''))
        : parseFloat(raw);
    } else if (hasComma) {
      const parts = raw.split(',');
      val = parts[parts.length - 1].length === 3
        ? parseFloat(raw.replace(/,/g, ''))
        : parseFloat(raw.replace(',', '.'));
    } else {
      val = parseFloat(raw);
    }
    return val >= 1000 ? val : null;
  }

  #parseCity(text, url, geocodedCity = '') {
    const cities = ['İstanbul', 'Ankara', 'İzmir', 'Bursa', 'Antalya', 'Adana', 'Konya',
      'Gaziantep', 'Kocaeli', 'Mersin', 'Eskişehir', 'Kayseri', 'Trabzon',
      'Samsun', 'Alanya', 'Bodrum', 'Fethiye', 'Muğla', 'Denizli'];

    // Her şehrin metinde kaç kez geçtiğini say
    const counts = {};
    for (const c of cities) {
      counts[c] = (text.match(new RegExp(c, 'g')) || []).length;
    }

    // Geocoder'dan gelen şehir metinde geçiyorsa öncelikle onu kullan
    if (geocodedCity && counts[geocodedCity] >= 1) return geocodedCity;

    // Yoksa en sık geçeni kullan
    const sorted = Object.entries(counts)
      .filter(([, n]) => n > 0)
      .sort(([, a], [, b]) => b - a);
    if (sorted.length) return sorted[0][0];

    // URL'den başıvur
    try {
      const pathname = new URL(url).pathname.toLowerCase();
      return cities.find((c) => {
        const norm = c.toLowerCase().replace(/ı/g, 'i').replace(/ş/g, 's')
          .replace(/ğ/g, 'g').replace(/ü/g, 'u').replace(/ö/g, 'o').replace(/ç/g, 'c');
        return pathname.includes(norm);
      }) || '';
    } catch { return ''; }
  }

  #parseDistrict(text) {
    // "İstanbul - Kadıköy" veya "İstanbul Kadıköy" şeklinde şehirden sonraki ilçeyi bul
    const cities = ['İstanbul', 'Ankara', 'İzmir', 'Bursa', 'Antalya', 'Adana'];
    // Bilinen ilçe/semtler listesi — yanlış kelimeler gelmesini önlemek için
    const KNOWN_DISTRICTS = [
      'Kadıköy','Üsküdar','Beşiktaş','Şişli','Beyoğlu','Fatih','Bakırköy','Ataşehir',
      'Kartal','Maltepe','Pendik','Sancaktepe','Ümraniye','Beykoz','Çekmeköy','Tuzla',
      'Sultanbeyli','Çatalca','Silivri','Beylikdüzü','Esenyurt','Avcılar','Küçükçekmece',
      'Bahçelievler','Bağcılar','Güngören','Esenler','Sultangazi','Zeytinburnu',
      'Kağıthane','Sarıyer','Eyüpsultan','Arnavutköy','Bayrampaşa','Gaziosmanpaşa',
      'Büyükçekmece','Çankaya','Keçiören','Mamak','Yenimahalle','Altındağ',
      'Konak','Bornova','Karşıyaka','Buca','Çiğli',
    ];
    for (const city of cities) {
      // "Şehir - İlçe" veya "Şehir İlçe Mahalle" formatları
      const m = text.match(new RegExp(city + '\\s*[-–]\\s*([A-ZÇĞİÖŞÜ][a-zçğıöşü]+(?:\\s+[A-ZÇĞİÖŞÜ][a-zçğıöşü]+)?)'));
      if (m) {
        const candidate = m[1];
        if (KNOWN_DISTRICTS.some(d => candidate.startsWith(d))) return candidate;
      }
    }
    // "Kadıköy Bostancı Mahallesi" gibi → "Kadıköy"
    const mahalle = text.match(/([A-ZÇĞİÖŞÜ][a-zçğıöşü]+)\s+[A-ZÇĞİÖŞÜ][a-zçğıöşü]+\s+Mahallesi/);
    if (mahalle && KNOWN_DISTRICTS.includes(mahalle[1])) return mahalle[1];
    return '';
  }

  #parseRooms(text) {
    const m = text.match(/(\d+)\+(\d+)/);
    return m ? `${m[1]}+${m[2]}` : '';
  }

  #parseM2(text, type) {
    // "Net Metrekare70 m²" veya "Net Metrekare 70" veya "Brüt Metrekare90 m²" gibi
    const patterns = type === 'brut'
      ? [/brüt metrekare[\s:]*?(\d+)/i, /brut[:\s]+(\d+)\s*m/i, /(\d+)\s*m².*brüt/i]
      : [/net metrekare[\s:]*?(\d+)/i, /net[:\s]+(\d+)\s*m/i, /(\d+)\s*m².*net/i];
    for (const pattern of patterns) {
      const m = text.match(pattern);
      if (m) return parseInt(m[1]);
    }
    return null;
  }

  #parseBuildingAge(text) {
    const m = text.match(/bina(?:n[ıi]n)?\s*ya(?:s|ş)[ıi][:\s]*([^\n]{1,30})/i);
    return m ? m[1].trim() : '';
  }

  #parseFloor(text) {
    // "Bulunduğu Kat3.Kat" / "3. Kat" / "Kat: 5"
    const m =
      text.match(/bulundu[gğ]u kat[:\s]*([^\n]{1,20})/i) ||
      text.match(/(\d+)\.?\s*kat/i) ||
      text.match(/kat[:\s]*(\d+\.?\s*kat)/i);
    if (!m) return '';
    const val = m[1].trim();
    // 'kat' kelimesi yoksa ekle
    return /kat/i.test(val) ? val : val + '. Kat';
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
