/**
 * direct-searcher.js — CB.com.tr ve Century21.com.tr için konum bazlı ilan URL toplayıcı.
 *
 * Bu sitelerin arama URL yapısı: https://{site}/{district-slug}-satilik/konut?pager_p=N
 * Apify actor için hazırlanan URL listesine eklenir.
 */

const MAX_PAGES = 3; // Site başına max sayfa (performans için)
const REQUEST_TIMEOUT = 15_000; // ms

/**
 * Türkçe ilçe adını URL uyumlu ASCII slug'a çevirir.
 * Örnek: "Kadıköy" → "kadikoy", "Beşiktaş" → "besiktas"
 */
function slugify(text) {
  if (!text) return '';
  return text
    .toLowerCase()
    .replace(/ğ/g, 'g')
    .replace(/ü/g, 'u')
    .replace(/ş/g, 's')
    .replace(/ı/g, 'i')
    .replace(/ö/g, 'o')
    .replace(/ç/g, 'c')
    .replace(/â/g, 'a')
    .replace(/î/g, 'i')
    .replace(/û/g, 'u')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
}

/**
 * HTML sayfasından SHB Portal ilan bağlantılarını çıkarır.
 * URL pattern: /{city}-{district}-{neighborhood}-satilik/{type}/{id}
 */
function extractListingLinks(html, baseUrl) {
  const linkPattern = /href="([^"]+(?:satilik|kiralik)[a-z0-9\/\-]*\/\d+)"/gi;
  const seen = new Set();
  const links = [];
  let match;
  while ((match = linkPattern.exec(html)) !== null) {
    const href = match[1];
    const full = href.startsWith('http') ? href : new URL(href, baseUrl).href;
    if (!seen.has(full)) {
      seen.add(full);
      links.push(full);
    }
  }
  return links;
}

/**
 * Belirtilen site ve ilçe slug'ı için listing sayfalarını çeker.
 * maxPages sayfa tarandıktan sonra durur.
 */
async function searchSite(baseUrl, districtSlug, category = 'konut', maxPages = MAX_PAGES) {
  const urls = [];

  for (let page = 1; page <= maxPages; page++) {
    const pageUrl =
      page === 1
        ? `${baseUrl}/${districtSlug}-satilik/${category}`
        : `${baseUrl}/${districtSlug}-satilik/${category}?pager_p=${page}`;

    try {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT);

      const resp = await fetch(pageUrl, {
        signal: controller.signal,
        headers: {
          'User-Agent':
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
          Accept: 'text/html,*/*',
          'Accept-Language': 'tr-TR,tr;q=0.9',
        },
        redirect: 'follow',
      });
      clearTimeout(timer);

      if (!resp.ok) break;

      const html = await resp.text();
      const pageLinks = extractListingLinks(html, baseUrl);

      if (pageLinks.length === 0) break; // Sonuç kalmadı

      urls.push(...pageLinks);
    } catch (err) {
      // Timeout veya ağ hatası — kalan sayfaları atla
      break;
    }
  }

  return urls;
}

export class DirectSearcher {
  constructor() {
    this._logger = null;
  }

  setLogger(logger) {
    this._logger = logger;
  }

  _log(msg, data = {}) {
    if (this._logger) this._logger.info(data, msg);
    else console.log('[DirectSearcher]', msg, data);
  }

  /**
   * İlçe adından CB.com.tr ve Century21.com.tr'deki ilan URL'lerini toplar.
   *
   * @param {string} districtName — Geocoder'dan gelen ilçe adı (Türkçe)
   * @returns {Promise<string[]>} — İlan detay URL'leri listesi
   */
  async searchAll(districtName) {
    if (!districtName) return [];

    const slug = slugify(districtName);
    if (!slug) return [];

    this._log(`Doğrudan arama başlıyor`, { district: districtName, slug });

    const sites = [
      { name: 'CB',       base: 'https://www.cb.com.tr' },
      { name: 'Century21', base: 'https://www.century21.com.tr' },
    ];

    const allUrls = [];

    await Promise.all(
      sites.map(async ({ name, base }) => {
        try {
          const links = await searchSite(base, slug);
          this._log(`${name} doğrudan arama tamamlandı`, {
            district: slug,
            count: links.length,
          });
          allUrls.push(...links);
        } catch (err) {
          this._log(`${name} doğrudan arama başarısız`, { err: err.message });
        }
      }),
    );

    // Deduplicate
    const unique = [...new Set(allUrls)];
    this._log('Doğrudan arama toplam', { total: unique.length });
    return unique;
  }
}
