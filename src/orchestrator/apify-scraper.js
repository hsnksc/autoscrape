/**
 * apify-scraper.js — Apify unified-scraper aktörü tetikleyici
 *
 * AutoScrape → Apify unified-real-estate-scraper → webhook → AutoScrape
 */

const APIFY_RUN_URL = 'https://api.apify.com/v2/acts';

export class ApifyScraper {
  #token;
  #actorId;
  #logger;

  constructor() {
    this.#token   = process.env.APIFY_TOKEN;
    // Apify Console'da görünen tam özelleştirilmiş actor id: "username~actor-name"
    this.#actorId = process.env.APIFY_ACTOR_ID || 'olivine_lemur~unified-real-estate-scraper';
    this.#logger  = null;
  }

  setLogger(logger) {
    this.#logger = logger;
  }

  /**
   * Apify aktörünü URL listesiyle çalıştırır.
   *
   * @param {string[]} urls          - Scrape edilecek ilan URL'leri
   * @param {string}   jobId         - AutoScrape job kimliği (webhook payload'a eklenir)
   * @param {string}   webhookUrl    - Apify bittikten sonra POST atacağı URL
   * @returns {Promise<string>}      - Apify run ID
   */
  async run(urls, jobId, webhookUrl) {
    if (!this.#token) throw new Error('APIFY_TOKEN eksik');
    if (!urls.length)  throw new Error('URL listesi boş');

    const actorIdEncoded = encodeURIComponent(this.#actorId);
    const endpoint = `${APIFY_RUN_URL}/${actorIdEncoded}/runs`;

    const body = {
      // Actor input
      urls,
      jobId,
      ...(webhookUrl ? { webhookUrl } : {}),
      concurrency:  parseInt(process.env.APIFY_CONCURRENCY  || '5'),
      requestDelay: parseFloat(process.env.APIFY_DELAY      || '0.5'),
      sahibindenCookies: process.env.SAHIBINDEN_COOKIES || '',
    };

    // Apify webhook: aktör bitince webhookUrl'e POST atar
    // Apify API'de webhooks, base64 ile encode edilmiş query param olarak gönderilir
    let queryString = `token=${this.#token}`;
    if (webhookUrl) {
      const webhooks = [
        {
          eventTypes: ['ACTOR.RUN.SUCCEEDED', 'ACTOR.RUN.FAILED'],
          requestUrl: webhookUrl,
          payloadTemplate: JSON.stringify({
            jobId,
            apifyRunId: '{{resource.id}}',
            status: '{{eventType}}',
            datasetId: '{{resource.defaultDatasetId}}',
          }),
        },
      ];
      queryString += `&webhooks=${Buffer.from(JSON.stringify(webhooks)).toString('base64')}`;
    }

    this.#logger?.info({ actorId: this.#actorId, urlCount: urls.length, jobId }, 'Apify run başlatılıyor');

    // Actor input doğrudan body olarak gönderilir (wrapper olmadan)
    const res = await fetch(`${endpoint}?${queryString}&timeout=120`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });

    if (!res.ok) {
      const err = await res.text();
      throw new Error(`Apify ${res.status}: ${err}`);
    }

    const data = await res.json();
    const runId = data?.data?.id;
    this.#logger?.info({ runId, jobId }, 'Apify run başlatıldı');
    return runId;
  }

  /**
   * Apify Dataset'teki sonuçları çeker (webhook fallback olarak kullanılabilir).
   * @param {string} datasetId
   * @returns {Promise<object[]>}
   */
  async fetchDataset(datasetId) {
    if (!this.#token) return [];

    const url = `https://api.apify.com/v2/datasets/${datasetId}/items?token=${this.#token}&format=json`;
    try {
      const res = await fetch(url);
      if (!res.ok) return [];
      return await res.json();
    } catch {
      return [];
    }
  }
}
