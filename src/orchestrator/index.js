#!/usr/bin/env node
import express from 'express';
import { createServer } from 'http';
import cors from 'cors';
import { pino } from 'pino';
import { v4 as uuidv4 } from 'uuid';
import { ExaSearcher } from './exa-searcher.js';
import { JobStatus } from './types.js';
import { WebSocketServer } from 'ws';
import { CacheManager } from './cache.js';
import { Geocoder } from './geocoder.js';
import { ApifyScraper } from './apify-scraper.js';
import { DirectSearcher } from './direct-searcher.js';
import { ZenRowsScraper } from './zenrows-scraper.js';
import { ZyteScraper } from './zyte-scraper.js';

const logger = pino({ level: process.env.LOG_LEVEL || 'info' });
const app = express();
const server = createServer(app);
app.use(cors());
app.use(express.json());

const exaSearcher = new ExaSearcher();
exaSearcher.setLogger(logger);

const cache = new CacheManager(logger);
const geocoder = new Geocoder();
geocoder.setLogger(logger);

const apifyScraper = new ApifyScraper();
apifyScraper.setLogger(logger);

const directSearcher = new DirectSearcher();
directSearcher.setLogger(logger);

const zenrowsScraper = new ZenRowsScraper();
zenrowsScraper.setLogger(logger);

const zyteScraper = new ZyteScraper();
zyteScraper.setLogger(logger);

// ZenRows'a yönlendirilecek domain'ler (sadece sahibinden — remax.com.tr Zyte'a yönlendirilir)
const ZENROWS_DOMAINS = ['sahibinden.com'];

const WEBHOOK_URL = process.env.WEBHOOK_URL || 'http://localhost:3001/api/webhook/apify';

// ── POST /api/search ──
app.post('/api/search', async (req, res) => {
  const { lat, lng, rooms, minPrice, maxPrice } = req.body;
  if (lat == null || lng == null) return res.status(400).json({ error: 'lat ve lng gerekli' });

  const jobId = uuidv4();

  // Fire and forget — async search
  runSearch(jobId, { lat, lng, rooms, minPrice, maxPrice });

  res.json({ jobId, status: JobStatus.SEARCHING });
});

async function runSearch(jobId, { lat, lng, rooms, minPrice, maxPrice }) {
  try {
    logger.info({ jobId, lat, lng, rooms }, 'Arama başlıyor');

    await cache.setStatus(jobId, JobStatus.SEARCHING);

    // 1. Koordinatı il/ilçe/mahalle + komşu mahallelere çevir
    const geoResult = await geocoder.reverseGeocode(lat, lng);
    const { location, mahalle, ilce, il, nearbyMahalleler = [], nearbyIlceler = [], district, city: geocodedCity } = geoResult;
    logger.info({ jobId, location, mahalle, ilce, il, nearbyMahalleler }, 'Geocode tamamlandı');

    // 2. Exa AI araması — il/ilçe/mahalle + komşu konumlar için paralel sorgular
    const { priced, linksOnly } = await exaSearcher.search({
      location, mahalle, ilce, il, nearbyMahalleler, nearbyIlceler,
      rooms, minPrice, maxPrice, geocodedCity,
    });
    logger.info({ jobId, priced: priced.length, linksOnly: linksOnly.length }, 'Exa araması tamamlandı');

    // 3. CB + Century21 doğrudan lokasyon araması (linksOnly'a eklenir)
    const directUrls = await directSearcher.searchAll(ilce || district);
    logger.info({ jobId, directUrls: directUrls.length }, 'Doğrudan arama tamamlandı');

    // Mevcut linksOnly URL set'i; direktten gelen duplicate'leri ele
    const existingUrls = new Set(linksOnly.map((r) => r.url));
    const newDirectLinks = directUrls
      .filter((u) => !existingUrls.has(u))
      .map((u) => ({ url: u }));
    const allLinksOnly = [...linksOnly, ...newDirectLinks];

    // Domain dağılımını logla (debug)
    const domainCounts = {};
    for (const r of allLinksOnly) {
      try { const d = new URL(r.url).hostname.replace('www.', ''); domainCounts[d] = (domainCounts[d] || 0) + 1; } catch {}
    }
    logger.info({ jobId, domainCounts }, 'URL domain dağılımı');

    if (allLinksOnly.length > 0) {
      // 4a. Fiyatlı ilanları beklerken sakla
      await cache.saveExaResults(jobId, priced);
      await cache.setStatus(jobId, JobStatus.SCRAPING);

      // ── Hybrid routing: sahibinden+remax → ZenRows, rest → Zyte ──
      const zenrowsUrls = [];
      const zyteUrls = [];
      for (const r of allLinksOnly) {
        try {
          const host = new URL(r.url).hostname.replace('www.', '');
          if (ZENROWS_DOMAINS.some((d) => host.includes(d))) {
            zenrowsUrls.push(r.url);
          } else {
            zyteUrls.push(r.url);
          }
        } catch {
          zyteUrls.push(r.url);
        }
      }

      logger.info({ jobId, zenrowsUrls: zenrowsUrls.length, zyteUrls: zyteUrls.length }, 'Hybrid routing dağılımı');

      // ZenRows — sahibinden + remax (paralel olarak başlat)
      const zenrowsPromise = zenrowsUrls.length > 0
        ? zenrowsScraper.scrapeAll(zenrowsUrls).catch((err) => {
            logger.error({ jobId, err }, 'ZenRows scrape hatası');
            return [];
          })
        : Promise.resolve([]);

      // Zyte — geri kalan tüm domain'ler (hepsiemlak, emlakjet, era, century21, turyap, realtyworld, cb)
      const zytePromise = zyteUrls.length > 0
        ? zyteScraper.scrapeAll(zyteUrls, { city: il || geocodedCity, district: ilce || district }).catch((err) => {
            logger.error({ jobId, err }, 'Zyte scrape hatası');
            return [];
          })
        : Promise.resolve([]);

      // Her ikisini de bekle
      const [zenrowsListings, zyteListings] = await Promise.all([zenrowsPromise, zytePromise]);
      logger.info({ jobId, zenrows: zenrowsListings.length, zyte: zyteListings.length }, 'Scrape sonuçları toplandı');

      const merged = qualify(dedup([...priced, ...zenrowsListings, ...zyteListings]));
      await cache.setResult(jobId, merged);
      await cache.setStatus(jobId, JobStatus.COMPLETED);
      broadcastResult(jobId, merged);
      logger.info({ jobId, count: merged.length }, 'Job tamamlandı (hybrid)');
    } else {
      // 4b. Link yoksa direkt tamamla
      const unique = qualify(dedup([...priced]));
      await cache.setResult(jobId, unique);
      await cache.setStatus(jobId, JobStatus.COMPLETED);
      broadcastResult(jobId, unique);
      logger.info({ jobId, count: unique.length }, 'Job tamamlandı (Apify gerekmedi)');
    }
  } catch (err) {
    logger.error({ jobId, err }, 'Arama başarısız');
    await cache.setStatus(jobId, JobStatus.FAILED, { error: err.message });
  }
}

// ── POST /api/webhook/apify ── (Apify aktör bittikten sonra buraya POST atar)
// İki farklı kaynak tetikleyebilir:
//   A) main.py doğrudan → { jobId, listings: [...], total }
//   B) Apify native webhook → { jobId, apifyRunId, status: "ACTOR.RUN.SUCCEEDED"|"FAILED", datasetId }
app.post('/api/webhook/apify', async (req, res) => {
  const { jobId, listings, datasetId, status: apifyEvent } = req.body;

  if (!jobId) {
    logger.warn({ body: req.body }, 'Webhook: jobId eksik');
    return res.status(400).json({ error: 'jobId gerekli' });
  }

  res.json({ ok: true }); // hemen 200 dön

  try {
    // Duplicate-fire koruması: job zaten tamamlandıysa atla
    const existing = await cache.getJob(jobId);
    if (existing?.status === JobStatus.COMPLETED) {
      logger.info({ jobId }, 'Webhook: job zaten tamamlandı, atlanıyor');
      return;
    }

    // ── Case A: main.py doğrudan listings gönderdi ──
    if (Array.isArray(listings)) {
      const exaResults = await cache.getExaResults(jobId);
      const merged = qualify(dedup([...exaResults, ...listings]));
      await cache.setResult(jobId, merged);
      await cache.setStatus(jobId, JobStatus.COMPLETED);
      broadcastResult(jobId, merged);
      logger.info({ jobId, merged: merged.length }, 'Job tamamlandı (main.py webhook)');
      return;
    }

    // ── Case B: Apify native webhook ──
    if (apifyEvent === 'ACTOR.RUN.FAILED') {
      logger.warn({ jobId }, 'Apify aktör başarısız — sadece Exa sonuçları kullanılıyor');
      const exaResults = await cache.getExaResults(jobId);
      if (exaResults.length > 0) {
        await cache.setResult(jobId, exaResults);
        await cache.setStatus(jobId, JobStatus.COMPLETED);
        broadcastResult(jobId, exaResults);
      } else {
        await cache.setStatus(jobId, JobStatus.FAILED, { error: 'Apify aktör başarısız oldu' });
      }
      return;
    }

    // ACTOR.RUN.SUCCEEDED + datasetId → fallback: dataset'ten çek
    if (datasetId) {
      const apifyListings = await apifyScraper.fetchDataset(datasetId);
      const exaResults = await cache.getExaResults(jobId);
      const merged = qualify(dedup([...exaResults, ...apifyListings]));
      await cache.setResult(jobId, merged);
      await cache.setStatus(jobId, JobStatus.COMPLETED);
      broadcastResult(jobId, merged);
      logger.info({ jobId, merged: merged.length }, 'Job tamamlandı (Apify dataset fallback)');
    }
  } catch (err) {
    logger.error({ jobId, err }, 'Webhook işleme başarısız');
    await cache.setStatus(jobId, JobStatus.FAILED, { error: err.message });
  }
});

// Apify run tamamlanana kadar polling yapar ve sonuçları döndürür
async function waitApifyResults(runId, maxWaitMs = 3 * 60 * 1000) {
  const token = process.env.APIFY_TOKEN;
  const pollInterval = 5_000;
  const deadline = Date.now() + maxWaitMs;

  while (Date.now() < deadline) {
    await new Promise((r) => setTimeout(r, pollInterval));

    const res = await fetch(`https://api.apify.com/v2/actor-runs/${runId}?token=${token}`);
    if (!res.ok) continue;
    const { data } = await res.json();
    const status = data?.status;

    if (status === 'SUCCEEDED') {
      const datasetId = data?.defaultDatasetId;
      return datasetId ? await apifyScraper.fetchDataset(datasetId) : [];
    }

    if (status === 'FAILED' || status === 'ABORTED' || status === 'TIMED-OUT') {
      logger.warn({ runId, status }, 'Apify başarısız');
      return [];
    }
  }

  logger.warn({ runId }, 'Apify polling: süre aşıldı');
  return [];
}

// Apify run tamamlanana kadar polling yapar (localhost geliştirme ortamı için)
async function pollApifyRun(jobId, runId, maxWaitMs = 3 * 60 * 1000) {
  const token = process.env.APIFY_TOKEN;
  const pollInterval = 5_000; // 5s
  const deadline = Date.now() + maxWaitMs;

  while (Date.now() < deadline) {
    await new Promise((r) => setTimeout(r, pollInterval));

    const res = await fetch(`https://api.apify.com/v2/actor-runs/${runId}?token=${token}`);
    if (!res.ok) continue;
    const { data } = await res.json();
    const status = data?.status;

    if (status === 'SUCCEEDED') {
      const datasetId = data?.defaultDatasetId;
      const apifyListings = datasetId ? await apifyScraper.fetchDataset(datasetId) : [];
      const exaListings   = (await cache.getExaResults(jobId)) ?? [];
      const merged = dedup([...exaListings, ...apifyListings]);
      await cache.setResult(jobId, merged);
      await cache.setStatus(jobId, JobStatus.COMPLETED);
      broadcastResult(jobId, merged);
      logger.info({ jobId, runId, count: merged.length }, 'Apify polling: job tamamlandı');
      return;
    }

    if (status === 'FAILED' || status === 'ABORTED' || status === 'TIMED-OUT') {
      const exaListings = (await cache.getExaResults(jobId)) ?? [];
      const unique = dedup([...exaListings]);
      await cache.setResult(jobId, unique);
      await cache.setStatus(jobId, JobStatus.COMPLETED);
      broadcastResult(jobId, unique);
      logger.warn({ jobId, runId, status }, 'Apify başarısız — sadece Exa sonuçları döndürüldü');
      return;
    }

    logger.debug({ jobId, runId, status }, 'Apify polling: bekleniyor');
  }

  logger.error({ jobId, runId }, 'Apify polling: süre aşıldı');
  await cache.setStatus(jobId, JobStatus.FAILED, { error: 'Apify timeout' });
}

/**
 * Fiyat, m2 (net veya brüt) ve emlak cinsi (rooms) olmayan ilanları filtreler.
 */
function qualify(listings) {
  return listings.filter((l) => {
    const hasPrice = l.price != null && l.price > 0;
    const hasM2 = (l.netM2 != null && l.netM2 > 0) || (l.grossM2 != null && l.grossM2 > 0);
    const hasType = l.rooms != null && String(l.rooms).trim() !== '';
    return hasPrice && hasM2 && hasType;
  });
}

function dedup(listings) {
  const seen = new Set();
  return listings.filter((r) => {
    const key = normalizeUrl(r.url);
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function normalizeUrl(url) {
  try {
    const u = new URL(url);
    u.search = '';
    return u.href.toLowerCase().replace(/\/$/, '');
  } catch {
    return url.toLowerCase();
  }
}

// ── GET /api/job/:jobId ──
app.get('/api/job/:jobId', async (req, res) => {
  const job = await cache.getJob(req.params.jobId);
  if (!job) return res.status(404).json({ error: 'Not found' });
  res.json(job);
});

// ── POST /api/search-zenrows ── ZenRows kıyaslama endpoint'i
// Aynı URL'leri ZenRows API ile scrape eder — Apify ile kıyaslamak için
app.post('/api/search-zenrows', async (req, res) => {
  const { lat, lng, rooms, minPrice, maxPrice } = req.body;
  if (lat == null || lng == null) return res.status(400).json({ error: 'lat ve lng gerekli' });

  try {
    const startTime = Date.now();

    // 1. Geocode
    const { location, district } = await geocoder.reverseGeocode(lat, lng);

    // 2. Exa — sadece linksOnly URL'leri topluyoruz
    const { priced, linksOnly } = await exaSearcher.search({ location, rooms, minPrice, maxPrice });

    // 3. Direct URLs
    const directUrls = await directSearcher.searchAll(district);
    const existingUrls = new Set(linksOnly.map((r) => r.url));
    const newDirectLinks = directUrls.filter((u) => !existingUrls.has(u));
    const allUrls = [...linksOnly.map((r) => r.url), ...newDirectLinks];

    // Domain dağılımı
    const domainCounts = {};
    for (const u of allUrls) {
      try { const d = new URL(u).hostname.replace('www.', ''); domainCounts[d] = (domainCounts[d] || 0) + 1; } catch {}
    }
    logger.info({ domainCounts, total: allUrls.length }, 'ZenRows URL domain dağılımı');

    // 4. ZenRows ile scrape
    const zenrowsListings = await zenrowsScraper.scrapeAll(allUrls);

    // 5. Exa priced + ZenRows sonuçlarını birleştir
    const merged = dedup([...priced, ...zenrowsListings]);

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);

    // Domain bazlı kaynak sayımı
    const sourceCounts = {};
    for (const l of merged) {
      sourceCounts[l.source] = (sourceCounts[l.source] || 0) + 1;
    }

    res.json({
      status: 'completed',
      elapsed: `${elapsed}s`,
      total: merged.length,
      sourceCounts,
      domainCounts,
      urlsSent: allUrls.length,
      listings: merged,
    });
  } catch (err) {
    logger.error({ err }, 'ZenRows arama hatası');
    res.status(500).json({ error: err.message });
  }
});

// ── POST /api/search-compare ── 3 sistemi kıyasla: ZenRows vs Apify vs Zyte
app.post('/api/search-compare', async (req, res) => {
  const { lat, lng, rooms, minPrice, maxPrice } = req.body;
  if (lat == null || lng == null) return res.status(400).json({ error: 'lat ve lng gerekli' });

  try {
    const startTime = Date.now();

    // 1. Geocode
    const { location, district } = await geocoder.reverseGeocode(lat, lng);

    // 2. Exa
    const { priced, linksOnly } = await exaSearcher.search({ location, rooms, minPrice, maxPrice });

    // 3. Direct URLs
    const directUrls = await directSearcher.searchAll(district);
    const existingUrls = new Set(linksOnly.map((r) => r.url));
    const newDirectLinks = directUrls.filter((u) => !existingUrls.has(u));
    const allUrls = [...linksOnly.map((r) => r.url), ...newDirectLinks];

    // Domain dağılımı
    const urlDomainCounts = {};
    for (const u of allUrls) {
      try { const d = new URL(u).hostname.replace('www.', ''); urlDomainCounts[d] = (urlDomainCounts[d] || 0) + 1; } catch {}
    }
    logger.info({ urlDomainCounts, total: allUrls.length }, 'Karşılaştırma: URL domain dağılımı');

    // 4. 3 scraper'ı paralel çalıştır
    const [zenrowsResults, apifyResults, zyteResults] = await Promise.all([
      zenrowsScraper.scrapeAll(allUrls).catch((err) => {
        logger.error({ err }, 'ZenRows karşılaştırma hatası');
        return [];
      }),
      // Apify için kısa polling (2dk timeout)
      (async () => {
        try {
          const runId = await apifyScraper.run(allUrls, `compare-${Date.now()}`, null);
          return await waitApifyResults(runId, 2 * 60 * 1000);
        } catch (err) {
          logger.error({ err }, 'Apify karşılaştırma hatası');
          return [];
        }
      })(),
      zyteScraper.scrapeAll(allUrls).catch((err) => {
        logger.error({ err }, 'Zyte karşılaştırma hatası');
        return [];
      }),
    ]);

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);

    // Domain bazlı sonuç sayımı
    const domainBreakdown = (listings) => {
      const dm = {};
      for (const l of listings) {
        dm[l.domain] = (dm[l.domain] || 0) + 1;
      }
      return dm;
    };

    res.json({
      status: 'completed',
      elapsed: `${elapsed}s`,
      urlsSent: allUrls.length,
      exaPriced: priced.length,
      urlDomainCounts,
      systems: {
        zenrows: {
          total: zenrowsResults.length,
          domains: domainBreakdown(zenrowsResults),
        },
        apify: {
          total: apifyResults.length,
          domains: domainBreakdown(apifyResults),
        },
        zyte: {
          total: zyteResults.length,
          domains: domainBreakdown(zyteResults),
        },
      },
      // Listings ayrı ayrı
      listings: {
        exa: priced,
        zenrows: zenrowsResults,
        apify: apifyResults,
        zyte: zyteResults,
      },
    });
  } catch (err) {
    logger.error({ err }, 'Karşılaştırma hatası');
    res.status(500).json({ error: err.message });
  }
});

// ── WebSocket ──
const wss = new WebSocketServer({ server, path: '/ws' });
const subs = new Map();

wss.on('connection', (ws) => {
  ws.on('message', (msg) => {
    try {
      const { subscribe } = JSON.parse(msg);
      if (subscribe) {
        if (!subs.has(subscribe)) subs.set(subscribe, new Set());
        subs.get(subscribe).add(ws);
      }
    } catch {}
  });
  ws.on('close', () => subs.forEach((s) => s.delete(ws)));
});

function broadcastResult(jobId, data) {
  const wsSet = subs.get(jobId);
  if (!wsSet) return;
  const msg = JSON.stringify({ jobId, data });
  for (const ws of wsSet) {
    if (ws.readyState === 1) ws.send(msg);
  }
  subs.delete(jobId);
}

server.listen(process.env.PORT || 3001, () => {
  console.log('Orchestrator on :3001');
});
