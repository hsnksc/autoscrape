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

    // 1. Koordinatı mahalle/ilçeye çevir
    const location = await geocoder.reverseGeocode(lat, lng);
    logger.info({ jobId, location }, 'Geocode tamamlandı');

    // 2. Exa AI araması — hem fiyatlı ilanlar hem sadece-link ilanlar
    const { priced, linksOnly } = await exaSearcher.search({ location, rooms, minPrice, maxPrice });
    logger.info({ jobId, priced: priced.length, linksOnly: linksOnly.length }, 'Exa araması tamamlandı');

    // 3. CB + Century21 doğrudan lokasyon araması (linksOnly'a eklenir)
    const directUrls = await directSearcher.searchAll(location.district);
    logger.info({ jobId, directUrls: directUrls.length }, 'Doğrudan arama tamamlandı');

    // Mevcut linksOnly URL set'i; direktten gelen duplicate'leri ele
    const existingUrls = new Set(linksOnly.map((r) => r.url));
    const newDirectLinks = directUrls
      .filter((u) => !existingUrls.has(u))
      .map((u) => ({ url: u }));
    const allLinksOnly = [...linksOnly, ...newDirectLinks];

    if (allLinksOnly.length > 0) {
      // 4a. Fiyatlı ilanları Apify beklerken sakla
      await cache.saveExaResults(jobId, priced);
      await cache.setStatus(jobId, JobStatus.SCRAPING);

      // localhost ise Apify webhook çağıramaz — polling fallback kullan
      const isLocalWebhook = /localhost|127\.0\.0\.1/.test(WEBHOOK_URL);
      const webhookUrlForApify = isLocalWebhook ? null : WEBHOOK_URL;

      // 5a. Apify aktörünü tetikle (Exa link-only + doğrudan URL'ler)
      const runId = await apifyScraper.run(
        allLinksOnly.map((r) => r.url),
        jobId,
        webhookUrlForApify,
      );
      logger.info({ jobId, runId, urlCount: allLinksOnly.length, polling: isLocalWebhook }, 'Apify başlatıldı');

      if (isLocalWebhook && runId) {
        // Polling fallback: localhost geliştirme ortamı için Apify run'ı izle
        pollApifyRun(jobId, runId).catch((err) =>
          logger.error({ jobId, err }, 'Apify polling hatası'),
        );
      }
      // Production'da Apify webhook /api/webhook/apify endpoint'ine POST atar
    } else {
      // 4b. Link yoksa direkt tamamla
      const unique = dedup([...priced]);
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
      const merged = dedup([...exaResults, ...listings]);
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
      const merged = dedup([...exaResults, ...apifyListings]);
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

// Apify run tamamlanana kadar polling yapar (localhost geliştirme ortamı için)
async function pollApifyRun(jobId, runId, maxWaitMs = 10 * 60 * 1000) {
  const token = process.env.APIFY_TOKEN;
  const pollInterval = 10_000; // 10s
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
