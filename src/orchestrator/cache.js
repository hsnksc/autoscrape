import IORedis from 'ioredis';
import { JobStatus } from './types.js';

const JOB_TTL = 3600;     // 1h for job status
const LISTING_TTL = 86400; // 24h for listing cache

export class CacheManager {
  #redis;
  #logger;

  constructor(logger) {
    this.#logger = logger;
    this.#redis = new IORedis(process.env.REDIS_URL || 'redis://localhost:6379');
  }

  async setStatus(jobId, status, extra = {}) {
    const key = `job:${jobId}`;
    await this.#redis.hset(key, {
      status,
      ...this.#serialize(extra),
      updatedAt: Date.now(),
    });
    await this.#redis.expire(key, JOB_TTL);
  }

  async getJob(jobId) {
    const data = await this.#redis.hgetall(`job:${jobId}`);
    if (!data || !data.status) return null;

    const resultKey = `job:${jobId}:result`;
    let result = null;
    if (data.status === JobStatus.COMPLETED) {
      const raw = await this.#redis.get(resultKey);
      result = raw ? JSON.parse(raw) : null;
    }

    return {
      jobId,
      status: data.status,
      result,
      updatedAt: parseInt(data.updatedAt),
      ...this.#parseExtras(data),
    };
  }

  /** Apify bekleme sırasında Exa'nın fiyatlı ilanlarını sakla. */
  async saveExaResults(jobId, listings) {
    const key = `job:${jobId}:exa_results`;
    await this.#redis.set(key, JSON.stringify(listings), 'EX', JOB_TTL);
  }

  /** Apify webhook geldiğinde Exa ilanlarını geri al. */
  async getExaResults(jobId) {
    const raw = await this.#redis.get(`job:${jobId}:exa_results`);
    return raw ? JSON.parse(raw) : [];
  }

  async storeListings(jobId, listings) {
    const key = `job:${jobId}:listings`;
    await this.#redis.set(key, JSON.stringify(listings), 'EX', JOB_TTL);
  }

  async setResult(jobId, result) {
    const key = `job:${jobId}:result`;
    await this.#redis.set(key, JSON.stringify(result), 'EX', JOB_TTL);
  }

  async storeListing(url, listing) {
    const key = `listing:${url}`;
    await this.#redis.set(key, JSON.stringify(listing), 'EX', LISTING_TTL);
  }

  async getListing(url) {
    const raw = await this.#redis.get(`listing:${url}`);
    return raw ? JSON.parse(raw) : null;
  }

  #serialize(obj) {
    const out = {};
    for (const [k, v] of Object.entries(obj)) {
      out[k] = typeof v === 'object' ? JSON.stringify(v) : String(v);
    }
    return out;
  }

  #parseExtras(data) {
    const out = {};
    for (const [k, v] of Object.entries(data)) {
      if (k === 'status' || k === 'updatedAt') continue;
      try {
        out[k] = JSON.parse(v);
      } catch {
        out[k] = v;
      }
    }
    return out;
  }
}
