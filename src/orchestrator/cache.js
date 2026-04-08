import IORedis from 'ioredis';
import { JobStatus } from './types.js';

const JOB_TTL = 3600;     // 1h for job status
const LISTING_TTL = 86400; // 24h for listing cache

export class CacheManager {
  #redis;
  #logger;
  /** In-memory fallback when Redis is unavailable */
  #mem = new Map();
  #redisOk = true;

  constructor(logger) {
    this.#logger = logger;
    this.#redis = new IORedis(process.env.REDIS_URL || 'redis://localhost:6379', {
      lazyConnect: true,
      maxRetriesPerRequest: null,
      retryStrategy: (times) => (times > 3 ? null : Math.min(times * 500, 2000)),
      enableOfflineQueue: false,
    });
    this.#redis.on('error', (err) => {
      if (this.#redisOk) {
        this.#logger?.warn({ err: err.message }, 'Redis bağlantı hatası — in-memory fallback aktif');
        this.#redisOk = false;
      }
    });
    this.#redis.on('connect', () => {
      this.#redisOk = true;
      this.#logger?.info('Redis bağlantısı kuruldu');
    });
  }

  async #r(fn) {
    try {
      const result = await fn();
      this.#redisOk = true;
      return result;
    } catch {
      this.#redisOk = false;
      return null;
    }
  }

  async setStatus(jobId, status, extra = {}) {
    const key = `job:${jobId}`;
    const payload = { status, ...this.#serialize(extra), updatedAt: String(Date.now()) };
    const ok = await this.#r(async () => {
      await this.#redis.hset(key, payload);
      await this.#redis.expire(key, JOB_TTL);
      return true;
    });
    if (!ok) {
      const existing = this.#mem.get(key) || {};
      this.#mem.set(key, { ...existing, ...payload });
    }
  }

  async getJob(jobId) {
    const key = `job:${jobId}`;
    let data = await this.#r(() => this.#redis.hgetall(key));
    if (!data) data = this.#mem.get(key) || null;
    if (!data || !data.status) return null;

    let result = null;
    if (data.status === JobStatus.COMPLETED) {
      const resultKey = `job:${jobId}:result`;
      const raw = await this.#r(() => this.#redis.get(resultKey))
        ?? this.#mem.get(resultKey) ?? null;
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

  async saveExaResults(jobId, listings) {
    const key = `job:${jobId}:exa_results`;
    const val = JSON.stringify(listings);
    const ok = await this.#r(() => this.#redis.set(key, val, 'EX', JOB_TTL));
    if (!ok) this.#mem.set(key, val);
  }

  async getExaResults(jobId) {
    const key = `job:${jobId}:exa_results`;
    const raw = (await this.#r(() => this.#redis.get(key))) ?? this.#mem.get(key) ?? null;
    return raw ? JSON.parse(raw) : [];
  }

  async storeListings(jobId, listings) {
    const key = `job:${jobId}:listings`;
    const val = JSON.stringify(listings);
    const ok = await this.#r(() => this.#redis.set(key, val, 'EX', JOB_TTL));
    if (!ok) this.#mem.set(key, val);
  }

  async setResult(jobId, result) {
    const key = `job:${jobId}:result`;
    const val = JSON.stringify(result);
    const ok = await this.#r(() => this.#redis.set(key, val, 'EX', JOB_TTL));
    if (!ok) this.#mem.set(key, val);
  }

  async storeListing(url, listing) {
    const key = `listing:${url}`;
    const val = JSON.stringify(listing);
    const ok = await this.#r(() => this.#redis.set(key, val, 'EX', LISTING_TTL));
    if (!ok) this.#mem.set(key, val);
  }

  async getListing(url) {
    const key = `listing:${url}`;
    const raw = (await this.#r(() => this.#redis.get(key))) ?? this.#mem.get(key) ?? null;
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
