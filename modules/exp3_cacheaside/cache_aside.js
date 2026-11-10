class CacheAsideService {
  constructor(db, redisClient, ttlSeconds = 15) {
    this.db = db;
    this.redis = redisClient;
    this.ttl = ttlSeconds;
  }

  async getProduct(id) {
    const cacheKey = `product:${id}`;
    const startCache = Date.now();
    
    // 1. Try Redis Cache
    const cached = await this.redis.get(cacheKey);
    if (cached) {
      return {
        source: 'CACHE_HIT',
        latencyMs: Date.now() - startCache,
        data: JSON.parse(cached)
      };
    }

    // 2. Cache Miss: Fetch from DB
    const startDb = Date.now();
    const product = this.db.prepare('SELECT * FROM products WHERE id = ?').get(id);
    const dbLatency = Date.now() - startDb;

    if (product) {
      // 3. Populate Cache
      await this.redis.set(cacheKey, JSON.stringify(product), 'EX', this.ttl);
    }

    return {
      source: 'CACHE_MISS',
      latencyMs: dbLatency,
      data: product || null
    };
  }

  async invalidateCache(id) {
    await this.redis.del(`product:${id}`);
  }
}

module.exports = CacheAsideService;
