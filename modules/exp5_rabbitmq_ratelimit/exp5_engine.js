class TokenBucketLimiter {
  constructor(capacity, refillRatePerSec) {
    this.capacity = capacity;
    this.refillRate = refillRatePerSec;
    this.tokens = capacity;
    this.lastRefill = Date.now();
  }

  tryConsume() {
    const now = Date.now();
    const elapsedSec = (now - this.lastRefill) / 1000;
    this.tokens = Math.min(this.capacity, this.tokens + elapsedSec * this.refillRate);
    this.lastRefill = now;

    if (this.tokens >= 1) {
      this.tokens -= 1;
      return true;
    }
    return false;
  }
}

class SlidingWindowLogLimiter {
  constructor(redisClient, windowSizeSec, maxRequests) {
    this.redis = redisClient;
    this.window = windowSizeSec;
    this.max = maxRequests;
  }

  async tryConsume(key) {
    const now = Date.now();
    const windowStart = now - (this.window * 1000);
    const redisKey = `sliding_log:${key}`;

    const luaScript = `
      redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
      local current = redis.call('ZCARD', KEYS[1])
      if current < tonumber(ARGV[2]) then
        redis.call('ZADD', KEYS[1], ARGV[3], ARGV[3])
        redis.call('EXPIRE', KEYS[1], tonumber(ARGV[4]))
        return 1
      else
        return 0
      end
    `;
    const res = await this.redis.eval(luaScript, 1, redisKey, windowStart, this.max, now, this.window);
    return res === 1;
  }
}

module.exports = { TokenBucketLimiter, SlidingWindowLogLimiter };
