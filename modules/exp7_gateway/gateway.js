const express = require('express');
const cors = require('cors');
const Redis = require('ioredis');
const productsRouter = require('../exp1_crud/products');
const LoadBalancerEngine = require('../exp2_loadbalancer/lb_engine');
const CacheAsideService = require('../exp3_cacheaside/cache_aside');
const { benchmarkQuery } = require('../exp4_catalog/catalog_bench');
const { TokenBucketLimiter, SlidingWindowLogLimiter } = require('../exp5_rabbitmq_ratelimit/exp5_engine');
const Database = require('better-sqlite3');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());

const db = new Database(path.join(__dirname, '../../votecore.db'));
const redis = new Redis({ host: '127.0.0.1', port: 6379, maxRetriesPerRequest: null });

const cacheAside = new CacheAsideService(db, redis);
const lbEngine = new LoadBalancerEngine([
  { url: 'http://localhost:3000', instanceId: 'instance-3000', activeConns: 0, totalRequests: 0 },
  { url: 'http://localhost:3001', instanceId: 'instance-3001', activeConns: 0, totalRequests: 0 }
]);
const tokenBucket = new TokenBucketLimiter(5, 1);
const slidingWindow = new SlidingWindowLogLimiter(redis, 10, 5);

// Gateway Authentication & Logging Middleware
app.use((req, res, next) => {
  const start = Date.now();
  res.setHeader('X-Gateway-Auth', 'VERIFIED');
  res.setHeader('X-Gateway-Processed', 'true');
  
  res.on('finish', () => {
    const duration = Date.now() - start;
    console.log(`[GATEWAY LOG] ${req.method} ${req.originalUrl} | Status: ${res.statusCode} | Latency: ${duration}ms`);
  });
  next();
});

// Gateway Health Endpoint
app.get('/health', (req, res) => {
  res.json({ status: 'UP', gatewayPort: PORT, gatewayAuth: 'VERIFIED', timestamp: new Date().toISOString() });
});

// Experiment 1 CRUD Router
app.use('/api/v1', productsRouter);

// Experiment 2 Load Balancer Proxy
app.get('/api/v1/lb-test', (req, res) => {
  const algo = req.query.algo || 'round-robin';
  const target = algo === 'least-connections' 
    ? lbEngine.getNextLeastConnections() 
    : lbEngine.getNextRoundRobin();
    
  res.json({
    algorithm: algo,
    routedTo: target.instanceId,
    targetUrl: target.url,
    totalNodeHits: target.totalRequests,
    activeConnectionsOnNode: target.activeConns
  });
});

// Experiment 3 Cache-Aside
app.get('/api/v1/cached-products/:id', async (req, res) => {
  const result = await cacheAside.getProduct(req.params.id);
  res.json(result);
});

// Experiment 4 Catalog Benchmark
app.get('/api/v1/catalog/benchmark', (req, res) => {
  const category = req.query.category || 'Electronics';
  const minPrice = parseFloat(req.query.minPrice || '10.0');
  const result = benchmarkQuery(category, minPrice);
  res.json(result);
});

// Experiment 5 Auth Login Rate Limiter
app.post('/api/v1/auth/login', async (req, res) => {
  const { username, strategy } = req.body;
  if (!username) return res.status(400).json({ error: 'Username required.' });

  if (strategy === 'token-bucket') {
    if (!tokenBucket.tryConsume()) {
      return res.status(429).json({ error: 'Too Many Requests (Token Bucket capacity exhausted).' });
    }
  } else {
    const allowed = await slidingWindow.tryConsume(username);
    if (!allowed) {
      return res.status(429).json({ error: 'Too Many Requests (Sliding Window Log limit reached).' });
    }
  }

  res.json({ success: true, message: `Login authorized for user ${username}`, strategy: strategy || 'sliding-window' });
});

module.exports = app;
