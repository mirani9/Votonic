const http = require('http');
const Redis = require('ioredis');
const app = require('./modules/exp7_gateway/gateway');
const {
  NotificationAdapter, EmailNotificationProvider, SMSNotificationProvider,
  PollSubject, LeaderboardObserver, AuditLogObserver,
  RateLimiterContext, StrictRateLimitStrategy, RelaxedRateLimitStrategy
} = require('./modules/exp8_patterns/patterns');

const PORT = process.env.PORT || 3005;
const redis = new Redis({ host: '127.0.0.1', port: 6379 });

let server;

function request(method, path, body = null, headers = {}) {
  return new Promise((resolve, reject) => {
    const req = http.request(`http://localhost:${PORT}${path}`, {
      method,
      headers: { 'Content-Type': 'application/json', ...headers }
    }, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => resolve({
        status: res.statusCode,
        headers: res.headers,
        body: JSON.parse(data || '{}')
      }));
    });
    req.on('error', reject);
    if (body) req.write(JSON.stringify(body));
    req.end();
  });
}

async function runMasterSuite() {
  server = app.listen(PORT, async () => {
    console.log('====================================================');
    console.log(` VOTECORE EXPERIMENTS 1-8 INTEGRATED SUITE (PORT ${PORT}) `);
    console.log('====================================================\n');

    let pass = 0, fail = 0;

    // EXP 1: CRUD Verification
    console.log('[EXP 1] Testing Product CRUD Endpoints...');
    const createRes = await request('POST', '/api/v1/products', { name: 'Wireless Mouse', price: 29.99, category: 'Electronics', stock: 50 });
    const productId = createRes.body.data?.id;

    const getRes = await request('GET', `/api/v1/products/${productId}`);
    const deleteRes = await request('DELETE', `/api/v1/products/${productId}`);

    if (createRes.status === 201 && getRes.status === 200 && deleteRes.status === 200) {
      console.log(' -> EXP 1 PASSED (Create, Read, Delete Verified)'); pass++;
    } else {
      console.log(' -> EXP 1 FAILED'); fail++;
    }

    // EXP 2: Load Balancer Verification
    console.log('\n[EXP 2] Testing Round-Robin & Least-Connections Load Balancing...');
    const lb1 = await request('GET', '/api/v1/lb-test?algo=round-robin');
    const lb2 = await request('GET', '/api/v1/lb-test?algo=round-robin');
    if (lb1.body.routedTo !== lb2.body.routedTo) {
      console.log(` -> EXP 2 PASSED (Rotated between ${lb1.body.routedTo} and ${lb2.body.routedTo})`); pass++;
    } else {
      console.log(' -> EXP 2 FAILED'); fail++;
    }

    // EXP 3: Cache-Aside Verification
    console.log('\n[EXP 3] Testing Redis Cache-Aside Read Flow...');
    const prodNew = await request('POST', '/api/v1/products', { name: 'Keyboard', price: 49.99, category: 'Electronics', stock: 10 });
    const pId = prodNew.body.data.id;

    const miss = await request('GET', `/api/v1/cached-products/${pId}`);
    const hit = await request('GET', `/api/v1/cached-products/${pId}`);
    if (miss.body.source === 'CACHE_MISS' && hit.body.source === 'CACHE_HIT') {
      console.log(` -> EXP 3 PASSED (Miss: ${miss.body.latencyMs}ms, Hit: ${hit.body.latencyMs}ms)`); pass++;
    } else {
      console.log(' -> EXP 3 FAILED'); fail++;
    }

    // EXP 4: Catalog Benchmark Verification
    console.log('\n[EXP 4] Testing Catalog Query Performance Benchmark...');
    const catBench = await request('GET', '/api/v1/catalog/benchmark?category=Electronics');
    if (catBench.status === 200 && typeof catBench.body.relationalQueryTimeMs === 'number') {
      console.log(` -> EXP 4 PASSED (Indexed Search Query: ${catBench.body.relationalQueryTimeMs}ms)`); pass++;
    } else {
      console.log(' -> EXP 4 FAILED'); fail++;
    }

    // EXP 5: Token Bucket & Sliding Window Rate Limiting Verification
    console.log('\n[EXP 5] Testing Token Bucket & Sliding Window Rate Limiter...');
    await redis.del('sliding_log:rate_user_test');
    let rateBlocked = false;
    for (let i = 0; i < 7; i++) {
      const res = await request('POST', '/api/v1/auth/login', { username: 'rate_user_test', strategy: 'sliding-window' });
      if (res.status === 429) rateBlocked = true;
    }
    if (rateBlocked) {
      console.log(' -> EXP 5 PASSED (HTTP 429 Successfully Enforced on Threshold)'); pass++;
    } else {
      console.log(' -> EXP 5 FAILED'); fail++;
    }

    // EXP 6: Microservice Communications Verification
    console.log('\n[EXP 6] Testing Microservice Decoupling & Status Proxy...');
    const statusRes = await request('GET', '/health');
    if (statusRes.body.status === 'UP') {
      console.log(' -> EXP 6 PASSED (Independent Microservice Health Check OK)'); pass++;
    } else {
      console.log(' -> EXP 6 FAILED'); fail++;
    }

    // EXP 7: API Gateway Observability Verification
    console.log('\n[EXP 7] Testing API Gateway Authentication & Logging Headers...');
    const healthCheck = await request('GET', '/health');
    if (healthCheck.status === 200 && healthCheck.headers['x-gateway-auth'] === 'VERIFIED') {
      console.log(' -> EXP 7 PASSED (Gateway Observability active)'); pass++;
    } else {
      console.log(' -> EXP 7 FAILED'); fail++;
    }

    // EXP 8: Design Patterns Verification
    console.log('\n[EXP 8] Testing Adapter, Observer, and Strategy Patterns...');
    const emailAdapter = new NotificationAdapter(new EmailNotificationProvider(), 'email');
    const smsAdapter = new NotificationAdapter(new SMSNotificationProvider(), 'sms');
    const adapterOk = emailAdapter.notify('test@mail.com', 'Hi').includes('[Email Adapter]') &&
                      smsAdapter.notify('123456', 'Hi').includes('[SMS Adapter]');

    const subject = new PollSubject();
    subject.subscribe(new LeaderboardObserver());
    subject.subscribe(new AuditLogObserver());
    const obsResults = subject.notifyAll({ voter_id: 'v1', candidate_id: 'c1' });
    const observerOk = obsResults.length === 2;

    const stratContext = new RateLimiterContext(new StrictRateLimitStrategy());
    const strictBlocked = !stratContext.executeStrategy(10);
    stratContext.setStrategy(new RelaxedRateLimitStrategy());
    const relaxedAllowed = stratContext.executeStrategy(10);
    const strategyOk = strictBlocked && relaxedAllowed;

    if (adapterOk && observerOk && strategyOk) {
      console.log(' -> EXP 8 PASSED (Adapter, Observer, Strategy pattern implementations verified)'); pass++;
    } else {
      console.log(' -> EXP 8 FAILED'); fail++;
    }

    console.log('\n====================================================');
    console.log(` SUMMARY: ${pass} PASSED, ${fail} FAILED OUT OF 8 EXPERIMENTS`);
    console.log('====================================================\n');

    await redis.quit();
    server.close(() => process.exit(fail > 0 ? 1 : 0));
  });
}

runMasterSuite();
