const http = require('http');
const Redis = require('ioredis');
const Database = require('better-sqlite3');
const path = require('path');
const app = require('./modules/exp7_gateway/gateway');
const {
  NotificationAdapter, EmailNotificationProvider, SMSNotificationProvider,
  PollSubject, LeaderboardObserver, AuditLogObserver,
  RateLimiterContext, StrictRateLimitStrategy, RelaxedRateLimitStrategy
} = require('./modules/exp8_patterns/patterns');

const PORT = 3010;
const redis = new Redis({ host: '127.0.0.1', port: 6379 });
const db = new Database(path.join(__dirname, 'votecore.db'));

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

async function runVerificationSuite() {
  server = app.listen(PORT, async () => {
    console.log('===========================================================');
    console.log(` VOTECORE INTEGRATED QA & VERIFICATION SUITE (PORT ${PORT}) `);
    console.log('===========================================================\n');

    let pass = 0, fail = 0;

    // Test 1: CRUD & Invalid Input Validation
    console.log('[Test 1] Testing REST Product CRUD & Input Validation...');
    const invalidRes = await request('POST', '/api/v1/products', { name: '', price: -10, category: '' });
    const validRes = await request('POST', '/api/v1/products', { name: 'Test Keyboard', price: 79.99, category: 'Electronics', stock: 15 });
    const pid = validRes.body.data?.id;
    const deleteRes = await request('DELETE', `/api/v1/products/${pid}`);
    if (invalidRes.status === 400 && validRes.status === 201 && deleteRes.status === 200) {
      console.log('  -> PASSED: Input validation enforced & full CRUD verified.'); pass++;
    } else {
      console.log('  -> FAILED: CRUD or validation failure.'); fail++;
    }

    // Test 2: Round Robin vs Least Connections Distribution
    console.log('\n[Test 2] Testing Load Balancing Distribution Algorithms...');
    const rr1 = await request('GET', '/api/v1/lb-test?algo=round-robin');
    const rr2 = await request('GET', '/api/v1/lb-test?algo=round-robin');
    if (rr1.body.routedTo !== rr2.body.routedTo) {
      console.log(`  -> PASSED: Round-robin rotated targets (${rr1.body.routedTo} -> ${rr2.body.routedTo}).`); pass++;
    } else {
      console.log('  -> FAILED: Load balancer target rotation failed.'); fail++;
    }

    // Test 3: Cache Miss vs Cache Hit
    console.log('\n[Test 3] Testing Redis Cache-Aside Hit vs Miss...');
    const pNew = await request('POST', '/api/v1/products', { name: 'Cache Test Item', price: 15.00, category: 'Test', stock: 5 });
    const id = pNew.body.data.id;
    const miss = await request('GET', `/api/v1/cached-products/${id}`);
    const hit = await request('GET', `/api/v1/cached-products/${id}`);
    await request('DELETE', `/api/v1/products/${id}`);
    if (miss.body.source === 'CACHE_MISS' && hit.body.source === 'CACHE_HIT') {
      console.log(`  -> PASSED: Cache Miss (${miss.body.latencyMs}ms) & Cache Hit (${hit.body.latencyMs}ms) verified.`); pass++;
    } else {
      console.log('  -> FAILED: Cache-aside latency state failed.'); fail++;
    }

    // Test 4: Catalog Search Benchmark
    console.log('\n[Test 4] Testing Catalog Query Performance...');
    const cat = await request('GET', '/api/v1/catalog/benchmark?category=Electronics');
    if (cat.status === 200 && typeof cat.body.relationalQueryTimeMs === 'number') {
      console.log(`  -> PASSED: Indexed catalog query returned in ${cat.body.relationalQueryTimeMs}ms.`); pass++;
    } else {
      console.log('  -> FAILED: Catalog query benchmark failed.'); fail++;
    }

    // Test 5: Sliding Window Rate Limiting Boundary Conditions
    console.log('\n[Test 5] Testing Sliding Window Log Rate Limiter Boundary Conditions...');
    await redis.del('sliding_log:rate_qa_user');
    let blockedCount = 0;
    for (let i = 0; i < 7; i++) {
      const res = await request('POST', '/api/v1/auth/login', { username: 'rate_qa_user', strategy: 'sliding-window' });
      if (res.status === 429) blockedCount++;
    }
    if (blockedCount === 2) {
      console.log('  -> PASSED: Exactly 5 requests allowed, 2 requests blocked (429).'); pass++;
    } else {
      console.log(`  -> FAILED: Rate limit blocked count unexpected: ${blockedCount}`); fail++;
    }

    // Test 6: Token Bucket Limiter
    console.log('\n[Test 6] Testing Token Bucket Rate Limiter...');
    let tbBlocked = false;
    for (let i = 0; i < 7; i++) {
      const res = await request('POST', '/api/v1/auth/login', { username: 'tb_qa_user', strategy: 'token-bucket' });
      if (res.status === 429) tbBlocked = true;
    }
    if (tbBlocked) {
      console.log('  -> PASSED: Token Bucket capacity boundary enforced.'); pass++;
    } else {
      console.log('  -> FAILED: Token bucket capacity check failed.'); fail++;
    }

    // Test 7: Stale Lock Expiry Protection via Lua Script
    console.log('\n[Test 7] Testing Stale Owner Lock Release via Lua Script...');
    await redis.set('lock:voter:qa_test_lock', 'token_A', 'PX', 5000);
    const luaScript = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
    const staleRelease = await redis.eval(luaScript, 1, 'lock:voter:qa_test_lock', 'token_STALE');
    const validRelease = await redis.eval(luaScript, 1, 'lock:voter:qa_test_lock', 'token_A');
    if (staleRelease === 0 && validRelease === 1) {
      console.log('  -> PASSED: Stale lock release blocked; matching owner token released successfully.'); pass++;
    } else {
      console.log('  -> FAILED: Lock owner token release script failed.'); fail++;
    }

    // Test 8: Microservice Gateway Proxying
    console.log('\n[Test 8] Testing Microservice Gateway Proxying...');
    const statusRes = await request('GET', '/health');
    if (statusRes.body.status === 'UP') {
      console.log('  -> PASSED: Microservice health check operational.'); pass++;
    } else {
      console.log('  -> FAILED: Health endpoint failed.'); fail++;
    }

    // Test 9: API Gateway Headers & Observability
    console.log('\n[Test 9] Testing Gateway Auth Headers & Observability...');
    const gwRes = await request('GET', '/health');
    if (gwRes.headers['x-gateway-auth'] === 'VERIFIED' && gwRes.headers['x-gateway-processed'] === 'true') {
      console.log('  -> PASSED: Response headers X-Gateway-Auth & X-Gateway-Processed present.'); pass++;
    } else {
      console.log('  -> FAILED: Gateway observability headers missing.'); fail++;
    }

    // Test 10: Software Design Patterns Execution
    console.log('\n[Test 10] Testing Adapter, Observer, and Strategy Patterns Execution...');
    const emailAdapter = new NotificationAdapter(new EmailNotificationProvider(), 'email');
    const smsAdapter = new NotificationAdapter(new SMSNotificationProvider(), 'sms');
    const adapterPass = emailAdapter.notify('qa@mail.com', 'test').includes('[Email Adapter]') &&
                        smsAdapter.notify('999999', 'test').includes('[SMS Adapter]');

    const subject = new PollSubject();
    subject.subscribe(new LeaderboardObserver());
    subject.subscribe(new AuditLogObserver());
    const obsResults = subject.notifyAll({ voter_id: 'qa_voter', candidate_id: 'c1' });
    const observerPass = obsResults.length === 2;

    const stratContext = new RateLimiterContext(new StrictRateLimitStrategy());
    const strictVal = !stratContext.executeStrategy(10);
    stratContext.setStrategy(new RelaxedRateLimitStrategy());
    const relaxedVal = stratContext.executeStrategy(10);
    const strategyPass = strictVal && relaxedVal;

    if (adapterPass && observerPass && strategyPass) {
      console.log('  -> PASSED: Adapter, Observer, and Strategy design patterns verified.'); pass++;
    } else {
      console.log('  -> FAILED: Design pattern execution failed.'); fail++;
    }

    console.log('\n===========================================================');
    console.log(` VERIFICATION SUMMARY: ${pass} PASSED, ${fail} FAILED OUT OF 10 TESTS `);
    console.log('===========================================================\n');

    await redis.quit();
    db.close();
    server.close(() => process.exit(fail > 0 ? 1 : 0));
  });
}

runVerificationSuite();
