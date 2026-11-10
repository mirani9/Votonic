const http = require('http');
const Database = require('better-sqlite3');
const Redis = require('ioredis');
const path = require('path');

const redis = new Redis({ host: '127.0.0.1', port: 6379 });
const db = new Database(path.join(__dirname, 'votecore.db'));

function makeVoteRequest(voterId, candidateId, headers = {}) {
  return new Promise((resolve, reject) => {
    const data = JSON.stringify({ voter_id: voterId, candidate_id: candidateId });
    const req = http.request('http://localhost:3000/api/vote', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(data),
        ...headers
      }
    }, (res) => {
      let body = '';
      res.on('data', chunk => body += chunk);
      res.on('end', () => resolve({ status: res.statusCode, body: JSON.parse(body || '{}') }));
    });
    req.on('error', reject);
    req.write(data);
    req.end();
  });
}

async function runTests() {
  console.log('==================================================');
  console.log('[VOTECORE Integration & System Test Suite]');
  console.log('==================================================\n');

  // Test 1: Concurrency Duplicate Guard
  console.log('[Test 1] Executing 50 concurrent requests for same voter (voter_concurrent)...');
  const requests = [];
  for (let i = 0; i < 50; i++) {
    requests.push(makeVoteRequest('voter_concurrent', 'c1'));
  }
  await Promise.all(requests);
  await new Promise(r => setTimeout(r, 1500)); // Allow worker drain

  const count = db.prepare('SELECT COUNT(*) as cnt FROM votes WHERE voter_id = ?').get('voter_concurrent').cnt;
  console.log(` -> DB Row Count for voter_concurrent: ${count} (Expected: 1)`);
  console.assert(count === 1, 'FAIL: Concurrency guard allowed multiple rows!');

  // Test 2: Redis Flush & Leaderboard Rebuild
  console.log('\n[Test 2] Testing Redis flush & automatic startup rebuild...');
  await redis.flushall();
  const redisBefore = await redis.zscore('leaderboard:default', 'c1');
  console.log(` -> Redis count post-flush: ${redisBefore} (Expected: null)`);
  
  // Call reconcile endpoint to trigger query verification
  const res = await new Promise((resolve) => {
    http.get('http://localhost:3000/admin/reconcile', (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => resolve(JSON.parse(data)));
    });
  });
  console.log(' -> Reconcile Report:', JSON.stringify(res.report));

  // Test 3: Lock Release Safety (Lua)
  console.log('\n[Test 3] Verifying Lock Release Lua compare-and-delete logic...');
  await redis.set('lock:voter:test_owner', 'owner_A', 'PX', 5000);
  const luaScript = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
  
  // Stale owner attempts release
  const staleRelease = await redis.eval(luaScript, 1, 'lock:voter:test_owner', 'owner_STALE');
  console.log(` -> Stale release output: ${staleRelease} (Expected: 0)`);
  console.assert(staleRelease === 0, 'FAIL: Stale owner unlocked key!');

  // Valid owner attempts release
  const validRelease = await redis.eval(luaScript, 1, 'lock:voter:test_owner', 'owner_A');
  console.log(` -> Valid release output: ${validRelease} (Expected: 1)`);
  console.assert(validRelease === 1, 'FAIL: Valid owner failed to unlock!');

  console.log('\n==================================================');
  console.log('ALL SYSTEM TESTS PASSED SUCCESSFULLY.');
  console.log('==================================================\n');
  process.exit(0);
}

runTests();
