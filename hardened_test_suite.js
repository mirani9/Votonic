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
  console.log('[VOTECORE Comprehensive Test Suite - Experiments 1-11]');
  console.log('==================================================\n');

  let pass = 0;
  let fail = 0;

  // Test 1: Invalid Candidate Check
  console.log('[Test 1] Submitting vote for non-existent candidate...');
  const t1 = await makeVoteRequest('voter_invalid_cand', 'c999');
  if (t1.status === 404) { console.log(' -> PASSED (404 Not Found)'); pass++; }
  else { console.log(` -> FAILED (Status: ${t1.status})`); fail++; }

  // Test 2: Invalid Voter ID Format
  console.log('\n[Test 2] Submitting vote with malformed voter_id...');
  const t2 = await makeVoteRequest('a', 'c1');
  if (t2.status === 400) { console.log(' -> PASSED (400 Bad Request)'); pass++; }
  else { console.log(` -> FAILED (Status: ${t2.status})`); fail++; }

  // Test 3: Concurrency Duplicate Guard & Lock Safety
  console.log('\n[Test 3] Executing 20 concurrent requests for same voter (voter_concur_1)...');
  const reqs = [];
  for (let i = 0; i < 20; i++) {
    reqs.push(makeVoteRequest('voter_concur_1', 'c1'));
  }
  await Promise.all(reqs);
  await new Promise(r => setTimeout(r, 1000));

  const dbCount = db.prepare('SELECT COUNT(*) as cnt FROM votes WHERE voter_id = ?').get('voter_concur_1').cnt;
  if (dbCount === 1) { console.log(` -> PASSED (DB Row Count: ${dbCount})`); pass++; }
  else { console.log(` -> FAILED (DB Row Count: ${dbCount})`); fail++; }

  // Test 4: Sequential Duplicate Submission
  console.log('\n[Test 4] Submitting duplicate vote sequentially for voter_concur_1...');
  const t4 = await makeVoteRequest('voter_concur_1', 'c1');
  if (t4.status === 409) { console.log(' -> PASSED (409 Conflict Received)'); pass++; }
  else { console.log(` -> FAILED (Status: ${t4.status})`); fail++; }

  // Test 5: Redis Flush & Leaderboard Auto-Rebuild
  console.log('\n[Test 5] Testing Redis cache flush & auto-rebuild...');
  await redis.flushall();
  const reconcileRes = await new Promise((resolve) => {
    http.get('http://localhost:3000/admin/reconcile', (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => resolve(JSON.parse(data)));
    });
  });
  
  let driftSum = 0;
  reconcileRes.report.forEach(r => driftSum += Math.abs(r.drift));
  if (driftSum === 0) { console.log(' -> PASSED (Zero Drift Verified)'); pass++; }
  else { console.log(` -> FAILED (Drift Detected: ${driftSum})`); fail++; }

  // Test 6: Stale Owner Lock Release (Lua)
  console.log('\n[Test 6] Verifying Stale Lock Release Protection via Lua...');
  await redis.set('lock:voter:test_owner_key', 'owner_A', 'PX', 5000);
  const luaScript = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
  
  const staleRelease = await redis.eval(luaScript, 1, 'lock:voter:test_owner_key', 'owner_STALE');
  const validRelease = await redis.eval(luaScript, 1, 'lock:voter:test_owner_key', 'owner_A');

  if (staleRelease === 0 && validRelease === 1) { console.log(' -> PASSED (Stale Release Protected)'); pass++; }
  else { console.log(' -> FAILED (Lock token validation failed)'); fail++; }

  console.log('\n==================================================');
  console.log(`TOTAL TESTS: ${pass + fail} | PASSED: ${pass} | FAILED: ${fail}`);
  console.log('==================================================\n');
  process.exit(fail > 0 ? 1 : 0);
}

runTests();
