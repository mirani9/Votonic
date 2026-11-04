const { spawn } = require('child_process');
const sleep = (ms) => new Promise((res) => setTimeout(res, ms));
let hasFailures = false;

function assert(condition, message) {
  if (condition) {
    console.log('[PASS] ' + message);
  } else {
    console.error('[FAIL] ' + message);
    hasFailures = true;
  }
}

async function runTests() {
  console.log('====================================================');
  console.log('🧪 VOTECORE HARDENED REGRESSION & INTEGRITY SUITE');
  console.log('====================================================');

  await fetch('http://127.0.0.1:3000/api/v1/poll/reset', { method: 'POST' });
  await sleep(150);

  // 1. INVALID CANDIDATE REJECTION
  console.log('\n--- 1. Backend Candidate Validation ---');
  const resInvalid = await fetch('http://127.0.0.1:3000/api/v1/poll/vote', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ candidateId: 'Alex Rivera', userId: 'user_invalid_1' })
  });
  assert(resInvalid.status === 400, '1.1 Reject unknown candidate (Alex Rivera) with HTTP 400');

  // 2. CONCURRENT RATE LIMITER BOUNDARY TEST
  console.log('\n--- 2. Atomic Rate Limiter Concurrency Boundary ---');
  const rateLimitReqs = Array.from({ length: 10 }).map((_, i) => 
    fetch('http://127.0.0.1:3000/api/v1/poll/vote', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ candidateId: 'Sophia Chen', userId: `user_rate_${i}` })
    })
  );
  const rateLimitResponses = await Promise.all(rateLimitReqs);
  const allowedCount = rateLimitResponses.filter(r => r.status === 200).length;
  const blockedCount = rateLimitResponses.filter(r => r.status === 429).length;

  assert(allowedCount === 5, `2.1 Exactly 5 boundary requests allowed (Got: ${allowedCount})`);
  assert(blockedCount === 5, `2.2 Exactly 5 boundary requests rate-limited (Got: ${blockedCount})`);

  // Clear rate limits for subsequent test steps
  await fetch('http://127.0.0.1:3000/api/v1/poll/reset', { method: 'POST' });
  await sleep(100);

  // 3. MULTI-PROCESS LOCK CONTENTION & STALE OWNER PROTECTION
  console.log('\n--- 3. Multi-Process Lock & Stale Release Verification ---');
  const proc2 = spawn('node', ['server.js'], { 
    env: { ...process.env, PORT: '3001', DB_FILE: './votecore_test.db', NODE_ENV: 'test' } 
  });
  await sleep(1500);

  // Simultaneous lock requests across Process 1 (Port 3000) and Process 2 (Port 3001)
  const p1Close = fetch('http://127.0.0.1:3000/api/v1/poll/close', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ ttlMs: 1000 })
  });
  const p2Close = fetch('http://127.0.0.1:3001/api/v1/poll/close', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ ttlMs: 1000 })
  });

  const [resProc1, resProc2] = await Promise.all([p1Close, p2Close]);
  const bodyProc1 = await resProc1.json();
  const bodyProc2 = await resProc2.json();

  const proc1Won = resProc1.status === 200 && resProc2.status === 409;
  const proc2Won = resProc1.status === 409 && resProc2.status === 200;
  assert(proc1Won || proc2Won, '3.1 Distributed Mutex: Exactly 1 process acquired lock (200), 1 rejected (409)');

  const winnerPort = proc1Won ? 3000 : 3001;
  const winnerToken = proc1Won ? bodyProc1.ownerToken : bodyProc2.ownerToken;

  // Wait for lock TTL expiration
  await sleep(1200);

  // Process 2 acquires NEW lock after expiration
  const p2Reacquire = await fetch('http://127.0.0.1:3001/api/v1/poll/close', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ ttlMs: 3000 })
  });
  assert(p2Reacquire.status === 200, '3.2 New owner re-acquired lock after expiration');

  // Old owner token attempts release
  const staleRelease = await fetch(`http://127.0.0.1:${winnerPort}/api/v1/poll/release-lock`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ ownerToken: winnerToken })
  });
  const staleReleaseBody = await staleRelease.json();
  assert(staleReleaseBody.success === false, '3.3 Stale owner token cannot release new owner lock');

  // Shared Poll Closed Verification across both processes
  const voteOnProc2 = await fetch('http://127.0.0.1:3001/api/v1/poll/vote', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ candidateId: 'Sophia Chen', userId: 'user_closed_test' })
  });
  assert(voteOnProc2.status === 403, '3.4 Shared persistent poll closure recognized across processes (HTTP 403)');

  proc2.kill();

  // Reset poll status for worker recovery step
  await fetch('http://127.0.0.1:3000/api/v1/poll/reset', { method: 'POST' });
  await sleep(100);

  // 4. WORKER RECOVERY, QUEUE BUFFERING & PERSISTENCE EVIDENCE
  console.log('\n--- 4. Worker Fault Recovery & Publisher Confirms ---');
  // Detach consumer
  await fetch('http://127.0.0.1:3000/api/v1/worker/toggle', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ active: false })
  });

  const userIdRecovery = 'user_recovery_' + Date.now();
  const vRes = await fetch('http://127.0.0.1:3000/api/v1/poll/vote', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ candidateId: 'Marcus Vance', userId: userIdRecovery })
  });
  const vData = await vRes.json();
  assert(vRes.status === 200 && vData.voteId, '4.1 Ingestion accepted with Publisher Confirm while consumer offline');

  const mQueue = await (await fetch('http://127.0.0.1:3000/api/v1/metrics')).json();
  assert(mQueue.lifecycle.queued === 1 && mQueue.lifecycle.pending === 1, '4.2 Message buffered in AMQP queue (queued=1, pending=1)');

  // Re-attach consumer
  await fetch('http://127.0.0.1:3000/api/v1/worker/toggle', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ active: true })
  });

  let verifiedDb = false;
  let record = null;
  for (let i = 0; i < 15; i++) {
    await sleep(200);
    const verify = await (await fetch('http://127.0.0.1:3000/api/v1/db-verify/' + vData.voteId)).json();
    if (verify.exists) {
      verifiedDb = true;
      record = verify.record;
      break;
    }
  }

  assert(verifiedDb, '4.3 Worker recovered and persisted exact vote ID to SQLite');
  console.log('Database Evidence Record:', JSON.stringify(record));

  console.log('====================================================');
  if (hasFailures) {
    console.error('❌ HARDENED SUITE FAILED');
    process.exit(1);
  } else {
    console.log('🎉 ALL HARDENED SYSTEM & REGRESSION CHECKS PASSED!');
  }
  console.log('====================================================');
}

runTests();