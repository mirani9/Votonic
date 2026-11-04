const assert = require('assert');

const BASE_URL = 'http://127.0.0.1:3000';

async function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function runTestSuite() {
  console.log('====================================================');
  console.log('🧪 VOTECORE INTEGRITY TEST SUITE');
  console.log('====================================================');

  try {
    // 0. Reset State
    await fetch(`${BASE_URL}/api/v1/poll/reset`, { method: 'POST' });

    // Test 1: Redis Service Health
    const mRes1 = await fetch(`${BASE_URL}/api/v1/metrics`);
    const mData1 = await mRes1.json();
    assert.strictEqual(mData1.services.redis, true, 'Redis service must be connected');
    console.log('[PASS] 1. Redis Service Health');

    // Test 2: RabbitMQ Service Health
    assert.strictEqual(mData1.services.rabbitmq, true, 'RabbitMQ service must be connected');
    console.log('[PASS] 2. RabbitMQ Service Health');

    // Test 3: Database Service Health
    assert.strictEqual(mData1.services.database, true, 'SQLite service must be initialized');
    console.log('[PASS] 3. Database Service Health');

    // Test 4: Worker Consumer Attached
    assert.strictEqual(mData1.queue.activeConsumers > 0, true, 'Worker consumer must be active');
    console.log('[PASS] 4. AMQP Worker Consumer Attached');

    // Test 5: Ingest Vote
    const voteRes = await fetch(`${BASE_URL}/api/v1/poll/vote`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        candidateId: 'Sophia Chen',
        userId: 'user_test_99'
      })
    });
    const voteData = await voteRes.json();
    assert.strictEqual(voteRes.status, 200, 'Vote ingestion must return HTTP 200');
    assert.ok(voteData.voteId, 'Response must include voteId');
    console.log('[PASS] 5. Vote Ingestion & ID Generation (HTTP 200)');

    // Test 6: Duplicate Voter Rejection
    const dupRes = await fetch(`${BASE_URL}/api/v1/poll/vote`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        candidateId: 'Sophia Chen',
        userId: 'user_test_99'
      })
    });
    assert.strictEqual(dupRes.status, 409, 'Duplicate vote must return HTTP 409');
    console.log('[PASS] 6. Duplicate Voter Rejection (HTTP 409)');

    // Wait for worker processing
    await sleep(300);

    // Test 7: SQLite Persistence Verification
    const dbRes = await fetch(`${BASE_URL}/api/v1/db-verify/${voteData.voteId}`);
    const dbData = await dbRes.json();
    assert.strictEqual(dbRes.status, 200, 'DB verification endpoint must respond HTTP 200');
    assert.strictEqual(dbData.exists, true, 'Vote must be physically written to SQLite');
    assert.strictEqual(dbData.record.vote_id, voteData.voteId, 'SQLite vote_id must match ingested voteId');
    console.log(`[PASS] 7. Exact Vote Persistence Verification in SQLite (vote_id: ${voteData.voteId})`);

    // Test 8: Dashboard Metric Reconciliation
    const mRes2 = await fetch(`${BASE_URL}/api/v1/metrics`);
    const mData2 = await mRes2.json();
    assert.strictEqual(mData2.lifecycle.persisted, 1, 'Metrics must show exactly 1 persisted vote');
    console.log('[PASS] 8. Dashboard Persisted Vote Metric Reconciled (Count = 1)');

    // Test 9: Mutex Lock Contention
    const [res1, res2] = await Promise.all([
      fetch(`${BASE_URL}/api/v1/poll/close`, { method: 'POST' }),
      fetch(`${BASE_URL}/api/v1/poll/close`, { method: 'POST' })
    ]);

    const statuses = [res1.status, res2.status].sort();
    assert.strictEqual(statuses[0], 200, 'One request must succeed (HTTP 200)');
    assert.strictEqual(statuses[1], 409, 'Concurrent request must be rejected (HTTP 409)');
    console.log('[PASS] 9. Single-Instance Mutex Lock Contention (1 Granted 200 / 1 Denied 409)');

    console.log('====================================================');
    console.log('🎉 INTEGRITY TEST SUITE PASSED 100%');
    console.log('====================================================');
  } catch (err) {
    console.error(`\n❌ TEST ASSERTION FAILED: ${err.message}`);
    console.log('====================================================');
    process.exit(1);
  }
}

runTestSuite();