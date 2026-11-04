const fs = require('fs');

const serverCode = `const express = require('express');
const Redis = require('ioredis');
const amqp = require('amqplib');
const sqlite3 = require('sqlite3').verbose();
const path = require('path');
const { randomUUID: uuidv4 } = require('crypto');

const app = express();
app.use(express.json());

const db = new sqlite3.Database('./votecore.db');
db.serialize(() => {
  db.run("PRAGMA journal_mode = WAL;");
  db.run("CREATE TABLE IF NOT EXISTS votes (id INTEGER PRIMARY KEY AUTOINCREMENT, vote_id TEXT UNIQUE, poll TEXT, candidate_id TEXT, user_id TEXT, ip_address TEXT, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP, UNIQUE(poll, user_id))");
});

const redis = new Redis({ host: '127.0.0.1', port: 6380, retryStrategy: (t) => Math.min(t * 100, 3000) });
let connection = null;
let channel = null;

let metrics = {
  received: 0,
  rejected: 0,
  accepted: 0,
  rateLimited: 0,
  duplicates: 0,
  failed: 0,
  activePoll: 'Campus Tech Leader 2026',
  isClosed: false
};

async function initRabbitMQ() {
  try {
    connection = await amqp.connect('amqp://localhost:5673');
    channel = await connection.createConfirmChannel();
    await channel.assertQueue('vote_queue', { durable: true });
    console.log('[AMQP] Consumer active on vote_queue.');

    channel.consume('vote_queue', async (msg) => {
      if (!msg) return;
      try {
        const vote = JSON.parse(msg.content.toString());
        db.run(
          "INSERT INTO votes (vote_id, poll, candidate_id, user_id, ip_address, timestamp) VALUES (?, ?, ?, ?, ?, ?)",
          [vote.voteId, vote.poll, vote.candidateId, vote.userId, vote.ip, vote.timestamp],
          function (err) {
            if (err) {
              if (err.message.includes('UNIQUE constraint failed')) {
                channel.ack(msg);
              } else {
                metrics.failed++;
                channel.nack(msg, false, false);
              }
            } else {
              console.log('[EVENT: VOTE_PERSISTED] Vote ID:', vote.voteId);
              channel.ack(msg);
            }
          }
        );
      } catch (e) {
        metrics.failed++;
        channel.nack(msg, false, false);
      }
    }, { noAck: false });
  } catch (e) {
    setTimeout(initRabbitMQ, 2000);
  }
}
initRabbitMQ();

async function slidingWindowRateLimiter(ip) {
  const windowMs = 10000;
  const maxLimit = 5;
  const now = Date.now();
  const rateKey = 'ratelimit:' + ip;
  const pipeline = redis.pipeline();
  pipeline.zremrangebyscore(rateKey, 0, now - windowMs);
  pipeline.zcard(rateKey);
  pipeline.zadd(rateKey, now, now + ':' + Math.random());
  pipeline.expire(rateKey, 11);
  const results = await pipeline.exec();
  return { allowed: results[1][1] < maxLimit };
}

app.post('/api/v1/poll/vote', async (req, res) => {
  metrics.received++;
  if (metrics.isClosed) {
    metrics.rejected++;
    return res.status(403).json({ error: 'Poll is closed' });
  }

  const clientIp = req.ip || '127.0.0.1';
  const { candidateId, userId } = req.body;
  if (!candidateId || !userId) {
    metrics.rejected++;
    return res.status(400).json({ error: 'Missing payload' });
  }

  const rateCheck = await slidingWindowRateLimiter(clientIp);
  if (!rateCheck.allowed) {
    metrics.rateLimited++;
    metrics.rejected++;
    return res.status(429).json({ error: 'Rate limit exceeded' });
  }

  const voteId = uuidv4();
  const voterSetKey = 'voter_set:' + metrics.activePoll;

  try {
    const added = await redis.sadd(voterSetKey, userId);
    if (added === 0) {
      metrics.duplicates++;
      metrics.rejected++;
      return res.status(409).json({ error: 'User already voted' });
    }

    await redis.zincrby('poll:leaderboard', 1, candidateId);
    const payload = { voteId, poll: metrics.activePoll, candidateId, userId, ip: clientIp, timestamp: new Date().toISOString() };

    if (!channel) throw new Error('Channel uninitialized');
    channel.sendToQueue('vote_queue', Buffer.from(JSON.stringify(payload)), { persistent: true });
    metrics.accepted++;
    return res.status(200).json({ success: true, voteId });
  } catch (err) {
    metrics.failed++;
    metrics.rejected++;
    return res.status(500).json({ error: err.message });
  }
});

app.post('/api/v1/poll/close', async (req, res) => {
  const lockKey = 'lock:poll_close:' + metrics.activePoll;
  const ownerToken = uuidv4();
  const acquired = await redis.set(lockKey, ownerToken, 'NX', 'PX', 3000);

  if (!acquired) {
    return res.status(409).json({ success: false, error: 'Lock contention' });
  }

  try {
    metrics.isClosed = true;
    return res.json({ success: true, ownerToken });
  } finally {
    const releaseLua = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
    await redis.eval(releaseLua, 1, lockKey, ownerToken);
  }
});

app.get('/api/v1/metrics', async (req, res) => {
  db.get('SELECT COUNT(*) as count FROM votes', async (err, row) => {
    const persisted = err ? 0 : row.count;
    let queued = 0, consumers = 0;
    if (channel) {
      try {
        const q = await channel.checkQueue('vote_queue');
        queued = q.messageCount;
        consumers = q.consumerCount;
      } catch (e) {}
    }
    const pending = Math.max(0, metrics.accepted - persisted - metrics.failed);
    res.json({
      lifecycle: {
        received: metrics.received,
        rejected: metrics.rejected,
        accepted: metrics.accepted,
        rateLimited: metrics.rateLimited,
        duplicates: metrics.duplicates,
        queued,
        persisted,
        pending,
        failed: metrics.failed
      },
      queue: { activeConsumers: consumers },
      services: {
        redis: redis.status === 'ready',
        rabbitmq: channel !== null,
        database: db !== null
      }
    });
  });
});

app.get('/api/v1/db-verify/:voteId', (req, res) => {
  db.get('SELECT * FROM votes WHERE vote_id = ?', [req.params.voteId], (err, row) => {
    if (err) return res.status(500).json({ error: err.message });
    return res.json({ exists: !!row, record: row || null });
  });
});

app.post('/api/v1/poll/reset', async (req, res) => {
  await redis.del('poll:leaderboard');
  await redis.del('voter_set:' + metrics.activePoll);
  const keys = await redis.keys('ratelimit:*');
  if (keys.length > 0) await redis.del(keys);

  metrics = {
    received: 0,
    rejected: 0,
    accepted: 0,
    rateLimited: 0,
    duplicates: 0,
    failed: 0,
    activePoll: 'Campus Tech Leader 2026',
    isClosed: false
  };
  db.run('DELETE FROM votes');
  res.json({ success: true });
});

app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'index.html')));
app.listen(3000, () => console.log('? VOTECORE Engine running on http://localhost:3000'));
`;

const testCode = `const sleep = (ms) => new Promise((res) => setTimeout(res, ms));
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
  console.log('?? VOTECORE INTEGRITY TEST SUITE');
  console.log('====================================================');
  await fetch('http://127.0.0.1:3000/api/v1/poll/reset', { method: 'POST' });

  const m1 = await (await fetch('http://127.0.0.1:3000/api/v1/metrics')).json();
  assert(m1.services.redis, '1. Redis Service Health');
  assert(m1.services.rabbitmq, '2. RabbitMQ Service Health');
  assert(m1.services.database, '3. Database Service Health');
  assert(m1.queue.activeConsumers > 0, '4. AMQP Worker Consumer Attached');

  const userId = 'user_trace_' + Date.now();
  const r1 = await fetch('http://127.0.0.1:3000/api/v1/poll/vote', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ candidateId: 'Sophia Chen', userId })
  });
  const d1 = await r1.json();
  assert(r1.status === 200 && d1.voteId, '5. Vote Ingestion & ID Generation (HTTP 200)');
  const voteId = d1.voteId;

  const r2 = await fetch('http://127.0.0.1:3000/api/v1/poll/vote', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ candidateId: 'Sophia Chen', userId })
  });
  assert(r2.status === 409, '6. Duplicate Voter Rejection (HTTP 409)');

  let persisted = false;
  for (let i = 0; i < 15; i++) {
    await sleep(300);
    const verifyRes = await (await fetch('http://127.0.0.1:3000/api/v1/db-verify/' + voteId)).json();
    if (verifyRes.exists) {
      persisted = true;
      break;
    }
  }
  assert(persisted, '7. Exact Vote Persistence Verification in SQLite (vote_id: ' + voteId + ')');

  const m2 = await (await fetch('http://127.0.0.1:3000/api/v1/metrics')).json();
  assert(m2.lifecycle.persisted === 1, '8. Dashboard Persisted Vote Metric Reconciled (Count = 1)');

  const req1 = fetch('http://127.0.0.1:3000/api/v1/poll/close', { method: 'POST' });
  const req2 = fetch('http://127.0.0.1:3000/api/v1/poll/close', { method: 'POST' });
  const [res1, res2] = await Promise.all([req1, req2]);
  const isLockContentionPass = (res1.status === 200 && res2.status === 409) || (res1.status === 409 && res2.status === 200);
  assert(isLockContentionPass, '9. Single-Instance Mutex Lock Contention (1 Granted 200 / 1 Denied 409)');

  console.log('====================================================');
  if (hasFailures) {
    console.error('? INTEGRITY TEST SUITE FAILED - SEE ERRORS ABOVE');
    process.exit(1);
  } else {
    console.log('?? ALL SYSTEM INTEGRITY CHECKS PASSED SUCCESSFULLY!');
  }
  console.log('====================================================');
}

runTests();
`;

fs.writeFileSync('server.js', serverCode);
fs.writeFileSync('test_suite.js', testCode);
console.log('? Cleanly written server.js and test_suite.js without any encoding issues!');
