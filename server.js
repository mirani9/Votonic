const express = require('express');
const Redis = require('ioredis');
const amqp = require('amqplib');
const sqlite3 = require('sqlite3').verbose();
const path = require('path');
const { randomUUID: uuidv4 } = require('crypto');

const PORT = process.env.PORT || 3000;
const DB_FILE = process.env.DB_FILE || './votecore_test.db';

const app = express();
app.use(express.json());

// Authoritative Candidate Registry
const POLL_CONFIG = {
  id: 'Campus Tech Leader 2026',
  validCandidates: ['Sophia Chen', 'Marcus Vance', 'Aaliyah Patel']
};

// Database Initialization (SQLite WAL Mode)
const db = new sqlite3.Database(DB_FILE);
db.serialize(() => {
  db.run("PRAGMA journal_mode = WAL;");
  db.run("PRAGMA busy_timeout = 5000;");
  db.run(`CREATE TABLE IF NOT EXISTS votes (
    id INTEGER PRIMARY KEY AUTOINCREMENT, 
    vote_id TEXT UNIQUE, 
    poll TEXT, 
    candidate_id TEXT, 
    user_id TEXT, 
    ip_address TEXT, 
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP, 
    UNIQUE(poll, user_id)
  )`);
});

const redis = new Redis({ host: '127.0.0.1', port: 6380, retryStrategy: (t) => Math.min(t * 100, 3000) });
let connection = null;
let channel = null;
let consumerTag = null;

let metrics = {
  received: 0,
  rejected: 0,
  accepted: 0,
  rateLimited: 0,
  duplicates: 0,
  failed: 0,
  invalidCandidates: 0
};

// RabbitMQ Topology Setup with DLQ
async function initRabbitMQ() {
  try {
    connection = await amqp.connect('amqp://localhost:5673');
    channel = await connection.createConfirmChannel();
    
    await channel.assertExchange('dlx_exchange', 'direct', { durable: true });
    await channel.assertQueue('vote_queue_dlq', { durable: true });
    await channel.bindQueue('vote_queue_dlq', 'dlx_exchange', 'dead_letter');

    await channel.assertQueue('vote_queue', { 
      durable: true,
      arguments: {
        'x-dead-letter-exchange': 'dlx_exchange',
        'x-dead-letter-routing-key': 'dead_letter'
      }
    });

    console.log(`[AMQP] Topology initialized with DLQ on PID ${process.pid}`);
    await startConsumer();
  } catch (e) {
    setTimeout(initRabbitMQ, 2000);
  }
}

async function startConsumer() {
  if (!channel || consumerTag) return;
  const res = await channel.consume('vote_queue', (msg) => {
    if (!msg) return;
    
    let vote;
    try {
      vote = JSON.parse(msg.content.toString());
    } catch (parseError) {
      console.error('[WORKER] Malformed JSON payload. Moving to DLQ.');
      return channel.nack(msg, false, false);
    }

    if (!POLL_CONFIG.validCandidates.includes(vote.candidateId)) {
      console.error(`[WORKER] Invalid candidate ${vote.candidateId}. Moving to DLQ.`);
      return channel.nack(msg, false, false);
    }

    db.run(
      `INSERT INTO votes (vote_id, poll, candidate_id, user_id, ip_address, timestamp) 
       VALUES (?, ?, ?, ?, ?, ?) 
       ON CONFLICT(poll, user_id) DO NOTHING`,
      [vote.voteId, vote.poll, vote.candidateId, vote.userId, vote.ip, vote.timestamp],
      function(err) {
        if (err) {
          console.error('[WORKER] SQLite Persistence Error:', err.message);
          metrics.failed++;
          channel.nack(msg, false, true);
        } else {
          console.log('[EVENT: VOTE_PERSISTED] Vote ID:', vote.voteId);
          channel.ack(msg);
        }
      }
    );
  }, { noAck: false });
  consumerTag = res.consumerTag;
}

async function stopConsumer() {
  if (channel && consumerTag) {
    await channel.cancel(consumerTag);
    consumerTag = null;
  }
}

initRabbitMQ();

// Atomic Sliding Window Rate Limiter (Lua Script)
const RATE_LIMIT_LUA = `
  local key = KEYS[1]
  local now = tonumber(ARGV[1])
  local windowMs = tonumber(ARGV[2])
  local limit = tonumber(ARGV[3])
  local member = ARGV[4]

  redis.call('zremrangebyscore', key, 0, now - windowMs)
  local current = redis.call('zcard', key)

  if current < limit then
    redis.call('zadd', key, now, member)
    redis.call('expire', key, math.ceil(windowMs / 1000) + 1)
    return 1
  else
    return 0
  end
`;

async function isRateAllowed(ip) {
  const windowMs = 10000;
  const maxLimit = 5;
  const now = Date.now();
  const rateKey = 'ratelimit:' + ip;
  const member = now + ':' + Math.random();
  const result = await redis.eval(RATE_LIMIT_LUA, 1, rateKey, now, windowMs, maxLimit, member);
  return result === 1;
}

async function compensateRedisWrite(pollId, candidateId, userId) {
  try {
    const pipeline = redis.pipeline();
    pipeline.srem(`voter_set:${pollId}`, userId);
    pipeline.zincrby('poll:leaderboard', -1, candidateId);
    await pipeline.exec();
    console.log(`[COMPENSATION] Rolled back Redis state for user ${userId}`);
  } catch (err) {
    console.error('[COMPENSATION ERROR] Failed to rollback Redis state:', err.message);
  }
}

// Vote Ingestion Endpoint
app.post('/api/v1/poll/vote', async (req, res) => {
  metrics.received++;

  const isClosed = await redis.get(`poll:status:${POLL_CONFIG.id}`);
  if (isClosed === 'CLOSED') {
    metrics.rejected++;
    return res.status(403).json({ error: 'Poll is closed' });
  }

  const clientIp = req.ip || '127.0.0.1';
  const { candidateId, userId } = req.body;

  if (!candidateId || !userId) {
    metrics.rejected++;
    return res.status(400).json({ error: 'Missing payload parameters' });
  }

  if (!POLL_CONFIG.validCandidates.includes(candidateId)) {
    metrics.rejected++;
    metrics.invalidCandidates++;
    return res.status(400).json({ error: 'Invalid candidate selection' });
  }

  const allowed = await isRateAllowed(clientIp);
  if (!allowed) {
    metrics.rateLimited++;
    metrics.rejected++;
    return res.status(429).json({ error: 'Rate limit exceeded' });
  }

  const voteId = uuidv4();
  const voterSetKey = `voter_set:${POLL_CONFIG.id}`;

  try {
    const added = await redis.sadd(voterSetKey, userId);
    if (added === 0) {
      metrics.duplicates++;
      metrics.rejected++;
      return res.status(409).json({ error: 'User already voted' });
    }

    await redis.zincrby('poll:leaderboard', 1, candidateId);

    const payload = { 
      voteId, 
      poll: POLL_CONFIG.id, 
      candidateId, 
      userId, 
      ip: clientIp, 
      timestamp: new Date().toISOString() 
    };

    if (!channel) {
      await compensateRedisWrite(POLL_CONFIG.id, candidateId, userId);
      throw new Error('AMQP Channel Uninitialized');
    }

    channel.sendToQueue('vote_queue', Buffer.from(JSON.stringify(payload)), { persistent: true });
    
    try {
      await channel.waitForConfirms();
    } catch (pubErr) {
      await compensateRedisWrite(POLL_CONFIG.id, candidateId, userId);
      metrics.failed++;
      metrics.rejected++;
      return res.status(500).json({ error: 'Publisher confirmation failed' });
    }

    metrics.accepted++;
    console.log('[EVENT: VOTE_ACCEPTED] Vote ID:', voteId);
    return res.status(200).json({ success: true, voteId });
  } catch (err) {
    metrics.failed++;
    metrics.rejected++;
    return res.status(500).json({ error: err.message });
  }
});

// Idempotent Poll Closure & Distributed Mutex Endpoint
app.post('/api/v1/poll/close', async (req, res) => {
  const ttlMs = req.body?.ttlMs || 3000;
  const lockKey = `lock:poll_close:${POLL_CONFIG.id}`;
  const pollStatusKey = `poll:status:${POLL_CONFIG.id}`;
  const ownerToken = uuidv4();

  // 1. Acquire Distributed Mutex (Atomic SET NX PX)
  const acquired = await redis.set(lockKey, ownerToken, 'NX', 'PX', ttlMs);

  if (!acquired) {
    return res.status(409).json({ success: false, error: 'Lock contention' });
  }

  // 2. Poll Status Read & Update
  const currentStatus = await redis.get(pollStatusKey);
  await redis.set(pollStatusKey, 'CLOSED');

  // 3. Hold lock past single-threaded event loop tick (200ms delay)
  await new Promise((resolve) => setTimeout(resolve, 200));

  // 4. Safe Lock Release
  const releaseLua = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
  await redis.eval(releaseLua, 1, lockKey, ownerToken);

  return res.status(200).json({ 
    success: true, 
    status: currentStatus === 'CLOSED' ? 'ALREADY_CLOSED' : 'CLOSED', 
    ownerToken 
  });
});

app.post('/api/v1/poll/release-lock', async (req, res) => {
  const { ownerToken } = req.body;
  const lockKey = `lock:poll_close:${POLL_CONFIG.id}`;
  const releaseLua = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
  const result = await redis.eval(releaseLua, 1, lockKey, ownerToken);
  return res.json({ success: result === 1 });
});

app.post('/api/v1/worker/toggle', async (req, res) => {
  const { active } = req.body;
  if (active) {
    await startConsumer();
  } else {
    await stopConsumer();
  }
  return res.json({ success: true, workerActive: !!consumerTag });
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
    const isClosedVal = await redis.get(`poll:status:${POLL_CONFIG.id}`);
    const pending = Math.max(0, metrics.accepted - persisted - metrics.failed);
    res.json({
      lifecycle: {
        received: metrics.received,
        rejected: metrics.rejected,
        accepted: metrics.accepted,
        rateLimited: metrics.rateLimited,
        duplicates: metrics.duplicates,
        invalidCandidates: metrics.invalidCandidates,
        queued,
        persisted,
        pending,
        failed: metrics.failed,
        isClosed: isClosedVal === 'CLOSED'
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

// System Reset Endpoint
app.post('/api/v1/poll/reset', async (req, res) => {
  try {
    await redis.del('poll:leaderboard');
    await redis.del(`voter_set:${POLL_CONFIG.id}`);
    await redis.del(`lock:poll_close:${POLL_CONFIG.id}`);
    await redis.del(`poll:status:${POLL_CONFIG.id}`);
    const keys = await redis.keys('ratelimit:*');
    if (keys.length > 0) await redis.del(keys);

    metrics = {
      received: 0,
      rejected: 0,
      accepted: 0,
      rateLimited: 0,
      duplicates: 0,
      failed: 0,
      invalidCandidates: 0
    };

    db.run('DELETE FROM votes', () => {
      res.json({ success: true });
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/v1/poll/leaderboard', async (req, res) => {
  try {
    const leaderboard = await redis.zrevrange('poll:leaderboard', 0, -1, 'WITHSCORES');
    const results = {};
    for (let i = 0; i < leaderboard.length; i += 2) {
      results[leaderboard[i]] = parseInt(leaderboard[i + 1], 10);
    }
    return res.json({ poll: POLL_CONFIG.id, results });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'index.html')));
app.listen(PORT, () => console.log(`⚡ VOTECORE Hardened Engine running on http://localhost:${PORT}`));