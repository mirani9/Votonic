const Database = require('better-sqlite3');
const Redis = require('ioredis');
const amqp = require('amqplib');
const path = require('path');

const db = new Database(path.join(__dirname, 'VOTONIC.db'));
db.pragma('journal_mode = WAL');
db.pragma('busy_timeout = 5000');

const redis = new Redis({ host: process.env.REDIS_HOST || '127.0.0.1', port: 6379 });
const QUEUE_NAME = 'votes_queue_v2';

async function startWorker() {
  try {
    const conn = await amqp.connect(process.env.RABBITMQ_URL || 'amqp://localhost');
    const channel = await conn.createChannel();
    await channel.assertQueue('votes_dlq', { durable: true }); await channel.assertQueue(QUEUE_NAME, { durable: true, arguments: { 'x-dead-letter-exchange': '', 'x-dead-letter-routing-key': 'votes_dlq' } });

    channel.prefetch(10);
    console.log('[Campus Election Worker] Listening for ballot messages...');

    channel.consume(QUEUE_NAME, async (msg) => {
      if (!msg) return;

      let payload;
      try {
        payload = JSON.parse(msg.content.toString());
      } catch (e) {
        channel.reject(msg, false);
        return;
      }

      const { election_id, voter_id, candidate_id } = payload;

      try {
        const insert = db.prepare('INSERT OR IGNORE INTO votes (election_id, voter_id, candidate_id, created_at) VALUES (?, ?, ?, ?)');
        const result = insert.run(election_id, voter_id, candidate_id, new Date().toISOString());

        if (result.changes > 0) {
          const actualCount = db.prepare('SELECT COUNT(*) as cnt FROM votes WHERE election_id = ? AND candidate_id = ?').get(election_id, candidate_id).cnt;
          await redis.zadd(`leaderboard:${election_id}`, actualCount, candidate_id);
          console.log(`[Worker] Ballot Committed: election=${election_id}, voter=${voter_id}, candidate=${candidate_id}`);
        }

        channel.ack(msg);
      } catch (err) {
        console.error('[Worker Execution Error]:', err.message);
        channel.reject(msg, false);
      }
    }, { noAck: false });

  } catch (err) {
    console.error('[Worker Error]:', err.message);
  }
}

startWorker();
