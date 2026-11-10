const http = require('http');

const TARGET_URL = 'http://localhost:3000/api/vote';
const TOTAL_VOTES = 200;
const CONCURRENCY = 10;

let sent = 0;
let completed = 0;
let start = Date.now();
const latencies = [];

function sendVote(voterId, candidateId) {
  return new Promise((resolve) => {
    const data = JSON.stringify({ voter_id: voterId, candidate_id: candidateId });
    const reqStart = Date.now();

    const req = http.request(TARGET_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(data)
      }
    }, (res) => {
      res.on('data', () => {});
      res.on('end', () => {
        latencies.push(Date.now() - reqStart);
        completed++;
        resolve();
      });
    });

    req.on('error', () => {
      completed++;
      resolve();
    });

    req.write(data);
    req.end();
  });
}

async function run() {
  console.log(`[Load Test] Dispatching ${TOTAL_VOTES} requests (Concurrency: ${CONCURRENCY})...`);
  const candidates = ['c1', 'c2', 'c3'];

  async function worker() {
    while (sent < TOTAL_VOTES) {
      const id = ++sent;
      const voterId = `load_voter_${id}`;
      const candidateId = candidates[id % 3];
      await sendVote(voterId, candidateId);
    }
  }

  const workers = [];
  for (let i = 0; i < CONCURRENCY; i++) {
    workers.push(worker());
  }

  await Promise.all(workers);
  const totalTimeSec = (Date.now() - start) / 1000;
  latencies.sort((a, b) => a - b);

  const p50 = latencies[Math.floor(latencies.length * 0.50)];
  const p95 = latencies[Math.floor(latencies.length * 0.95)];
  const p99 = latencies[Math.floor(latencies.length * 0.99)];

  console.log('\n==================================================');
  console.log(`Completed ${completed} requests in ${totalTimeSec.toFixed(2)}s`);
  console.log(`Throughput: ${(completed / totalTimeSec).toFixed(2)} req/sec`);
  console.log(`Latency p50: ${p50}ms | p95: ${p95}ms | p99: ${p99}ms`);
  console.log('==================================================\n');
}

run();
