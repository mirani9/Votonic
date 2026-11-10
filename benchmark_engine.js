const http = require('http');
const Redis = require('ioredis');
const app = require('./modules/exp7_gateway/gateway');

const PORT = 3011;
const redis = new Redis({ host: '127.0.0.1', port: 6379 });
let server;

function request(method, path) {
  return new Promise((resolve) => {
    const start = process.hrtime.bigint();
    const req = http.request(`http://localhost:${PORT}${path}`, { method }, (res) => {
      res.on('data', () => {});
      res.on('end', () => {
        const end = process.hrtime.bigint();
        const latencyMs = Number(end - start) / 1e6;
        resolve({ status: res.statusCode, latencyMs });
      });
    });
    req.on('error', () => resolve({ status: 500, latencyMs: 0 }));
    req.end();
  });
}

function calcStats(latencies) {
  latencies.sort((a, b) => a - b);
  const sum = latencies.reduce((acc, v) => acc + v, 0);
  const avg = sum / latencies.length;
  const median = latencies[Math.floor(latencies.length * 0.5)];
  const p95 = latencies[Math.floor(latencies.length * 0.95)];
  return {
    avg: parseFloat(avg.toFixed(3)),
    median: parseFloat(median.toFixed(3)),
    p95: parseFloat(p95.toFixed(3))
  };
}

async function runBenchmarks() {
  server = app.listen(PORT, async () => {
    console.log('===========================================================');
    console.log(' VOTECORE SYSTEM PERFORMANCE & BENCHMARK REPORT ');
    console.log('===========================================================\n');

    // Benchmark 1: Cache Miss vs Cache Hit (50 Iterations)
    const pRes = await request('POST', '/api/v1/products');
    const pId = 1;

    // Cache Miss Latency Test
    await redis.del(`product:${pId}`);
    const missLatencies = [];
    for (let i = 0; i < 50; i++) {
      await redis.del(`product:${pId}`);
      const res = await request('GET', `/api/v1/cached-products/${pId}`);
      missLatencies.push(res.latencyMs);
    }

    // Cache Hit Latency Test
    await request('GET', `/api/v1/cached-products/${pId}`); // Warm cache
    const hitLatencies = [];
    for (let i = 0; i < 50; i++) {
      const res = await request('GET', `/api/v1/cached-products/${pId}`);
      hitLatencies.push(res.latencyMs);
    }

    const missStats = calcStats(missLatencies);
    const hitStats = calcStats(hitLatencies);

    console.log('--- [BENCHMARK 1] REDIS CACHE-ASIDE READ PERFORMANCE ---');
    console.log(`Requests Executed: 50 | Total Sample Count: 100`);
    console.log(`Cache Miss Latency  => Avg: ${missStats.avg} ms | Median: ${missStats.median} ms | p95: ${missStats.p95} ms`);
    console.log(`Cache Hit Latency   => Avg: ${hitStats.avg} ms | Median: ${hitStats.median} ms | p95: ${hitStats.p95} ms`);
    console.log(`Performance Delta   => Cache Hit is ${(missStats.avg / (hitStats.avg || 1)).toFixed(2)}x FASTER than Cache Miss.\n`);

    // Benchmark 2: Load Balancing Latency Comparison
    const rrLatencies = [];
    const lcLatencies = [];

    for (let i = 0; i < 50; i++) {
      const resRR = await request('GET', '/api/v1/lb-test?algo=round-robin');
      rrLatencies.push(resRR.latencyMs);
      const resLC = await request('GET', '/api/v1/lb-test?algo=least-connections');
      lcLatencies.push(resLC.latencyMs);
    }

    const rrStats = calcStats(rrLatencies);
    const lcStats = calcStats(lcLatencies);

    console.log('--- [BENCHMARK 2] LOAD BALANCING ALGORITHM PERFORMANCE ---');
    console.log(`Requests Executed: 50 per algorithm`);
    console.log(`Round-Robin Latency => Avg: ${rrStats.avg} ms | Median: ${rrStats.median} ms | p95: ${rrStats.p95} ms`);
    console.log(`Least-Conn Latency  => Avg: ${lcStats.avg} ms | Median: ${lcStats.median} ms | p95: ${lcStats.p95} ms\n`);

    console.log('===========================================================');
    console.log(' BENCHMARK COMPLETED SUCCESSFULLY');
    console.log('===========================================================\n');

    await redis.quit();
    server.close(() => process.exit(0));
  });
}

runBenchmarks();
