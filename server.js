const express = require('express');
const cors = require('cors');
const path = require('path');
const Database = require('better-sqlite3');
const Redis = require('ioredis');
const amqp = require('amqplib');
const productsRouter = require('./modules/exp1_crud/products');

const app = express();
const PORT = process.env.PORT || 3000;
const INSTANCE_ID = process.env.INSTANCE_ID || `instance-${PORT}`;

app.use(cors());
app.use(express.json({ limit: '50kb' }));
app.use(express.static(path.join(__dirname, 'public')));

app.use((req, res, next) => {
  req.correlationId = req.headers['x-correlation-id'] || `corr-${Math.random().toString(36).substring(2, 10)}`;
  res.setHeader('X-Correlation-Id', req.correlationId);
  res.setHeader('X-Instance-Id', INSTANCE_ID);
  res.setHeader('X-Gateway-Auth', 'VERIFIED');
  res.setHeader('X-Gateway-Processed', 'true');
  next();
});

const db = new Database(path.join(__dirname, 'VOTONIC.db'));
db.pragma('journal_mode = WAL');
db.pragma('busy_timeout = 5000');

db.exec(`
  CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL,
    dob TEXT NOT NULL,
    role TEXT CHECK(role IN ('STUDENT', 'CANDIDATE', 'ADMIN')) NOT NULL,
    department TEXT NOT NULL,
    created_at TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS elections (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT,
    department_scope TEXT DEFAULT 'ALL',
    status TEXT CHECK(status IN ('UPCOMING', 'ACTIVE', 'COMPLETED')) DEFAULT 'ACTIVE',
    created_at TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS candidates (
    id TEXT PRIMARY KEY,
    election_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    name TEXT NOT NULL,
    manifesto TEXT NOT NULL,
    status TEXT CHECK(status IN ('PENDING', 'APPROVED', 'REJECTED')) DEFAULT 'PENDING',
    FOREIGN KEY(election_id) REFERENCES elections(id)
  );

  CREATE TABLE IF NOT EXISTS votes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    election_id TEXT NOT NULL,
    voter_id TEXT NOT NULL,
    candidate_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(election_id, voter_id)
  );
`);

// Seed Default System Data
const userCount = db.prepare('SELECT COUNT(*) as cnt FROM users').get().cnt;
if (userCount === 0) {
  const insertUser = db.prepare('INSERT INTO users (id, name, email, dob, role, department, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)');
  insertUser.run('ADMIN01', 'Dean Office Admin', 'admin@campus.edu', '1985-01-01', 'ADMIN', 'Administration', new Date().toISOString());
  insertUser.run('STU101', 'Rahul Sharma', 'rahul@student.edu', '2003-05-14', 'STUDENT', 'Computer Science', new Date().toISOString());
  insertUser.run('STU102', 'Priya Patel', 'priya@student.edu', '2004-08-22', 'CANDIDATE', 'Information Technology', new Date().toISOString());

  const insertElection = db.prepare('INSERT INTO elections (id, title, description, department_scope, status, created_at) VALUES (?, ?, ?, ?, ?, ?)');
  insertElection.run('elec_2026_cs', 'Computer Science Council President 2026', 'Annual election for CS Department President.', 'Computer Science', 'ACTIVE', new Date().toISOString());
  insertElection.run('elec_2026_sports', 'Campus Sports Union Secretary 2026', 'University Sports Representative Election.', 'ALL', 'ACTIVE', new Date().toISOString());

  const insertCandidate = db.prepare('INSERT INTO candidates (id, election_id, user_id, name, manifesto, status) VALUES (?, ?, ?, ?, ?, ?)');
  insertCandidate.run('c1', 'elec_2026_cs', 'STU101', 'Rahul Sharma', '24x7 Lab Access and Open-Source Workshops', 'APPROVED');
  insertCandidate.run('c2', 'elec_2026_cs', 'STU102', 'Priya Patel', 'Industry Internship Drives and Peer Tutoring', 'APPROVED');
}

const redis = new Redis({ host: process.env.REDIS_HOST || '127.0.0.1', port: 6379, maxRetriesPerRequest: null });

let channel = null;
const QUEUE_NAME = 'votes_queue';
async function initRabbit() {
  try {
    const conn = await amqp.connect(process.env.RABBITMQ_URL || 'amqp://localhost');
    channel = await conn.createConfirmChannel();
    await channel.assertQueue('votes_dlq', { durable: true });
    await channel.assertQueue(QUEUE_NAME, {
      durable: true,
      arguments: { 'x-dead-letter-exchange': '', 'x-dead-letter-routing-key': 'votes_dlq' }
    });
  } catch (err) {}
}
initRabbit();

app.get('/health', (req, res) => res.json({ status: 'UP', service: 'VOTONIC Core' }));
app.use('/api/v1', productsRouter);

// AUTHENTICATION API
app.post('/api/auth/login', (req, res) => {
  const { user_id, dob, role } = req.body;
  if (!user_id || !dob) {
    return res.status(400).json({ error: "User ID and Date of Birth (DOB) are required." });
  }

  const user = db.prepare('SELECT * FROM users WHERE id = ? AND dob = ?').get(user_id, dob);
  if (!user) {
    return res.status(401).json({ error: "Invalid login credentials. Please check your ID and DOB." });
  }

  if (role && user.role !== role && user.role !== 'ADMIN') {
    return res.status(403).json({ error: `Access denied. Account is registered as ${user.role}, not ${role}.` });
  }

  res.json({ success: true, user: { id: user.id, name: user.name, role: user.role, department: user.department } });
});

// GET ACTIVE ELECTIONS
app.get('/api/elections', (req, res) => {
  const elections = db.prepare('SELECT * FROM elections ORDER BY created_at DESC').all();
  res.json({ success: true, data: elections });
});

// GET APPROVED CANDIDATES FOR VOTING
app.get('/api/elections/:id/candidates', (req, res) => {
  const candidates = db.prepare("SELECT * FROM candidates WHERE election_id = ? AND status = 'APPROVED'").all(req.params.id);
  res.json({ success: true, data: candidates });
});

// CANDIDATE SUBMIT NOMINATION
app.post('/api/candidates/apply', (req, res) => {
  const { election_id, user_id, name, manifesto } = req.body;
  if (!election_id || !name || !manifesto) {
    return res.status(400).json({ error: "Election, Name, and Manifesto are required." });
  }

  const id = `cand_${Date.now()}`;
  db.prepare('INSERT INTO candidates (id, election_id, user_id, name, manifesto, status) VALUES (?, ?, ?, ?, ?, ?)').run(
    id, election_id, user_id || `usr_${Date.now()}`, name, manifesto, 'PENDING'
  );
  
  res.status(201).json({ success: true, candidate_id: id, message: "Nomination request submitted successfully! Awaiting Administrator Approval." });
});

// ADMIN: GET PENDING APPLICATIONS
app.get('/api/admin/candidates/pending', (req, res) => {
  const pending = db.prepare("SELECT * FROM candidates WHERE status = 'PENDING' ORDER BY id DESC").all();
  res.json({ success: true, data: pending });
});

// ADMIN: APPROVE / REJECT NOMINATION
app.post('/api/admin/candidates/:id/status', (req, res) => {
  const { status } = req.body;
  if (!['APPROVED', 'REJECTED'].includes(status)) {
    return res.status(400).json({ error: "Status must be APPROVED or REJECTED." });
  }
  db.prepare('UPDATE candidates SET status = ? WHERE id = ?').run(status, req.params.id);
  res.json({ success: true, status, message: `Candidate nomination ${status}.` });
});

// ADMIN: CLOSE A SPECIFIC ELECTION
app.post('/api/admin/elections/:id/close', (req, res) => {
  const election = db.prepare('SELECT * FROM elections WHERE id = ?').get(req.params.id);
  if (!election) return res.status(404).json({ error: "Election not found." });

  db.prepare("UPDATE elections SET status = 'COMPLETED' WHERE id = ?").run(req.params.id);
  res.json({ success: true, message: `Election "${election.title}" has been closed.` });
});

// STRICT SINGLE-VOTE ENFORCEMENT ROUTE
app.post('/api/vote', async (req, res) => {
  const { election_id, voter_id, candidate_id } = req.body;

  if (!election_id || !voter_id || !candidate_id) {
    return res.status(400).json({ error: "Missing required ballot parameters." });
  }

  // 1. Verify Election Active Status
  const election = db.prepare('SELECT status FROM elections WHERE id = ?').get(election_id);
  if (!election || election.status !== 'ACTIVE') {
    return res.status(403).json({ error: "Voting is currently closed for this election." });
  }

  // 2. Pre-Check Database Truth (Primary Single-Vote Guard)
  const existingVote = db.prepare('SELECT id FROM votes WHERE election_id = ? AND voter_id = ?').get(election_id, voter_id);
  if (existingVote) {
    return res.status(409).json({ error: `Duplicate Ballot Denied: Student ID "${voter_id}" has already cast a vote in this election.` });
  }

  // 3. Fast-Path Mutex Lock in Redis (Concurrent Request Protection)
  const lockKey = `lock:vote:${election_id}:${voter_id}`;
  const ownerToken = Math.random().toString(36).substring(2);
  const acquiredLock = await redis.set(lockKey, ownerToken, 'NX', 'PX', 5000);

  if (!acquiredLock) {
    return res.status(409).json({ error: "Concurrent voting request in progress. Duplicate attempt blocked." });
  }

  const msgPayload = Buffer.from(JSON.stringify({
    election_id, voter_id, candidate_id, correlation_id: req.correlationId, timestamp: new Date().toISOString()
  }));

  if (channel) {
    channel.sendToQueue(QUEUE_NAME, msgPayload, { persistent: true }, async (err) => {
      // Release Mutex via Lua Script
      const luaScript = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
      await redis.eval(luaScript, 1, lockKey, ownerToken);

      if (err) return res.status(500).json({ error: "Queue delivery failure." });

      res.json({
        status: "ACCEPTED",
        message: "Vote cast successfully.",
        election_id,
        voter_id,
        correlation_id: req.correlationId
      });
    });
  } else {
    try {
      db.prepare('INSERT INTO votes (election_id, voter_id, candidate_id, created_at) VALUES (?, ?, ?, ?)').run(
        election_id, voter_id, candidate_id, new Date().toISOString()
      );
      
      const luaScript = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
      await redis.eval(luaScript, 1, lockKey, ownerToken);

      res.json({ status: "ACCEPTED", message: "Vote recorded successfully.", correlation_id: req.correlationId });
    } catch (dbErr) {
      const luaScript = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
      await redis.eval(luaScript, 1, lockKey, ownerToken);

      if (dbErr.message.includes('UNIQUE')) {
        return res.status(409).json({ error: `Duplicate Ballot Denied: Student ID "${voter_id}" has already voted.` });
      }
      res.status(500).json({ error: "Database insertion error." });
    }
  }
});

// PUBLIC PREVIOUS WINNERS API (NO LOGIN REQUIRED)
app.get('/api/elections/previous-winners', (req, res) => {
  const completedElections = db.prepare("SELECT * FROM elections WHERE status = 'COMPLETED'").all();
  const winners = [];

  for (const e of completedElections) {
    const candidates = db.prepare("SELECT * FROM candidates WHERE election_id = ? AND status = 'APPROVED'").all(e.id);
    const totalVotes = db.prepare('SELECT COUNT(*) as cnt FROM votes WHERE election_id = ?').get(e.id).cnt;
    let topCandidate = null;
    let maxVotes = -1;

    for (const c of candidates) {
      const cnt = db.prepare('SELECT COUNT(*) as cnt FROM votes WHERE election_id = ? AND candidate_id = ?').get(e.id, c.id).cnt;
      if (cnt > maxVotes) {
        maxVotes = cnt;
        topCandidate = c;
      }
    }

    if (topCandidate) {
      const percentage = totalVotes > 0 ? ((maxVotes / totalVotes) * 100).toFixed(1) : 0;
      winners.push({
        election_id: e.id,
        election_title: e.title,
        election_description: e.description,
        winner_name: topCandidate.name,
        winning_percentage: percentage,
        total_votes_cast: totalVotes
      });
    }
  }

  res.json({ success: true, data: winners });
});

app.listen(PORT, () => console.log(`[VOTONIC Engine] Running on port ${PORT}`));
