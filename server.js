const express = require('express');
const Database = require('better-sqlite3');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 10000;

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

const db = new Database('votonic.db');

db.exec(`
  PRAGMA journal_mode = WAL;
  CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    dob TEXT NOT NULL,
    role TEXT NOT NULL
  );
  CREATE TABLE IF NOT EXISTS elections (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT,
    status TEXT DEFAULT 'ACTIVE'
  );
  CREATE TABLE IF NOT EXISTS candidates (
    id TEXT PRIMARY KEY,
    election_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    name TEXT NOT NULL,
    manifesto TEXT,
    status TEXT DEFAULT 'PENDING'
  );
  CREATE TABLE IF NOT EXISTS votes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    election_id TEXT NOT NULL,
    voter_id TEXT NOT NULL,
    candidate_id TEXT NOT NULL,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(election_id, voter_id)
  );
  CREATE TABLE IF NOT EXISTS certified_winners (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    election_id TEXT UNIQUE NOT NULL,
    election_title TEXT NOT NULL,
    election_description TEXT,
    winner_name TEXT NOT NULL,
    winning_percentage REAL NOT NULL,
    total_votes_cast INTEGER NOT NULL,
    certified_at DATETIME DEFAULT CURRENT_TIMESTAMP
  );
`);

// Auto-seed / Repair demo accounts with INSERT OR REPLACE
const seedUser = db.prepare('INSERT OR REPLACE INTO users (id, name, dob, role) VALUES (?, ?, ?, ?)');
seedUser.run('STU101', 'Aarav Sharma', '2003-05-14', 'student');
seedUser.run('STU102', 'Priya Patel', '2004-08-22', 'candidate');
seedUser.run('ADMIN01', 'Election Commission Admin', '1985-01-01', 'admin');

const elecCount = db.prepare('SELECT count(*) as count FROM elections').get();
if (elecCount.count === 0) {
  db.prepare('INSERT INTO elections (id, title, description, status) VALUES (?, ?, ?, ?)').run(
    'ELEC2026_01',
    'Campus Student Council President 2026',
    'Official presidential election for student council governance.',
    'ACTIVE'
  );
}

const candCount = db.prepare('SELECT count(*) as count FROM candidates').get();
if (candCount.count === 0) {
  db.prepare('INSERT INTO candidates (id, election_id, user_id, name, manifesto, status) VALUES (?, ?, ?, ?, ?, ?)').run(
    'CAND_01',
    'ELEC2026_01',
    'STU102',
    'Priya Patel',
    'Enhance campus wifi coverage and extend library hours during exams.',
    'APPROVED'
  );
}

// 1. Authentication Endpoint (Case-Insensitive Match)
app.post('/api/auth/login', (req, res) => {
  const { user_id, dob, role } = req.body;
  if (!user_id || !dob || !role) {
    return res.status(400).json({ error: 'User ID, DOB, and Role are required.' });
  }

  try {
    const cleanId = String(user_id).trim();
    const cleanDob = String(dob).trim();
    const cleanRole = String(role).trim().toLowerCase();

    const user = db.prepare('SELECT * FROM users WHERE UPPER(id) = UPPER(?) AND dob = ? AND LOWER(role) = ?').get(cleanId, cleanDob, cleanRole);

    if (!user) {
      return res.status(401).json({ error: 'Invalid ID, Date of Birth, or Role.' });
    }
    return res.json({ success: true, user });
  } catch (err) {
    return res.status(500).json({ error: 'Database authentication failed.' });
  }
});

// 2. Admin User Creation Endpoint
app.post('/api/admin/users/create', (req, res) => {
  const { user_id, name, dob, role } = req.body;
  if (!user_id || !name || !dob || !role) {
    return res.status(400).json({ error: 'All fields (User ID, Name, DOB, Role) are required.' });
  }

  try {
    const stmt = db.prepare('INSERT INTO users (id, name, dob, role) VALUES (?, ?, ?, ?)');
    stmt.run(String(user_id).trim(), String(name).trim(), String(dob).trim(), String(role).trim().toLowerCase());
    return res.json({ success: true, message: `User ${user_id} registered successfully.` });
  } catch (err) {
    if (err.message.includes('UNIQUE constraint failed')) {
      return res.status(400).json({ error: 'User ID already exists.' });
    }
    return res.status(500).json({ error: 'Failed to create user account.' });
  }
});

// 3. Elections & Candidates API Routes
app.get('/api/elections', (req, res) => {
  res.json({ success: true, data: db.prepare('SELECT * FROM elections').all() });
});

app.get('/api/elections/:id/candidates', (req, res) => {
  res.json({ success: true, data: db.prepare('SELECT * FROM candidates WHERE election_id = ? AND status = "APPROVED"').all(req.params.id) });
});

// 4. Vote Endpoint
app.post('/api/vote', (req, res) => {
  const { election_id, voter_id, candidate_id } = req.body;
  if (!election_id || !voter_id || !candidate_id) {
    return res.status(400).json({ error: 'Missing required voting parameters.' });
  }
  try {
    const info = db.prepare('INSERT INTO votes (election_id, voter_id, candidate_id) VALUES (?, ?, ?)').run(election_id, voter_id, candidate_id);
    return res.json({ success: true, message: 'Ballot recorded successfully.', correlation_id: info.lastInsertRowid });
  } catch (err) {
    if (err.message.includes('UNIQUE constraint failed')) {
      return res.status(400).json({ error: 'You have already voted in this election.' });
    }
    return res.status(500).json({ error: 'Failed to submit ballot.' });
  }
});

// 5. Candidate Application Endpoint
app.post('/api/candidates/apply', (req, res) => {
  const { election_id, user_id, name, manifesto } = req.body;
  try {
    db.prepare('INSERT INTO candidates (id, election_id, user_id, name, manifesto, status) VALUES (?, ?, ?, ?, ?, "PENDING")').run('CAND_' + Date.now(), election_id, user_id, name, manifesto);
    res.json({ success: true, message: 'Nomination request submitted.' });
  } catch (err) {
    res.status(500).json({ error: 'Nomination submission failed.' });
  }
});

// 6. Admin Governance & Election Management
app.get('/api/admin/candidates/pending', (req, res) => {
  res.json({ success: true, data: db.prepare('SELECT * FROM candidates WHERE status = "PENDING"').all() });
});

app.post('/api/admin/candidates/:id/status', (req, res) => {
  db.prepare('UPDATE candidates SET status = ? WHERE id = ?').run(req.body.status, req.params.id);
  res.json({ success: true });
});

app.post('/api/admin/elections/:id/close', (req, res) => {
  const electionId = req.params.id;
  const elec = db.prepare('SELECT * FROM elections WHERE id = ?').get(electionId);
  if (!elec) return res.status(404).json({ error: 'Election not found.' });

  const votes = db.prepare('SELECT candidate_id, count(*) as count FROM votes WHERE election_id = ? GROUP BY candidate_id ORDER BY count DESC').all(electionId);
  const totalVotes = db.prepare('SELECT count(*) as count FROM votes WHERE election_id = ?').get(electionId).count;

  let winnerName = 'No Votes Cast';
  let winningPct = 0;

  if (votes.length > 0) {
    const winnerCand = db.prepare('SELECT name FROM candidates WHERE id = ?').get(votes[0].candidate_id);
    if (winnerCand) winnerName = winnerCand.name;
    winningPct = totalVotes > 0 ? ((votes[0].count / totalVotes) * 100).toFixed(1) : 0;
  }

  db.prepare('UPDATE elections SET status = "CLOSED" WHERE id = ?').run(electionId);
  db.prepare('INSERT OR REPLACE INTO certified_winners (election_id, election_title, election_description, winner_name, winning_percentage, total_votes_cast) VALUES (?, ?, ?, ?, ?, ?)').run(
    electionId, elec.title, elec.description, winnerName, winningPct, totalVotes
  );

  res.json({ success: true, winner: winnerName, winningPct });
});

app.get('/api/elections/previous-winners', (req, res) => {
  res.json({ success: true, data: db.prepare('SELECT * FROM certified_winners ORDER BY certified_at DESC').all() });
});

app.listen(PORT, () => {
  console.log(`VOTONIC Server running on port ${PORT}`);
});
