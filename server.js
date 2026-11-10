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

// Always seed/repair essential demo accounts
const seedUser = db.prepare('INSERT OR REPLACE INTO users (id, name, dob, role) VALUES (?, ?, ?, ?)');
seedUser.run('STU101', 'Aarav Sharma', '2003-05-14', 'student');
seedUser.run('STU102', 'Priya Patel', '2004-08-22', 'candidate');
seedUser.run('ADMIN01', 'Election Commission Admin', '1985-01-01', 'admin');

if (db.prepare('SELECT count(*) as count FROM elections').get().count === 0) {
  db.prepare('INSERT INTO elections (id, title, description, status) VALUES (?, ?, ?, ?)').run(
    'ELEC2026_01',
    'Campus Student Council President 2026',
    'Official presidential election.',
    'ACTIVE'
  );
}

if (db.prepare('SELECT count(*) as count FROM candidates').get().count === 0) {
  db.prepare('INSERT INTO candidates (id, election_id, user_id, name, manifesto, status) VALUES (?, ?, ?, ?, ?, ?)').run(
    'CAND_01',
    'ELEC2026_01',
    'STU102',
    'Priya Patel',
    'Enhance campus wifi coverage.',
    'APPROVED'
  );
}

// Case-Insensitive Auth Endpoint (Fixes Login Error)
app.post('/api/auth/login', (req, res) => {
  const { user_id, dob, role } = req.body;
  if (!user_id || !dob || !role) return res.status(400).json({ error: 'Required fields missing' });
  const user = db.prepare('SELECT * FROM users WHERE UPPER(id) = UPPER(?) AND dob = ? AND LOWER(role) = ?').get(String(user_id).trim(), String(dob).trim(), String(role).trim().toLowerCase());
  if (!user) return res.status(401).json({ error: 'Invalid ID, DOB, or Role' });
  return res.json({ success: true, user });
});

app.get('/api/elections', (req, res) => res.json({ success: true, data: db.prepare('SELECT * FROM elections').all() }));

app.get('/api/elections/:id/candidates', (req, res) => res.json({ success: true, data: db.prepare("SELECT * FROM candidates WHERE election_id = ? AND status = 'APPROVED'").all(req.params.id) }));

app.post('/api/vote', (req, res) => {
  try {
    const info = db.prepare('INSERT INTO votes (election_id, voter_id, candidate_id) VALUES (?, ?, ?)').run(req.body.election_id, req.body.voter_id, req.body.candidate_id);
    return res.json({ success: true, correlation_id: info.lastInsertRowid });
  } catch (e) {
    return res.status(400).json({ error: 'Already voted or invalid ballot.' });
  }
});

app.get('/api/elections/previous-winners', (req, res) => res.json({ success: true, data: db.prepare('SELECT * FROM certified_winners ORDER BY certified_at DESC').all() }));

app.listen(PORT, () => console.log('VOTONIC Live on ' + PORT));
