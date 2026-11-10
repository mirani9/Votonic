const express = require('express');
const router = express.Router();
const Database = require('better-sqlite3');
const path = require('path');

const db = new Database(path.join(__dirname, '../../votecore.db'));
db.pragma('journal_mode = WAL');

db.exec(`
  CREATE TABLE IF NOT EXISTS products (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    price REAL NOT NULL,
    category TEXT NOT NULL,
    stock INTEGER DEFAULT 0,
    created_at TEXT NOT NULL
  );
`);

// GET /api/v1/products
router.get('/products', (req, res) => {
  const products = db.prepare('SELECT * FROM products').all();
  res.json({ success: true, count: products.length, data: products });
});

// GET /api/v1/products/:id
router.get('/products/:id', (req, res) => {
  const product = db.prepare('SELECT * FROM products WHERE id = ?').get(req.params.id);
  if (!product) return res.status(404).json({ success: false, error: 'Product not found' });
  res.json({ success: true, data: product });
});

// POST /api/v1/products
router.post('/products', (req, res) => {
  const { name, price, category, stock } = req.body;
  if (!name || typeof price !== 'number' || price < 0 || !category) {
    return res.status(400).json({ success: false, error: 'Invalid payload. Fields required: name, price (>0), category.' });
  }
  const stmt = db.prepare('INSERT INTO products (name, price, category, stock, created_at) VALUES (?, ?, ?, ?, ?)');
  const info = stmt.run(name, price, category, stock || 0, new Date().toISOString());
  const newProduct = db.prepare('SELECT * FROM products WHERE id = ?').get(info.lastInsertRowid);
  res.status(201).json({ success: true, data: newProduct });
});

// PUT /api/v1/products/:id
router.put('/products/:id', (req, res) => {
  const { name, price, category, stock } = req.body;
  const existing = db.prepare('SELECT * FROM products WHERE id = ?').get(req.params.id);
  if (!existing) return res.status(404).json({ success: false, error: 'Product not found' });

  const stmt = db.prepare('UPDATE products SET name = ?, price = ?, category = ?, stock = ? WHERE id = ?');
  stmt.run(name || existing.name, price ?? existing.price, category || existing.category, stock ?? existing.stock, req.params.id);
  const updated = db.prepare('SELECT * FROM products WHERE id = ?').get(req.params.id);
  res.json({ success: true, data: updated });
});

// DELETE /api/v1/products/:id
router.delete('/products/:id', (req, res) => {
  const existing = db.prepare('SELECT * FROM products WHERE id = ?').get(req.params.id);
  if (!existing) return res.status(404).json({ success: false, error: 'Product not found' });

  db.prepare('DELETE FROM products WHERE id = ?').run(req.params.id);
  res.json({ success: true, message: `Product ${req.params.id} deleted successfully` });
});

module.exports = router;
