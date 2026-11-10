const Database = require('better-sqlite3');
const path = require('path');

const db = new Database(path.join(__dirname, '../../votecore.db'));

db.exec(`
  CREATE TABLE IF NOT EXISTS catalog_relational (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sku TEXT UNIQUE,
    title TEXT,
    category TEXT,
    price REAL,
    attributes_json TEXT
  );
  CREATE INDEX IF NOT EXISTS idx_cat_category ON catalog_relational(category);
  CREATE INDEX IF NOT EXISTS idx_cat_price ON catalog_relational(price);
`);

function seedCatalog() {
  const count = db.prepare('SELECT COUNT(*) as cnt FROM catalog_relational').get().cnt;
  if (count < 100) {
    const insert = db.prepare('INSERT INTO catalog_relational (sku, title, category, price, attributes_json) VALUES (?, ?, ?, ?, ?)');
    for (let i = 1; i <= 200; i++) {
      insert.run(
        `SKU-${1000 + i}`,
        `Item ${i}`,
        i % 2 === 0 ? 'Electronics' : 'Books',
        (i * 12.5) % 300 + 5,
        JSON.stringify({ brand: `Brand_${i % 5}`, weight: `${i * 10}g` })
      );
    }
  }
}
seedCatalog();

function benchmarkQuery(category, minPrice) {
  const startRelational = process.hrtime.bigint();
  const relResults = db.prepare('SELECT * FROM catalog_relational WHERE category = ? AND price >= ?').all(category, minPrice);
  const endRelational = process.hrtime.bigint();

  const relationalLatencyMs = Number(endRelational - startRelational) / 1e6;

  return {
    query: { category, minPrice },
    relationalResultsCount: relResults.length,
    relationalQueryTimeMs: parseFloat(relationalLatencyMs.toFixed(3)),
    mongoNote: "Document structures (MongoDB) store nested attributes without JSON stringification overhead, enabling document-level index scans."
  };
}

module.exports = { benchmarkQuery };
