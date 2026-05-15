const Database = require('better-sqlite3');
const path = require('path');

// Connexion à la base de données SQLite3
const db = new Database(path.join(__dirname, 'orders.db'));

// Activation des foreign keys et du mode WAL pour de meilleures performances
db.pragma('journal_mode = WAL');
db.pragma('foreign_keys = ON');

// Création de la table orders si elle n'existe pas
db.exec(`
  CREATE TABLE IF NOT EXISTS orders (
    order_id         TEXT PRIMARY KEY,
    client_id        TEXT NOT NULL,
    client_name      TEXT NOT NULL,
    pickup_address   TEXT NOT NULL,
    delivery_address TEXT NOT NULL,
    items            TEXT NOT NULL,
    status           TEXT NOT NULL DEFAULT 'PENDING',
    driver_id        TEXT,
    created_at       TEXT NOT NULL
  )
`);

console.log('[Order-DB] SQLite3 initialisée avec succès');

// ─── CRUD Operations ───────────────────────────────────────────────────────────

function createOrder({ order_id, client_id, client_name, pickup_address, delivery_address, items, created_at }) {
  const stmt = db.prepare(`
    INSERT INTO orders (order_id, client_id, client_name, pickup_address, delivery_address, items, status, created_at)
    VALUES (?, ?, ?, ?, ?, ?, 'PENDING', ?)
  `);
  stmt.run(order_id, client_id, client_name, pickup_address, delivery_address, JSON.stringify(items), created_at);
  return getOrderById(order_id);
}

function getOrderById(order_id) {
  const row = db.prepare('SELECT * FROM orders WHERE order_id = ?').get(order_id);
  return row ? formatOrder(row) : null;
}

function updateOrderStatus(order_id, status, driver_id = null) {
  if (driver_id) {
    db.prepare('UPDATE orders SET status = ?, driver_id = ? WHERE order_id = ?')
      .run(status, driver_id, order_id);
  } else {
    db.prepare('UPDATE orders SET status = ? WHERE order_id = ?')
      .run(status, order_id);
  }
  return getOrderById(order_id);
}

function listOrdersByClient(client_id) {
  const rows = db.prepare('SELECT * FROM orders WHERE client_id = ? ORDER BY created_at DESC').all(client_id);
  return rows.map(formatOrder);
}

function deleteOrder(order_id) {
  const result = db.prepare('DELETE FROM orders WHERE order_id = ?').run(order_id);
  return result.changes > 0;
}

// Convertit les items de JSON string → tableau
function formatOrder(row) {
  return {
    ...row,
    items: JSON.parse(row.items || '[]'),
    driver_id: row.driver_id || ''
  };
}

module.exports = { createOrder, getOrderById, updateOrderStatus, listOrdersByClient, deleteOrder };