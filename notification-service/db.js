const Database = require('better-sqlite3');
const path = require('path');

const db = new Database(path.join(__dirname, 'notifications.db'));
db.pragma('journal_mode = WAL');

db.exec(`
  CREATE TABLE IF NOT EXISTS notifications (
    notification_id TEXT PRIMARY KEY,
    recipient_id    TEXT NOT NULL,
    recipient_type  TEXT NOT NULL,
    message         TEXT NOT NULL,
    order_id        TEXT,
    type            TEXT NOT NULL,
    created_at      TEXT NOT NULL
  )
`);

console.log('[Notif-DB] SQLite3 initialisée avec succès');

function saveNotification({ notification_id, recipient_id, recipient_type, message, order_id, type }) {
  db.prepare(`
    INSERT INTO notifications (notification_id, recipient_id, recipient_type, message, order_id, type, created_at)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `).run(notification_id, recipient_id, recipient_type, message, order_id || '', type, new Date().toISOString());

  return getNotificationById(notification_id);
}

function getNotificationById(notification_id) {
  return db.prepare('SELECT * FROM notifications WHERE notification_id = ?').get(notification_id);
}

function getNotificationsByRecipient(recipient_id) {
  return db.prepare('SELECT * FROM notifications WHERE recipient_id = ? ORDER BY created_at DESC').all(recipient_id);
}

module.exports = { saveNotification, getNotificationById, getNotificationsByRecipient };