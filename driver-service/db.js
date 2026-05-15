const { createRxDatabase, addRxPlugin } = require('rxdb');
const { getRxStorageLevelDB }            = require('rxdb-storage-leveldb');
const path                               = require('path');

// ─── Schéma de la collection "drivers" ────────────────────────────────────────
const driverSchema = {
  version: 0,
  primaryKey: 'driver_id',
  type: 'object',
  properties: {
    driver_id:        { type: 'string', maxLength: 36 },
    name:             { type: 'string' },
    phone:            { type: 'string' },
    vehicle:          { type: 'string' },
    available:        { type: 'boolean' },
    latitude:         { type: 'number' },
    longitude:        { type: 'number' },
    current_order_id: { type: 'string' },
    created_at:       { type: 'string' }
  },
  required: ['driver_id', 'name', 'phone', 'vehicle']
};

// ─── Instance RxDB (singleton) ─────────────────────────────────────────────────
let driversCollection = null;

async function getCollection() {
  if (driversCollection) return driversCollection;

  const db = await createRxDatabase({
    name: path.join(__dirname, 'drivers_rxdb'),   // dossier LevelDB
    storage: getRxStorageLevelDB()
  });

  await db.addCollections({
    drivers: { schema: driverSchema }
  });

  driversCollection = db.drivers;
  console.log('[Driver-RxDB] Base de données RxDB+LevelDB initialisée (drivers_rxdb/)');
  return driversCollection;
}

// ─── CRUD ──────────────────────────────────────────────────────────────────────

async function createDriver({ driver_id, name, phone, vehicle }) {
  const col = await getCollection();
  const doc = await col.insert({
    driver_id,
    name,
    phone,
    vehicle,
    available:        true,
    latitude:         0,
    longitude:        0,
    current_order_id: '',
    created_at:       new Date().toISOString()
  });
  return doc.toJSON();
}

async function getDriverById(driver_id) {
  const col = await getCollection();
  const doc = await col.findOne(driver_id).exec();
  return doc ? doc.toJSON() : null;
}

async function updateDriverLocation(driver_id, latitude, longitude) {
  const col = await getCollection();
  const doc = await col.findOne(driver_id).exec();
  if (!doc) return null;
  await doc.patch({ latitude, longitude });
  return doc.toJSON();
}

async function updateDriverAvailability(driver_id, available) {
  const col = await getCollection();
  const doc = await col.findOne(driver_id).exec();
  if (!doc) return null;
  await doc.patch({ available });
  return doc.toJSON();
}

async function assignDriverToOrder(driver_id, order_id) {
  const col = await getCollection();
  const doc = await col.findOne(driver_id).exec();
  if (!doc) return null;
  await doc.patch({ current_order_id: order_id, available: false });
  return doc.toJSON();
}

async function getAvailableDrivers() {
  const col = await getCollection();
  const docs = await col.find({
    selector: { available: { $eq: true } }
  }).exec();
  return docs.map(d => d.toJSON());
}

module.exports = {
  createDriver,
  getDriverById,
  updateDriverLocation,
  updateDriverAvailability,
  assignDriverToOrder,
  getAvailableDrivers
};