const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');
const { v4: uuidv4 } = require('uuid');

const db = require('./db');
const { connectProducer, startConsumer, publishEvent, TOPICS } = require('./kafka');

// ─── Chargement du fichier .proto ──────────────────────────────────────────────
const PROTO_DIR  = process.env.PROTO_DIR || path.join(__dirname, '../proto');
const PROTO_PATH = path.join(PROTO_DIR, 'driver.proto');

const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true
});

const driverProto = grpc.loadPackageDefinition(packageDefinition).driver;

// ─── Implémentation des méthodes gRPC ─────────────────────────────────────────

async function registerDriver(call, callback) {
  try {
    const { name, phone, vehicle } = call.request;

    if (!name || !phone || !vehicle) {
      return callback({
        code: grpc.status.INVALID_ARGUMENT,
        message: 'Champs obligatoires: name, phone, vehicle'
      });
    }

    const driver = await db.createDriver({ driver_id: uuidv4(), name, phone, vehicle });
    console.log(`[Driver-gRPC] Livreur enregistré: ${driver.driver_id}`);
    callback(null, driver);
  } catch (err) {
    callback({ code: grpc.status.INTERNAL, message: err.message });
  }
}

async function getDriver(call, callback) {
  try {
    const { driver_id } = call.request;
    const driver = await db.getDriverById(driver_id);

    if (!driver) {
      return callback({
        code: grpc.status.NOT_FOUND,
        message: `Livreur ${driver_id} introuvable`
      });
    }
    callback(null, driver);
  } catch (err) {
    callback({ code: grpc.status.INTERNAL, message: err.message });
  }
}

/**
 * Met à jour la position GPS du livreur
 * Publié sur Kafka: driver.location.updated
 */
async function updateLocation(call, callback) {
  try {
    const { driver_id, latitude, longitude } = call.request;
    const driver = await db.updateDriverLocation(driver_id, latitude, longitude);

    if (!driver) {
      return callback({
        code: grpc.status.NOT_FOUND,
        message: `Livreur ${driver_id} introuvable`
      });
    }

    // Publier la mise à jour de position
    publishEvent(TOPICS.LOCATION_UPDATED, {
      driver_id,
      latitude,
      longitude,
      timestamp: new Date().toISOString()
    });

    callback(null, driver);
  } catch (err) {
    callback({ code: grpc.status.INTERNAL, message: err.message });
  }
}

async function updateAvailability(call, callback) {
  try {
    const { driver_id, available } = call.request;
    const driver = await db.updateDriverAvailability(driver_id, available);

    if (!driver) {
      return callback({
        code: grpc.status.NOT_FOUND,
        message: `Livreur ${driver_id} introuvable`
      });
    }
    callback(null, driver);
  } catch (err) {
    callback({ code: grpc.status.INTERNAL, message: err.message });
  }
}

async function getAvailableDrivers(call, callback) {
  try {
    const drivers = await db.getAvailableDrivers();
    callback(null, { drivers });
  } catch (err) {
    callback({ code: grpc.status.INTERNAL, message: err.message });
  }
}

/**
 * Assigne un livreur à une commande
 * Publié sur Kafka: delivery.assigned
 */
async function assignDriver(call, callback) {
  try {
    const { driver_id, order_id } = call.request;
    const driver = await db.assignDriverToOrder(driver_id, order_id);

    if (!driver) {
      return callback({
        code: grpc.status.NOT_FOUND,
        message: `Livreur ${driver_id} introuvable`
      });
    }

    // Publication de l'événement d'assignation
    publishEvent(TOPICS.DELIVERY_ASSIGNED, {
      order_id,
      driver_id,
      driver_name: driver.name,
      driver_phone: driver.phone,
      timestamp: new Date().toISOString()
    });

    console.log(`[Driver-gRPC] Livreur ${driver_id} assigné à la commande ${order_id}`);
    callback(null, driver);
  } catch (err) {
    callback({ code: grpc.status.INTERNAL, message: err.message });
  }
}

// ─── Handler Kafka: auto-assignation quand une commande est créée ─────────────
async function handleOrderCreated(orderData) {
  console.log(`[Driver-Kafka] Nouvelle commande reçue, recherche d'un livreur disponible...`);

  const availableDrivers = await db.getAvailableDrivers();
  if (availableDrivers.length === 0) {
    console.log('[Driver-Kafka] Aucun livreur disponible pour le moment');
    return;
  }

  // Assigner le premier livreur disponible (logique simplifiée)
  const driver = availableDrivers[0];
  await db.assignDriverToOrder(driver.driver_id, orderData.order_id);

  publishEvent(TOPICS.DELIVERY_ASSIGNED, {
    order_id: orderData.order_id,
    client_id: orderData.client_id,
    driver_id: driver.driver_id,
    driver_name: driver.name,
    driver_phone: driver.phone,
    timestamp: new Date().toISOString()
  });

  console.log(`[Driver-Kafka] Livreur ${driver.name} auto-assigné à la commande ${orderData.order_id}`);
}

// ─── Démarrage du serveur gRPC ─────────────────────────────────────────────────
async function main() {
  

  // Connecter Kafka
  await connectProducer();
  await startConsumer(handleOrderCreated);

  const server = new grpc.Server();

  server.addService(driverProto.DriverService.service, {
    registerDriver,
    getDriver,
    updateLocation,
    updateAvailability,
    getAvailableDrivers,
    assignDriver
  });

  const PORT = process.env.DRIVER_SERVICE_PORT || '50052';

  server.bindAsync(
    `0.0.0.0:${PORT}`,
    grpc.ServerCredentials.createInsecure(),
    (err, port) => {
      if (err) {
        console.error('[Driver-gRPC] Erreur démarrage:', err);
        process.exit(1);
      }
      server.start();
      console.log(`\n✅ Driver Service (gRPC) démarré sur le port ${port}`);
      console.log(`   DB: RxDB + LevelDB (drivers_rxdb/)`);
      console.log(`   Kafka Topics consommés: order.created`);
      console.log(`   Kafka Topics produits: delivery.assigned, driver.location.updated\n`);
    }
  );
}

main();