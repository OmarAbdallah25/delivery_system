const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');
const { v4: uuidv4 } = require('uuid');

const db = require('./db');
const { connectProducer, publishEvent, TOPICS } = require('./kafka');

// ─── Chargement du fichier .proto ──────────────────────────────────────────────
const PROTO_DIR  = process.env.PROTO_DIR || path.join(__dirname, '../proto');
const PROTO_PATH = path.join(PROTO_DIR, 'order.proto');

const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true
});

const orderProto = grpc.loadPackageDefinition(packageDefinition).order;

// ─── Implémentation des méthodes gRPC ─────────────────────────────────────────

/**
 * Crée une nouvelle commande
 * Publié sur Kafka: order.created
 */
function createOrder(call, callback) {
  try {
    const { client_id, client_name, pickup_address, delivery_address, items } = call.request;

    if (!client_id || !client_name || !pickup_address || !delivery_address) {
      return callback({
        code: grpc.status.INVALID_ARGUMENT,
        message: 'Champs obligatoires manquants: client_id, client_name, pickup_address, delivery_address'
      });
    }

    const order = db.createOrder({
      order_id: uuidv4(),
      client_id,
      client_name,
      pickup_address,
      delivery_address,
      items: items || [],
      created_at: new Date().toISOString()
    });

    // Publication de l'événement sur Kafka (asynchrone - ne bloque pas la réponse gRPC)
    publishEvent(TOPICS.ORDER_CREATED, {
      order_id: order.order_id,
      client_id: order.client_id,
      client_name: order.client_name,
      pickup_address: order.pickup_address,
      delivery_address: order.delivery_address,
      items: order.items,
      timestamp: new Date().toISOString()
    });

    console.log(`[Order-gRPC] Commande créée: ${order.order_id}`);
    callback(null, order);

  } catch (err) {
    console.error('[Order-gRPC] Erreur createOrder:', err.message);
    callback({ code: grpc.status.INTERNAL, message: err.message });
  }
}

/**
 * Récupère une commande par son ID
 */
function getOrder(call, callback) {
  try {
    const { order_id } = call.request;
    const order = db.getOrderById(order_id);

    if (!order) {
      return callback({
        code: grpc.status.NOT_FOUND,
        message: `Commande ${order_id} introuvable`
      });
    }

    callback(null, order);
  } catch (err) {
    callback({ code: grpc.status.INTERNAL, message: err.message });
  }
}

/**
 * Met à jour le statut d'une commande
 * Statuts valides: PENDING, ASSIGNED, IN_PROGRESS, DELIVERED, CANCELLED
 * Publié sur Kafka: order.status.updated
 */
function updateOrderStatus(call, callback) {
  try {
    const { order_id, status } = call.request;
    const validStatuses = ['PENDING', 'ASSIGNED', 'IN_PROGRESS', 'DELIVERED', 'CANCELLED'];

    if (!validStatuses.includes(status)) {
      return callback({
        code: grpc.status.INVALID_ARGUMENT,
        message: `Statut invalide. Valeurs acceptées: ${validStatuses.join(', ')}`
      });
    }

    const order = db.getOrderById(order_id);
    if (!order) {
      return callback({
        code: grpc.status.NOT_FOUND,
        message: `Commande ${order_id} introuvable`
      });
    }

    const updatedOrder = db.updateOrderStatus(order_id, status);

    // Publication de l'événement sur Kafka
    publishEvent(TOPICS.ORDER_STATUS_UPDATED, {
      order_id,
      client_id: updatedOrder.client_id,
      new_status: status,
      driver_id: updatedOrder.driver_id,
      timestamp: new Date().toISOString()
    });

    console.log(`[Order-gRPC] Statut mis à jour: ${order_id} → ${status}`);
    callback(null, updatedOrder);

  } catch (err) {
    callback({ code: grpc.status.INTERNAL, message: err.message });
  }
}

/**
 * Liste toutes les commandes d'un client
 */
function listOrders(call, callback) {
  try {
    const { client_id } = call.request;
    const orders = db.listOrdersByClient(client_id);
    callback(null, { orders });
  } catch (err) {
    callback({ code: grpc.status.INTERNAL, message: err.message });
  }
}

/**
 * Supprime une commande
 */
function deleteOrder(call, callback) {
  try {
    const { order_id } = call.request;
    const success = db.deleteOrder(order_id);

    if (!success) {
      return callback({
        code: grpc.status.NOT_FOUND,
        message: `Commande ${order_id} introuvable`
      });
    }

    callback(null, { success: true, message: `Commande ${order_id} supprimée` });
  } catch (err) {
    callback({ code: grpc.status.INTERNAL, message: err.message });
  }
}

// ─── Démarrage du serveur gRPC ─────────────────────────────────────────────────
async function main() {
  await connectProducer();

  const server = new grpc.Server();

  server.addService(orderProto.OrderService.service, {
    createOrder,
    getOrder,
    updateOrderStatus,
    listOrders,
    deleteOrder
  });

  const PORT = process.env.ORDER_SERVICE_PORT || '50051';

  server.bindAsync(
    `0.0.0.0:${PORT}`,
    grpc.ServerCredentials.createInsecure(),
    (err, port) => {
      if (err) {
        console.error('[Order-gRPC] Erreur démarrage:', err);
        process.exit(1);
      }
      server.start();
      console.log(`\n✅ Order Service (gRPC) démarré sur le port ${port}`);
      console.log(`   DB: SQLite3 (orders.db)`);
      console.log(`   Kafka Topics produits: order.created, order.status.updated\n`);
    }
  );
}

main();