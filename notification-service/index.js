const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');
const { v4: uuidv4 } = require('uuid');

const db = require('./db');
const { startConsumer, TOPICS } = require('./kafka');

// ─── Chargement du fichier .proto ──────────────────────────────────────────────
const PROTO_DIR  = process.env.PROTO_DIR || path.join(__dirname, '../proto');
const PROTO_PATH = path.join(PROTO_DIR, 'notification.proto');

const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true
});

const notifProto = grpc.loadPackageDefinition(packageDefinition).notification;

// ─── Implémentation des méthodes gRPC ─────────────────────────────────────────

/**
 * Envoie et enregistre une notification
 */
function sendNotification(call, callback) {
  try {
    const { recipient_id, recipient_type, message, order_id, type } = call.request;

    if (!recipient_id || !message || !type) {
      return callback({
        code: grpc.status.INVALID_ARGUMENT,
        message: 'Champs obligatoires: recipient_id, message, type'
      });
    }

    const notif = db.saveNotification({
      notification_id: uuidv4(),
      recipient_id,
      recipient_type: recipient_type || 'client',
      message,
      order_id,
      type
    });

    // Simulation d'envoi (SMS/Push)
    console.log(`[Notif-gRPC] 📱 Notification envoyée à ${recipient_type} ${recipient_id}: "${message}"`);
    callback(null, notif);

  } catch (err) {
    callback({ code: grpc.status.INTERNAL, message: err.message });
  }
}

/**
 * Récupère toutes les notifications d'un destinataire
 */
function getNotifications(call, callback) {
  try {
    const { recipient_id } = call.request;
    const notifications = db.getNotificationsByRecipient(recipient_id);
    callback(null, { notifications });
  } catch (err) {
    callback({ code: grpc.status.INTERNAL, message: err.message });
  }
}

// ─── Handler Kafka: traitement des événements métier ─────────────────────────
function handleKafkaEvent(topic, data) {
  if (topic === TOPICS.DELIVERY_ASSIGNED) {
    // Un livreur a été assigné → notifier le client ET le livreur
    db.saveNotification({
      notification_id: uuidv4(),
      recipient_id: data.client_id || 'unknown',
      recipient_type: 'client',
      message: `Votre commande ${data.order_id} a été assignée au livreur ${data.driver_name} (${data.driver_phone})`,
      order_id: data.order_id,
      type: 'DRIVER_ASSIGNED'
    });

    db.saveNotification({
      notification_id: uuidv4(),
      recipient_id: data.driver_id,
      recipient_type: 'driver',
      message: `Nouvelle course assignée: commande ${data.order_id}`,
      order_id: data.order_id,
      type: 'DRIVER_ASSIGNED'
    });

    console.log(`[Notif-Kafka] 📱 Notifications "DRIVER_ASSIGNED" créées pour commande ${data.order_id}`);
  }

  if (topic === TOPICS.ORDER_STATUS_UPDATED) {
    // Statut mis à jour → notifier le client
    const statusMessages = {
      ASSIGNED:    'Votre commande a été prise en charge par un livreur',
      IN_PROGRESS: 'Votre livreur est en route !',
      DELIVERED:   'Votre commande a été livrée. Merci !',
      CANCELLED:   'Votre commande a été annulée.'
    };

    const message = statusMessages[data.new_status] || `Statut de votre commande: ${data.new_status}`;

    db.saveNotification({
      notification_id: uuidv4(),
      recipient_id: data.client_id || 'unknown',
      recipient_type: 'client',
      message,
      order_id: data.order_id,
      type: 'STATUS_UPDATE'
    });

    console.log(`[Notif-Kafka] 📱 Notification "STATUS_UPDATE" → ${data.new_status} pour commande ${data.order_id}`);
  }
}

// ─── Démarrage ────────────────────────────────────────────────────────────────
async function main() {
  await startConsumer(handleKafkaEvent);

  const server = new grpc.Server();

  server.addService(notifProto.NotificationService.service, {
    sendNotification,
    getNotifications
  });

  const PORT = process.env.NOTIF_SERVICE_PORT || '50053';

  server.bindAsync(
    `0.0.0.0:${PORT}`,
    grpc.ServerCredentials.createInsecure(),
    (err, port) => {
      if (err) {
        console.error('[Notif-gRPC] Erreur démarrage:', err);
        process.exit(1);
      }
      server.start();
      console.log(`\n✅ Notification Service (gRPC) démarré sur le port ${port}`);
      console.log(`   DB: SQLite3 (notifications.db)`);
      console.log(`   Kafka Topics consommés: delivery.assigned, order.status.updated\n`);
    }
  );
}

main();