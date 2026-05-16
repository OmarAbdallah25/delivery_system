const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'notification-service',
  brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
  retry: { initialRetryTime: 3000, retries: 5 }
});

const consumer = kafka.consumer({ groupId: 'notification-service-group' });

const TOPICS = {
  DELIVERY_ASSIGNED:   'delivery.assigned',     // Consommateur
  ORDER_STATUS_UPDATED:'order.status.updated'   // Consommateur
};

// Callback injecté depuis index.js
let onEventCallback = null;

async function startConsumer(handleEvent) {
  onEventCallback = handleEvent;

  try {
    await consumer.connect();
    // S'abonner aux deux topics
    await consumer.subscribe({ topics: [TOPICS.DELIVERY_ASSIGNED, TOPICS.ORDER_STATUS_UPDATED], fromBeginning: false });

    await consumer.run({
      eachMessage: async ({ topic, message }) => {
        const data = JSON.parse(message.value.toString());
        console.log(`[Notif-Kafka] Message reçu sur "${topic}":`, data);

        if (onEventCallback) {
          await onEventCallback(topic, data);
        }
      }
    });

    console.log('[Notif-Kafka] Consommateur démarré - écoute: delivery.assigned, order.status.updated');
  } catch (err) {
    console.error('[Notif-Kafka] Erreur consommateur:', err.message);
  }
}

module.exports = { startConsumer, TOPICS };