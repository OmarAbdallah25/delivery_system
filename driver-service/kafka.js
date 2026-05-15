const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'driver-service',
  brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
  retry: { initialRetryTime: 3000, retries: 5 }
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'driver-service-group' });

// Topics utilisés par ce microservice
const TOPICS = {
  ORDER_CREATED:      'order.created',       // Consommateur
  DELIVERY_ASSIGNED:  'delivery.assigned',   // Producteur
  LOCATION_UPDATED:   'driver.location.updated' // Producteur
};

// Callback appelé quand une nouvelle commande est détectée
let onOrderCreatedCallback = null;

async function connectProducer() {
  try {
    await producer.connect();
    console.log('[Driver-Kafka] Producteur connecté');
  } catch (err) {
    console.error('[Driver-Kafka] Erreur producteur:', err.message);
  }
}

/**
 * Lance le consommateur Kafka
 * Écoute "order.created" et appelle le callback pour assigner un livreur
 */
async function startConsumer(handleOrderCreated) {
  try {
    onOrderCreatedCallback = handleOrderCreated;
    await consumer.connect();
    await consumer.subscribe({ topic: TOPICS.ORDER_CREATED, fromBeginning: false });

    await consumer.run({
      eachMessage: async ({ topic, message }) => {
        const data = JSON.parse(message.value.toString());
        console.log(`[Driver-Kafka] Message reçu sur "${topic}":`, data);

        if (topic === TOPICS.ORDER_CREATED && onOrderCreatedCallback) {
          await onOrderCreatedCallback(data);
        }
      }
    });

    console.log('[Driver-Kafka] Consommateur démarré - écoute: order.created');
  } catch (err) {
    console.error('[Driver-Kafka] Erreur consommateur:', err.message);
  }
}

async function publishEvent(topic, eventData) {
  try {
    await producer.send({
      topic,
      messages: [{ key: eventData.order_id || String(Date.now()), value: JSON.stringify(eventData) }]
    });
    console.log(`[Driver-Kafka] Événement publié sur "${topic}":`, eventData);
  } catch (err) {
    console.error(`[Driver-Kafka] Erreur publication:`, err.message);
  }
}

module.exports = { connectProducer, startConsumer, publishEvent, TOPICS };