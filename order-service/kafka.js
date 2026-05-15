const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'order-service',
  brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
  retry: { initialRetryTime: 3000, retries: 5 }
});

const producer = kafka.producer();
let connected = false;

async function connectProducer() {
  try {
    await producer.connect();
    connected = true;
    console.log('[Order-Kafka] Producteur connecté');
  } catch (err) {
    console.error('[Order-Kafka] Erreur connexion producteur:', err.message);
  }
}

// Publie un événement sur un topic Kafka
async function publishEvent(topic, eventData) {
  if (!connected) {
    console.warn('[Order-Kafka] Producteur non connecté, message ignoré');
    return;
  }
  try {
    await producer.send({
      topic,
      messages: [
        {
          key: eventData.order_id || String(Date.now()),
          value: JSON.stringify(eventData)
        }
      ]
    });
    console.log(`[Order-Kafka] Événement publié sur "${topic}":`, eventData);
  } catch (err) {
    console.error(`[Order-Kafka] Erreur publication sur "${topic}":`, err.message);
  }
}

// Topics utilisés par ce microservice
const TOPICS = {
  ORDER_CREATED: 'order.created',         // Producteur
  ORDER_STATUS_UPDATED: 'order.status.updated' // Producteur
};

module.exports = { connectProducer, publishEvent, TOPICS };