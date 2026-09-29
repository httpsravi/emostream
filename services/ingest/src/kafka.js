// Kafka producer for the ingest service.
const { Kafka } = require('kafkajs');

const TOPIC = 'emoji-events';

// WHY clientId: shows up in broker logs / Kafka UI so we know which app is connected.
// WHY env var: on your PC it's localhost:9092; inside Docker it will be kafka:29092 (the INTERNAL door).
const kafka = new Kafka({
  clientId: 'ingest',
  brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
});

// WHY one producer for the whole app: it keeps one TCP connection to the broker and reuses it.
// Creating a producer per request would reconnect every time (slow).
const producer = kafka.producer();

async function connectProducer() {
  // WHY await before listen(): if Kafka is down we want the app to fail at startup,
  // not accept HTTP requests it can't deliver.
  await producer.connect();
}

async function sendEmoji({ emoji, userId, ts }) {
  // WHY key = emoji: Kafka hashes the key to pick a partition, so every 🔥 lands in the
  // same partition -> one aggregator instance sees the full 🔥 count. (Tradeoff: hot partitions.)
  // WHY acks default (-1 = all in-sync replicas): send() resolves only after the broker
  // has stored the message, so a 202 really means "it's safely in Kafka".
  await producer.send({
    topic: TOPIC,
    messages: [
      {
        key: emoji,
        value: JSON.stringify({ emoji, userId, ts }), // Kafka stores bytes, so we serialize to JSON
      },
    ],
  });
}

async function disconnectProducer() {
  // WHY: flushes in-flight sends and closes the connection cleanly on shutdown.
  await producer.disconnect();
}

module.exports = { connectProducer, sendEmoji, disconnectProducer };
