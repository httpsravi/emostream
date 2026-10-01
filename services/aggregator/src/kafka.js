const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'aggregator',
  brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
});

const consumer = kafka.consumer({ groupId: 'aggregator' });
const producer = kafka.producer();

async function publishWindows(windows) {
  if (windows.length === 0) return;

  await producer.send({
    topic: 'aggregated-emoji',
    messages: windows.map((w) => ({
      key: String(w.windowStart),
      value: JSON.stringify(w),
    })),
  });
}

module.exports = { consumer, producer, publishWindows };
