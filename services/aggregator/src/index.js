const pino = require('pino');
const { consumer, producer, publishWindows } = require('./kafka');
const { createAggregator } = require('./window');
const { connectHistory, saveWindows, disconnectHistory } = require('./history');
const { redis, addToLeaderboard } = require('./leaderboard');

const logger = pino();
const FLUSH_EVERY_MS = 500;

const agg = createAggregator({ windowMs: 2000, graceMs: 500 });

function parseEvent(message) {
  try {
    const event = JSON.parse(message.value.toString());
    if (typeof event.emoji !== 'string' || typeof event.ts !== 'number') return null;
    return event;
  } catch {
    return null;
  }
}

const SINKS = ['kafka', 'mongo', 'leaderboard'];

async function flushAndPublish(now) {
  const closed = agg.flush(now);
  if (closed.length === 0) return;

  const results = await Promise.allSettled([
    publishWindows(closed),
    saveWindows(closed),
    addToLeaderboard(closed),
  ]);

  results.forEach((r, i) => {
    if (r.status === 'rejected') logger.error({ err: r.reason, sink: SINKS[i] }, 'save failed');
  });
  for (const w of closed) {
    logger.info({ windowStart: w.windowStart, counts: w.counts }, 'window published');
  }
}

async function start() {
  await Promise.all([producer.connect(), connectHistory(), redis.connect()]);
  logger.info('kafka producer, mongo and redis connected');
  await consumer.connect();
  await consumer.subscribe({ topic: 'emoji-events', fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ partition, message }) => {
      const event = parseEvent(message);
      if (!event) {
        logger.warn({ partition, offset: message.offset }, 'skipping bad message');
        return;
      }
      if (!agg.add(event)) {
        logger.warn({ emoji: event.emoji, ts: event.ts }, 'late event dropped');
      }
    },
  });
  logger.info('aggregator running');

  const timer = setInterval(() => {
    flushAndPublish(Date.now()).catch((err) => logger.error({ err }, 'publish failed'));
  }, FLUSH_EVERY_MS);

  const shutdown = async (signal) => {
    logger.info(`${signal} received, shutting down`);
    clearInterval(timer);
    await consumer.disconnect();
    await flushAndPublish(Infinity);
    await producer.disconnect();
    await disconnectHistory();
    await redis.quit();
    logger.info({ stats: agg.stats() }, 'shutdown complete');
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

start().catch((err) => {
  logger.error({ err }, 'failed to start');
  process.exit(1);
});
