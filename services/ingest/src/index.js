// Ingest API: receives emoji reactions over HTTP and writes them to Kafka.
const express = require('express');
const pino = require('pino');
const { connectProducer, sendEmoji, disconnectProducer } = require('./kafka');
const redis = require('./redis');
const { fixedWindowLimiter } = require('./rateLimit');

const logger = pino();
const PORT = process.env.PORT || 3000;

// Only these emojis are accepted, so random strings never become Kafka keys.
const ALLOWED_EMOJIS = ['😂', '🔥', '❤️', '👍', '😮'];

const app = express();
app.use(express.json());

app.get('/health', (req, res) => res.json({ ok: true }));

const limiter = fixedWindowLimiter({ limit: 5, windowSec: 1, logger });

app.post('/emoji', limiter, async (req, res) => {
  const { userId, emoji } = req.body || {};

  if (typeof userId !== 'string' || userId.trim() === '') {
    return res.status(400).json({ error: 'userId is required' });
  }
  if (!ALLOWED_EMOJIS.includes(emoji)) {
    return res.status(400).json({ error: 'emoji not allowed', allowed: ALLOWED_EMOJIS });
  }

  try {
    await sendEmoji({ emoji, userId, ts: Date.now() }); // server time, clients can't be trusted
    // 202 = accepted for processing (it's in Kafka, not counted yet)
    res.status(202).json({ status: 'queued' });
  } catch (err) {
    logger.error({ err }, 'kafka send failed');
    res.status(503).json({ error: 'could not queue event' });
  }
});

async function start() {
  await connectProducer();
  logger.info('kafka producer connected');
  await redis.connect();
  logger.info('redis connected');

  const server = app.listen(PORT, () => logger.info(`ingest listening on :${PORT}`));

  // Graceful shutdown: stop taking new requests, then close Kafka, then exit.
  const shutdown = async (signal) => {
    logger.info(`${signal} received, shutting down`);
    server.close(async () => {
      await disconnectProducer();
      await redis.quit();
      logger.info('shutdown complete');
      process.exit(0);
    });
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

start().catch((err) => {
  logger.error({ err }, 'failed to start');
  process.exit(1);
});
