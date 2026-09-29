const redis = require('./redis');

function fixedWindowLimiter({ limit, windowSec, logger }) {
  return async (req, res, next) => {
    const userId = req.body && req.body.userId;
    if (!userId) return next();

    const window = Math.floor(Date.now() / 1000 / windowSec);
    const key = `rl:fixed:${userId}:${window}`;

    try {
      const [[, count]] = await redis.multi().incr(key).expire(key, windowSec).exec();

      if (count > limit) {
        return res.status(429).json({ error: 'too many requests' });
      }
      next();
    } catch (err) {
      logger.error({ err }, 'rate limiter failed, allowing request');
      next();
    }
  };
}

function slidingWindowLimiter({ limit, windowSec, logger }) {
  const windowMs = windowSec * 1000;

  return async (req, res, next) => {
    const userId = req.body && req.body.userId;
    if (!userId) return next();

    const now = Date.now();
    const key = `rl:sliding:${userId}`;
    const member = `${now}-${Math.random()}`;

    try {
      const results = await redis
        .multi()
        .zremrangebyscore(key, 0, now - windowMs)
        .zadd(key, now, member)
        .zcard(key)
        .pexpire(key, windowMs)
        .exec();
      const count = results[2][1];

      if (count > limit) {
        return res.status(429).json({ error: 'too many requests' });
      }
      next();
    } catch (err) {
      logger.error({ err }, 'rate limiter failed, allowing request');
      next();
    }
  };
}

module.exports = { fixedWindowLimiter, slidingWindowLimiter };
