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

module.exports = { fixedWindowLimiter };
