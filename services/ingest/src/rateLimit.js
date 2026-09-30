const { randomUUID } = require('crypto');
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

redis.defineCommand('slidingWindowAcquire', {
  numberOfKeys: 1,
  lua: `
    local key = KEYS[1]
    local limit = tonumber(ARGV[1])
    local windowMs = tonumber(ARGV[2])
    local member = ARGV[3]
    local now = tonumber(ARGV[4])

    if not now then
      local t = redis.call('TIME')
      now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
    end

    redis.call('ZREMRANGEBYSCORE', key, '-inf', now - windowMs)

    if redis.call('ZCARD', key) >= limit then
      return 0
    end

    redis.call('ZADD', key, now, member)
    redis.call('PEXPIRE', key, windowMs)
    return 1
  `,
});

async function acquireSlidingWindow({ userId, limit, windowMs, now }) {
  const args = [`rl:sliding:${userId}`, limit, windowMs, randomUUID()];
  if (now !== undefined) args.push(now);
  const allowed = await redis.slidingWindowAcquire(...args);
  return allowed === 1;
}

function slidingWindowLimiter({ limit, windowSec, logger }) {
  const windowMs = windowSec * 1000;

  return async (req, res, next) => {
    const userId = req.body && req.body.userId;
    if (!userId) return next();

    try {
      const allowed = await acquireSlidingWindow({ userId, limit, windowMs });
      if (!allowed) {
        return res.status(429).json({ error: 'too many requests' });
      }
      next();
    } catch (err) {
      logger.error({ err }, 'rate limiter failed, allowing request');
      next();
    }
  };
}

module.exports = { fixedWindowLimiter, slidingWindowLimiter, acquireSlidingWindow };
