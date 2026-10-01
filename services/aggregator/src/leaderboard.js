const Redis = require('ioredis');

const LEADERBOARD_KEY = 'leaderboard:emoji';

const redis = new Redis(process.env.REDIS_URL || 'redis://localhost:6379', {
  lazyConnect: true,
});

async function addToLeaderboard(windows) {
  if (windows.length === 0) return;

  const pipeline = redis.pipeline();
  for (const w of windows) {
    for (const [emoji, n] of Object.entries(w.counts)) {
      pipeline.zincrby(LEADERBOARD_KEY, n, emoji);
    }
  }

  const results = await pipeline.exec();
  const failed = results.find(([err]) => err);
  if (failed) throw failed[0];
}

module.exports = { redis, LEADERBOARD_KEY, addToLeaderboard };
