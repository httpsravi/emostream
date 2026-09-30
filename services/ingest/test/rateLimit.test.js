const { randomUUID } = require('crypto');
const redis = require('../src/redis');
const { acquireSlidingWindow, slidingWindowLimiter } = require('../src/rateLimit');

const LIMIT = 5;
const WINDOW_MS = 1000;
const usedKeys = [];

function newUser() {
  const userId = `test-${randomUUID()}`;
  usedKeys.push(`rl:sliding:${userId}`);
  return userId;
}

const acquire = (userId, now) => acquireSlidingWindow({ userId, limit: LIMIT, windowMs: WINDOW_MS, now });

async function acquireAt(userId, times) {
  const results = [];
  for (const t of times) results.push(await acquire(userId, t));
  return results;
}

beforeAll(async () => {
  await redis.connect();
});

afterAll(async () => {
  if (usedKeys.length) await redis.del(...usedKeys);
  await redis.quit();
});

test('first 5 requests in one second are accepted, the 6th is rejected', async () => {
  const user = newUser();
  const results = await acquireAt(user, [1000, 1100, 1200, 1300, 1400, 1500]);
  expect(results).toEqual([true, true, true, true, true, false]);
});

test('identical timestamps are all stored, not deduplicated', async () => {
  const user = newUser();
  const results = await acquireAt(user, [5000, 5000, 5000, 5000, 5000, 5000]);
  expect(results).toEqual([true, true, true, true, true, false]);
  expect(await redis.zcard(`rl:sliding:${user}`)).toBe(5);
});

test('rejected requests do not extend the blocking period', async () => {
  const user = newUser();
  await acquireAt(user, [0, 0, 0, 0, 0]);

  const rejected = await acquireAt(user, [100, 500, 900, 999]);
  expect(rejected).toEqual([false, false, false, false]);
  expect(await redis.zcard(`rl:sliding:${user}`)).toBe(5);

  expect(await acquire(user, 1000)).toBe(true);
});

test('requests become eligible again as older accepted requests leave the window', async () => {
  const user = newUser();
  await acquireAt(user, [0, 100, 200, 300, 400]);

  expect(await acquire(user, 999)).toBe(false);
  expect(await acquire(user, 1000)).toBe(true);
  expect(await acquire(user, 1001)).toBe(false);
  expect(await acquire(user, 1100)).toBe(true);
  expect(await acquire(user, 1150)).toBe(false);
});

test('5 just before a second boundary and 5 just after cannot bypass the limit', async () => {
  const user = newUser();
  const before = await acquireAt(user, [9990, 9992, 9994, 9996, 9998]);
  const after = await acquireAt(user, [10000, 10001, 10002, 10003, 10004]);

  expect(before).toEqual([true, true, true, true, true]);
  expect(after).toEqual([false, false, false, false, false]);
  expect(await acquire(user, 10990)).toBe(true);
});

test('concurrent requests from one user never exceed the limit (Redis server time)', async () => {
  const user = newUser();
  const results = await Promise.all(Array.from({ length: 50 }, () => acquire(user)));
  expect(results.filter(Boolean)).toHaveLength(LIMIT);
});

test('concurrent requests with the same timestamp never exceed the limit', async () => {
  const user = newUser();
  const results = await Promise.all(Array.from({ length: 50 }, () => acquire(user, 20000)));
  expect(results.filter(Boolean)).toHaveLength(LIMIT);
});

test('separate users have independent limits', async () => {
  const alice = newUser();
  const bob = newUser();

  expect(await acquireAt(alice, [0, 0, 0, 0, 0, 0])).toEqual([true, true, true, true, true, false]);
  expect(await acquireAt(bob, [0, 0, 0, 0, 0, 0])).toEqual([true, true, true, true, true, false]);
});

test('key gets a TTL no longer than the window', async () => {
  const user = newUser();
  await acquire(user);
  const ttl = await redis.pttl(`rl:sliding:${user}`);
  expect(ttl).toBeGreaterThan(0);
  expect(ttl).toBeLessThanOrEqual(WINDOW_MS);
});

describe('slidingWindowLimiter middleware', () => {
  const logger = { error: jest.fn() };
  const limiter = slidingWindowLimiter({ limit: LIMIT, windowSec: 1, logger });

  function call(userId) {
    return new Promise((resolve) => {
      const res = {
        status(code) {
          return { json: (body) => resolve({ code, body }) };
        },
      };
      limiter({ body: { userId } }, res, () => resolve({ code: 'next' }));
    });
  }

  test('passes the first 5 to the handler and answers 429 after that', async () => {
    const user = newUser();
    const results = [];
    for (let i = 0; i < 6; i++) results.push(await call(user));

    expect(results.slice(0, 5).every((r) => r.code === 'next')).toBe(true);
    expect(results[5]).toEqual({ code: 429, body: { error: 'too many requests' } });
  });
});
