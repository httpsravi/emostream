const { createAggregator } = require('../src/window');

const make = () => createAggregator({ windowMs: 2000, graceMs: 500 });

test('counts each emoji inside one window', () => {
  const agg = make();
  agg.add({ emoji: '🔥', ts: 100 });
  agg.add({ emoji: '🔥', ts: 900 });
  agg.add({ emoji: '😂', ts: 1500 });

  expect(agg.flush(2500)).toEqual([
    { windowStart: 0, windowEnd: 2000, counts: { '🔥': 2, '😂': 1 } },
  ]);
});

test('an event exactly on the boundary belongs to the next window', () => {
  const agg = make();
  agg.add({ emoji: '🔥', ts: 1999 });
  agg.add({ emoji: '🔥', ts: 2000 });

  expect(agg.flush(10000)).toEqual([
    { windowStart: 0, windowEnd: 2000, counts: { '🔥': 1 } },
    { windowStart: 2000, windowEnd: 4000, counts: { '🔥': 1 } },
  ]);
});

test('a window is not flushed until its end plus the grace period', () => {
  const agg = make();
  agg.add({ emoji: '🔥', ts: 100 });

  expect(agg.flush(2000)).toEqual([]);
  expect(agg.flush(2499)).toEqual([]);
  expect(agg.flush(2500)).toHaveLength(1);
});

test('a slightly late event inside the grace period is still counted', () => {
  const agg = make();
  agg.add({ emoji: '🔥', ts: 100 });
  agg.add({ emoji: '🔥', ts: 1900 });

  expect(agg.flush(2400)).toEqual([]);
  expect(agg.flush(2500)[0].counts).toEqual({ '🔥': 2 });
});

test('events for an already flushed window are dropped and counted as late', () => {
  const agg = make();
  agg.add({ emoji: '🔥', ts: 100 });
  agg.flush(2500);

  expect(agg.add({ emoji: '🔥', ts: 1800 })).toBe(false);
  expect(agg.stats().lateEvents).toBe(1);
  expect(agg.flush(99999)).toEqual([]);
});

test('flush returns only closed windows and keeps open ones', () => {
  const agg = make();
  agg.add({ emoji: '🔥', ts: 100 });
  agg.add({ emoji: '😂', ts: 2100 });

  expect(agg.flush(2500).map((w) => w.windowStart)).toEqual([0]);
  expect(agg.stats().openWindows).toBe(1);
  expect(agg.flush(4500).map((w) => w.windowStart)).toEqual([2000]);
  expect(agg.stats().openWindows).toBe(0);
});

test('windows come out oldest first even if events arrive out of order', () => {
  const agg = make();
  agg.add({ emoji: '🔥', ts: 4100 });
  agg.add({ emoji: '🔥', ts: 100 });
  agg.add({ emoji: '🔥', ts: 2100 });

  expect(agg.flush(10000).map((w) => w.windowStart)).toEqual([0, 2000, 4000]);
});

test('flush with no events returns an empty list', () => {
  expect(make().flush(10000)).toEqual([]);
});
