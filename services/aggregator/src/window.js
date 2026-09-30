function createAggregator({ windowMs = 2000, graceMs = 500 } = {}) {
  const windows = new Map();
  let lastFlushedEnd = 0;
  let lateEvents = 0;

  function add({ emoji, ts }) {
    const start = Math.floor(ts / windowMs) * windowMs;

    if (start + windowMs <= lastFlushedEnd) {
      lateEvents++;
      return false;
    }

    if (!windows.has(start)) windows.set(start, new Map());
    const counts = windows.get(start);
    counts.set(emoji, (counts.get(emoji) || 0) + 1);
    return true;
  }

  function flush(now) {
    const closed = [];

    for (const [start, counts] of windows) {
      const end = start + windowMs;
      if (end + graceMs <= now) {
        closed.push({ windowStart: start, windowEnd: end, counts: Object.fromEntries(counts) });
        windows.delete(start);
        lastFlushedEnd = Math.max(lastFlushedEnd, end);
      }
    }

    return closed.sort((a, b) => a.windowStart - b.windowStart);
  }

  function stats() {
    return { openWindows: windows.size, lateEvents };
  }

  return { add, flush, stats };
}

module.exports = { createAggregator };
