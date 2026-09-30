const TARGET = process.env.TARGET || 'http://localhost:3000/emoji';
const USERS = Number(process.env.USERS || 20);
const MIN_DELAY_MS = Number(process.env.MIN_DELAY_MS || 100);
const MAX_DELAY_MS = Number(process.env.MAX_DELAY_MS || 500);

const EMOJIS = ['😂', '🔥', '❤️', '👍', '😮'];

const stats = { sent: 0, accepted: 0, limited: 0, errors: 0 };
const totals = { sent: 0, accepted: 0, limited: 0, errors: 0 };
let running = true;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const pick = (list) => list[Math.floor(Math.random() * list.length)];
const randomDelay = () => MIN_DELAY_MS + Math.random() * (MAX_DELAY_MS - MIN_DELAY_MS);

function record(field) {
  stats[field]++;
  totals[field]++;
}

async function sendOne(userId) {
  record('sent');
  try {
    const res = await fetch(TARGET, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ userId, emoji: pick(EMOJIS) }),
    });
    if (res.status === 202) record('accepted');
    else if (res.status === 429) record('limited');
    else record('errors');
  } catch {
    record('errors');
  }
}

async function runUser(userId) {
  while (running) {
    await sendOne(userId);
    await sleep(randomDelay());
  }
}

const ticker = setInterval(() => {
  console.log(
    `sent: ${stats.sent} | 202: ${stats.accepted} | 429: ${stats.limited} | errors: ${stats.errors}`
  );
  stats.sent = stats.accepted = stats.limited = stats.errors = 0;
}, 1000);

process.on('SIGINT', () => {
  running = false;
  clearInterval(ticker);
  console.log('\nTOTAL', totals);
  process.exit(0);
});

console.log(`${USERS} users → ${TARGET} (delay ${MIN_DELAY_MS}-${MAX_DELAY_MS}ms)`);
for (let i = 1; i <= USERS; i++) {
  runUser(`user-${i}`);
}
