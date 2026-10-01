const mongoose = require('mongoose');

const windowSchema = new mongoose.Schema(
  {
    windowStart: { type: Date, required: true, unique: true },
    windowEnd: { type: Date, required: true },
    counts: { type: Map, of: Number },
    total: { type: Number },
  },
  { versionKey: false }
);

const EmojiWindow = mongoose.model('EmojiWindow', windowSchema, 'windows');

async function connectHistory() {
  await mongoose.connect(process.env.MONGO_URL || 'mongodb://localhost:27017/emostream');
}

async function saveWindows(windows) {
  if (windows.length === 0) return;

  const ops = windows.map((w) => {
    const inc = { total: 0 };
    for (const [emoji, n] of Object.entries(w.counts)) {
      inc[`counts.${emoji}`] = n;
      inc.total += n;
    }

    return {
      updateOne: {
        filter: { windowStart: new Date(w.windowStart) },
        update: { $inc: inc, $setOnInsert: { windowEnd: new Date(w.windowEnd) } },
        upsert: true,
      },
    };
  });

  await EmojiWindow.bulkWrite(ops);
}

async function disconnectHistory() {
  await mongoose.disconnect();
}

module.exports = { EmojiWindow, connectHistory, saveWindows, disconnectHistory };
