import express from "express";
import { clientStart, pushToTaskQueue } from "../queue/redisQueue.js";

const app = express();
const port = 3000;

app.use(express.json());

try {
  await clientStart();
  console.log("Redis server running in api...");
} catch (error) {
  console.error(`Failed to connected to with redis client`);
  process.exit(1);
}

app.get("/", (req, res) => {
  res.status(201).json({
    msg: `Home route`,
  });
});

app.post("/api/crypto/update", async (req, res) => {
  try {
    const { type, payload } = req.body;
    const { symbol, interval } = payload;

    if (type !== "crypto_price_update" || !payload || !symbol || !interval) {
      return res.status(403).json({
        msg: `Please provide required details.`,
      });
    }

    const task = {
      type: type,
      payload: {
        symbol: symbol.toUpperCase(),
        interval: interval,
      },
    };

    await pushToTaskQueue(task);

    return res.status(201).json({
      msg: `Pushed to Queues.`,
      task,
    });
  } catch (err) {
    console.error(`Error while pushing to queue from api: ${err}`);
    return res.status(500).json({ msg: `Internal server error.` });
  }
});

app.listen(port, () => {
  console.log(`api server running on ${port}`);
});
