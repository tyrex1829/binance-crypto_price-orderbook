import express from "express";
import { client, pushToTaskQueue } from "../queue/redisQueue";

const app = express();
const port = 3000;

app.use(express.json());

app.get("/", (req, res) => {
  res.status(201).json({
    msg: `Home route`,
  });
});

app.post("/api/crypto/update", async (req, res) => {
  try {
    const { symbol, interval } = req.body;

    if (!symbol || !interval) {
      return res.status(403).json({
        msg: `Please provide required details.`,
      });
    }

    const task = {
      stockType: "crypto_price_update",
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
    console.error(`Error while pushing to queue.`);
  }
});

app.listen(port, () => {
  console.log(`api server running on 3000`);
});
