import axios from "axios";
import {
  clientStart,
  popFromTaskQueue,
  pushToDoneTaskQueue,
} from "../queue/redisQueue.js";

/*
 * Pop from queue.
 * Do the task.
 * push to doneTaskQueue.
 */

// to get crypto price with rest-api of binance
async function getCryptoPrice(symbolOfCrypto) {
  try {
    const response = await axios.get(
      `https://api.binance.com/api/v3/ticker/price?symbol=${symbolOfCrypto}`
    );
    return response.data.price;
  } catch (error) {
    console.error(
      `Error fetching the stock price of ${symbolOfCrypto}: ${error}`
    );
    return null;
  }
}

async function doTheTask(task) {
  try {
    console.log(`Starting to process the task: ${JSON.stringify(task)}`);
    const { symbol } = task.payload;

    console.log(`Getting price of ${symbol}`);
    const cryptoPrice = await getCryptoPrice(symbol);

    if (!cryptoPrice) {
      console.error(`Failed to get price of ${symbol}, skipping task.`);
      return;
    }

    console.log(`Crypto price of ${symbol} is ${cryptoPrice}`);

    const doneTask = {
      symbol: symbol,
      stockPrice: cryptoPrice,
      completedAt: new Date().toISOString(),
    };

    await pushToDoneTaskQueue(doneTask);
    console.log(`Successfully performed task and pushed to done queue`);
  } catch (error) {
    console.error(`Error while performing the task`);
  }
}

async function startEngine() {
  console.log(`Connecting to Redis and starting engine...`);
  console.log(`Start engine process...`);

  await popFromTaskQueue(async (task) => {
    try {
      console.log(`Task fetched from TaskQueue: ${JSON.stringify(task)}`);
      console.log(`Processing a new task...`);
      await doTheTask(task);
      console.log(`Finished processing task: ${JSON.stringify(task)}`);
    } catch (error) {
      console.error(`Error in startEngine: ${error.message}`);
    }
  });
}

await clientStart().catch((err) => {
  console.error("Failed to connect to Redis:", err);
  process.exit(1);
});
console.log(`Redis server running...`);

startEngine();
