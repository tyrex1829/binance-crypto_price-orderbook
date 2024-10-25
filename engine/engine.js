import axios from "axios";
import { popFromTaskQueue, pushToDoneTaskQueue } from "../queue/redisQueue";

/*
 * Pop from queue.
 * Do the task.
 * push to doneTaskQueue.
 */

// to get crypto price with rest-api of binance
async function getCryptoPrice(symbolOfCrypto) {
  try {
    const response = await axios.get(
      `https://api.binance.com/api/v3/ticker/price?symbol=${symbol}`
    );
    return response.data.price;
  } catch (error) {
    console.error(`Error fetching the stock price of ${symbol}: ${error}`);
  }
}

async function doTheTask(task) {
  try {
    const { symbol } = task.payload;

    console.log(`Getting price of ${symbol}`);
    const cryptoPrice = await getCryptoPrice(symbol);
    console.log(`Stock price of ${symbol} is ${stockPrice}`);

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

function startEngine() {
  popFromTaskQueue(async (tasks) => {
    await doTheTask(tasks);
  });
}

startEngine();
