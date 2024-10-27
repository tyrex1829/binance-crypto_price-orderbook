import axios from "axios";
import {
  clientStart,
  popFromTaskQueue,
  pushToDoneTaskQueue,
} from "../queue/redisQueue.js";
import { WebSocket } from "ws";

/*
 * Pop from queue.
 * Do the task.
 * push to doneTaskQueue.
 */

// to get crypto price with rest-api of binance
async function getCryptoPrice(task, reconnectDelay = 3000) {
  const { symbol } = task.payload;
  const wsUrl = `wss://stream.binance.com:9443/ws/${symbol.toLowerCase()}@trade`;
  console.log(`Connecting to WebSocket URL: ${wsUrl}`);
  const ws = new WebSocket(wsUrl);

  ws.on("open", () => {
    console.log("Successfully connected to wss api");
    reconnectDelay = 3000;
  });

  // let lastPrice = null;
  ws.on("message", async (data) => {
    const trade = JSON.parse(data);
    let cryptoPrice = parseFloat(trade.p);
    // if (lastPrice === null || cryptoPrice !== lastPrice) {
    // lastPrice = cryptoPrice;
    let doneTask = {
      symbol: symbol,
      stockPrice: cryptoPrice,
      completedAt: new Date().toISOString(),
    };
    await pushToDoneTaskQueue(doneTask);
    // } else {
    //   console.log(`price didn't changed`);
    // }
    console.log(`Real-time price of ${symbol}: ${cryptoPrice}`);
  });

  ws.on("error", (error) =>
    console.error(`Error getting price of ${symbol}: ${error}`)
  );

  ws.on("close", () => {
    console.log(`Connection closed with wss api for ${symbol} Reconnecting...`);
    setTimeout(() => {
      getCryptoPrice(task, Math.min(reconnectDelay * 2, 6000));
    }, reconnectDelay);
  });
}

// async function doTheTask(task) {
//   try {
//     console.log(`Starting to process the task: ${JSON.stringify(task)}`);
//     const { symbol } = task.payload;

//     console.log(`Getting price of ${symbol}`);
//     // const cryptoPrice = await getCryptoPrice(symbol);
//     getCryptoPrice(symbol);

//     const doneTask = {
//       symbol: symbol,
//       stockPrice: cryptoPrice,
//       completedAt: new Date().toISOString(),
//     };

//     await pushToDoneTaskQueue(doneTask);
//     console.log(
//       `Successfully performed task and pushed to done queue from engine`
//     );
//   } catch (error) {
//     console.error(`Error while performing the task in engine`);
//   }
// }

async function startEngine() {
  console.log(`Connecting to Redis and starting engine...`);
  console.log(`Start engine process...`);

  await popFromTaskQueue(async (task) => {
    try {
      console.log(`Task fetched from TaskQueue: ${JSON.stringify(task)}`);
      console.log(`Processing a new task...`);
      await getCryptoPrice(task);
    } catch (error) {
      console.error(`Error in startEngine: ${error.message}`);
    }
  });
}

await clientStart().catch((err) => {
  console.error("Failed to connect to Redis in engine:", err);
  process.exit(1);
});
console.log(`Redis server running in engine...`);

startEngine();
