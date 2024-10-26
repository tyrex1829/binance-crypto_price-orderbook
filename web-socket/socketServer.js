import { WebSocket, WebSocketServer } from "ws";
import {
  client,
  clientStart,
  popFromDoneTaskQueue,
} from "../queue/redisQueue.js";
import { v4 as uuidv4 } from "uuid";

const mpp = new Map();

const wss = new WebSocketServer({ port: 8080 });

wss.on("connection", (ws) => {
  const clientId = uuidv4();
  mpp.set(clientId, ws);
  ws.clientId = clientId;
  console.log(`User Connected with ID: ${clientId}`);

  ws.on("error", (err) => {
    console.error(`Error in ws-server: ${err}`);
  });
  ws.on("message", (message) => {
    console.log(`Msg: ${message}`);
  });
  ws.on("close", () => {
    console.log(`User disconnected: ${clientId}`);
    mpp.delete(clientId);
  });
});

function sendToEachClient(message) {
  wss.clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      console.log(`Sending message to client: ${JSON.stringify(message)}`);
      client.send(JSON.stringify(message));
    }
  });
}

await clientStart().catch((err) => {
  console.error(`Failed to connect to Redis in websocket-server: ${err}`);
  process.exit(1);
});
console.log("Redis server running in websocket-server...");

async function getCompletedTasks() {
  await popFromDoneTaskQueue((doneTask) => {
    console.log(`Sending done tasks to clients: ${JSON.stringify(doneTask)}`);
    sendToEachClient(doneTask);
  });
}

getCompletedTasks();

console.log(`ws-server is running on 8080`);

export default wss;
