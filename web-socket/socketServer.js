import { WebSocket, WebSocketServer } from "ws";
import client from "redis";
import { popFromDoneTaskQueue } from "../queue/redisQueue";

const wss = new WebSocketServer({ port: 8080 });

wss.on("connection", (ws) => {
  console.log(`User Connected`);

  ws.on("error", (err) => {
    console.error(`Error in ws-server: ${err}`);
  });
  ws.on("message", (message) => {
    console.log(`Msg: ${message}`);
  });
  ws.on("close", () => {
    console.log(`User disconnected`);
  });
});

function sendToEachClient(message) {
  wss.clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(JSON.stringify(message));
    }
  });
}

async function getCompletedTasks() {
  popFromDoneTaskQueue((doneTask) => {
    console.log(`Sending done tasks to clients: ${doneTask}`);
    sendToEachClient(doneTask);
  });
}

getCompletedTasks();

console.log(`ws-server is running on 8080`);

export default wss;
