import { createClient } from "redis";

const client = createClient();

async function clientStart() {
  try {
    await client.connect();
    console.log(`Connecting to redis...`);
  } catch (error) {
    console.error(`Error while connecting to redis: ${error}`);
  }
}

clientStart();

console.log(`redis server running`);

export default client;
