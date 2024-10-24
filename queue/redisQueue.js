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

const pushToTaskQueue = async (job) => {
  try {
    await client.rPush("TaskQueue", JSON.stringify(job));
    console.log(`Successfully pushed to task queue: ${job}`);
  } catch (error) {
    console.error(`Error while pushing to task queue: ${error}`);
  }
};

const popFromTaskQueue = async (cb) => {
  try {
    console.log(`Waiting for job in TaskQueue...`);
    const [queueName, job] = await client.blPop("TaskQueue", 0);
    if (job) {
      console.log(`got the job: ${job}`);
      cb(JSON.parse(job));
    }
  } catch (error) {
    console.error(`Error while getting the job: ${error}`);
  }
};

const pushToDoneTaskQueue = async (doneJob) => {
  try {
    await client.lPush("DoneTaskQueue", JSON.stringify(doneJob));
    console.log(`Successfully pushed to done queue: ${doneJob}`);
  } catch (error) {
    console.error(`Error while pushing to done queue: ${error}`);
  }
};

const popFromDoneTaskQueue = async (cb) => {
  try {
    console.log(`Waiting for job in DoneTaskQueue...`);
    const [queueName, job] = await client.brPop("DoneTaskQueue", 0);
    if (job) {
      console.log(`Got the done job: ${job}`);
      cb(JSON.parse(job));
    }
  } catch (error) {
    console.error(`Error while getting the job: ${error}`);
  }
};

export {
  client,
  pushToTaskQueue,
  popFromTaskQueue,
  pushToDoneTaskQueue,
  popFromDoneTaskQueue,
};
