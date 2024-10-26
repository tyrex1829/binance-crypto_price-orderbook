import { createClient } from "redis";

const client = createClient();

async function clientStart() {
  try {
    if (!client.isOpen) {
      console.log(`Connecting to redis in queues...`);
      await client.connect();
      console.log(`Connected to redis`);
    }
  } catch (error) {
    console.error(`Error while connecting to redis in queues: ${error}`);
    throw error;
  }
}

const ensureConnected = async () => {
  if (!client.isOpen) {
    await clientStart();
  }
};

const pushToTaskQueue = async (job) => {
  try {
    await ensureConnected();
    await client.rPush("TaskQueue", JSON.stringify(job));
    console.log(
      `Successfully pushed to task queue in queues: ${JSON.stringify(job)}`
    );
  } catch (error) {
    console.error(`Error while pushing to task queue in queues: ${error}`);
  }
};

const popFromTaskQueue = async (cb) => {
  try {
    await ensureConnected();
    while (true) {
      console.log(`Waiting for job in TaskQueue...`);
      const taskData = await client.blPop("TaskQueue", 0);
      console.log(`blPop response: ${JSON.stringify(taskData, null, 2)}`);

      if (taskData && taskData.key && taskData.element) {
        const job = taskData.element;
        try {
          console.log(`Queue Name: ${taskData.key}, Job: ${job}`);
          const parsedJob = JSON.parse(job);
          console.log(
            `Parsed job from TaskQueue: ${JSON.stringify(parsedJob, null, 2)}`
          );
          await cb(parsedJob);
        } catch (e) {
          console.error(`Error parsing job from TaskQueue: ${e}`);
        }
      } else {
        console.log(
          `No valid task data returned from blPop, taskData: ${JSON.stringify(
            taskData,
            null,
            2
          )}`
        );
      }
    }
  } catch (error) {
    console.error(`Error while getting the job: ${error}`);
  }
};

const pushToDoneTaskQueue = async (doneJob) => {
  try {
    await ensureConnected();
    await client.lPush("DoneTaskQueue", JSON.stringify(doneJob));
    console.log(
      `Successfully pushed to done queue: ${JSON.stringify(doneJob)}`
    );
  } catch (error) {
    console.error(`Error while pushing to done queue: ${error}`);
  }
};

const popFromDoneTaskQueue = async (cb) => {
  try {
    await ensureConnected();
    while (true) {
      console.log(`Waiting for job in DoneTaskQueue...`);
      const taskData = await client.brPop("DoneTaskQueue", 0);
      console.log(`blPop response: ${JSON.stringify(taskData, null, 2)}`);

      if (taskData && taskData.key && taskData.element) {
        const job = taskData.element;
        try {
          console.log(`Queue name: ${taskData.key} & job: ${job}`);
          const parsedJob = JSON.parse(job);
          await cb(parsedJob);
        } catch (err) {
          console.error(`Error parsing job from doneTaskQueue: ${err}`);
        }
      } else {
        console.log(
          `No valid task data returned from blPop, taskData: ${JSON.stringify(
            taskData,
            null,
            2
          )}`
        );
      }
    }
  } catch (error) {
    console.error(`Error while getting the job: ${error}`);
  }
};

export {
  client,
  clientStart,
  pushToTaskQueue,
  popFromTaskQueue,
  pushToDoneTaskQueue,
  popFromDoneTaskQueue,
};
