import { createBullBoard } from '@bull-board/api';
import { BullMQAdapter } from '@bull-board/api/bullMQAdapter';
import { ExpressAdapter } from '@bull-board/express';
import type { WorkerOptions } from 'bullmq';
import { Worker } from 'bullmq';
import express from 'express';

import { connection, eventsQueue } from '@openpanel/queue';
import { cronQueue } from '@openpanel/queue/src/queues';

import { cronJob } from './jobs/cron';
import { eventsJob } from './jobs/events';

const PORT = parseInt(process.env.WORKER_PORT || '3000', 10);
const serverAdapter = new ExpressAdapter();
serverAdapter.setBasePath(process.env.SELF_HOSTED ? '/worker' : '/');
const app = express();

const workerOptions: WorkerOptions = {
  connection,
  concurrency: parseInt(process.env.CONCURRENCY || '1', 10),
};

async function start() {
  const eventsWorker = new Worker(eventsQueue.name, eventsJob, workerOptions);

  const cronWorker = new Worker(cronQueue.name, cronJob, workerOptions);

  createBullBoard({
    queues: [new BullMQAdapter(eventsQueue), new BullMQAdapter(cronQueue)],
    serverAdapter: serverAdapter,
  });

  app.use('/', serverAdapter.getRouter());

  app.listen(PORT, () => {
    console.log(`For the UI, open http://localhost:${PORT}/`);
  });

  const gracefulShutdown = async (signal: NodeJS.Signals) => {
    console.log(`Received ${signal}, closing server...`);
    await eventsWorker.close();
    await cronWorker.close();
    // Other asynchronous closings
    process.exit(0);
  };

  process.on('SIGINT', () => gracefulShutdown('SIGINT'));
  process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
  process.on('uncaughtException', function (err) {
    // Handle the error safely
    console.error('--- Uncaught exception ---');
    console.error(err);
    console.error('--------------------------');
  });

  process.on('unhandledRejection', (reason, promise) => {
    // Handle the error safely
    console.error('--- Uncaught exception ---');
    console.error(reason);
    console.error('---');
    console.error(promise);
    console.error('--------------------------');
  });

  await cronQueue.add(
    'salt',
    {
      type: 'salt',
      payload: undefined,
    },
    {
      jobId: 'salt',
      repeat: {
        utc: true,
        pattern: '0 0 * * *',
      },
    }
  );

  await cronQueue.add(
    'flush',
    {
      type: 'flushEvents',
      payload: undefined,
    },
    {
      jobId: 'flushEvents',
      repeat: {
        every: process.env.BATCH_INTERVAL
          ? parseInt(process.env.BATCH_INTERVAL, 10)
          : 1000 * 10,
      },
    }
  );

  await cronQueue.add(
    'flush',
    {
      type: 'flushProfiles',
      payload: undefined,
    },
    {
      jobId: 'flushProfiles',
      repeat: {
        every: process.env.BATCH_INTERVAL
          ? parseInt(process.env.BATCH_INTERVAL, 10)
          : 1000 * 10,
      },
    }
  );

  const repeatableJobs = await cronQueue.getRepeatableJobs();

  console.log('Repeatable jobs:');
  console.log(repeatableJobs);
}

start();
