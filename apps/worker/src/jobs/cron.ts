import type { Job } from 'bullmq';

import type { CronQueuePayload } from '@openpanel/queue/src/queues';

import { flush } from './cron.flush';
import { salt } from './cron.salt';

export async function cronJob(job: Job<CronQueuePayload>) {
  switch (job.data.type) {
    case 'salt': {
      return await salt();
    }
    case 'flush': {
      return await flush();
    }
  }
}
