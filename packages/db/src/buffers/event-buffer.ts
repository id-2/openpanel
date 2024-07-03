import SuperJSON from 'superjson';

import { toDots } from '@openpanel/common';
import { redis, redisPub } from '@openpanel/redis';

import { ch } from '../clickhouse-client';
import { transformEvent } from '../services/event.service';
import type { IClickhouseEvent } from '../services/event.service';
import type { QueueItem } from './buffer';
import { RedisBuffer } from './buffer';

export class EventBuffer extends RedisBuffer<IClickhouseEvent> {
  public table = 'events';

  constructor() {
    super({
      redis,
      async onCompleted(savedEvents) {
        const multi = redis.multi();
        for (const event of savedEvents) {
          redisPub.publish('event', SuperJSON.stringify(transformEvent(event)));
          multi.setex(
            `live:event:${event.project_id}:${event.profile_id}`,
            '',
            60 * 5
          );
        }
        await multi.exec();

        return savedEvents.map((event) => event.id);
      },
    });
  }

  // For events we always need to keep the latest event in the buffer
  // So batch size needs to be atleast double the amount of sessionEnds we have in redis (bullmq)
  public async batchSize() {
    try {
      const sessionEnds = await redis.keys('bull:events:sessionEnd:*');
      return sessionEnds.filter((key) => !key.endsWith(':logs')).length * 3;
    } catch (e) {
      return process.env.BATCH_SIZE
        ? parseInt(process.env.BATCH_SIZE, 10)
        : 10000;
    }
  }

  public async processQueue(
    queue: Parameters<RedisBuffer<IClickhouseEvent>['processQueue']>[0]
  ) {
    const itemsToClickhouse = new Set<QueueItem<IClickhouseEvent>>();

    // Sort data by created_at
    // oldest first
    queue.sort(
      (a, b) =>
        new Date(a.event.created_at).getTime() -
        new Date(b.event.created_at).getTime()
    );

    // All events thats not a screen_view can be sent to clickhouse
    // We only need screen_views since we want to calculate the duration of each screen
    // To do this we need a minimum of 2 screen_views
    queue
      .filter((item) => item.event.name !== 'screen_view')
      .forEach((item) => {
        itemsToClickhouse.add(item);
      });

    // Group screen_view events by session_id
    const grouped = queue
      .filter((item) => item.event.name === 'screen_view')
      .reduce(
        (acc, item) => {
          const exists = acc[item.event.session_id];
          if (exists) {
            return {
              ...acc,
              [item.event.session_id]: [...exists, item].sort(
                (a, b) =>
                  new Date(a.event.created_at).getTime() -
                  new Date(b.event.created_at).getTime()
              ),
            };
          }

          return {
            ...acc,
            [item.event.session_id]: [item],
          };
        },
        {} as Record<string, { event: IClickhouseEvent; index: number }[]>
      );

    // Iterate over each group
    for (const [sessionId, screenViews] of Object.entries(grouped)) {
      // If there is only one screen_view event we can send it back to redis since we can't calculate the duration
      const hasSessionEnd = queue.find(
        (item) =>
          item.event.name === 'session_end' &&
          item.event.session_id === sessionId
      );

      screenViews.forEach((item, index) => {
        const nextScreenView = screenViews[index + 1];
        // if nextScreenView does not exists we can't calculate the duration (last event in session)
        if (nextScreenView) {
          const duration =
            new Date(nextScreenView.event.created_at).getTime() -
            new Date(item.event.created_at).getTime();
          itemsToClickhouse.add({
            ...item,
            event: {
              ...item.event,
              duration,
            },
          });
          // push last event in session if we have a session_end event
        } else if (hasSessionEnd) {
          itemsToClickhouse.add(item);
        }
      });
    } // for of end

    await ch.insert({
      table: 'events',
      values: Array.from(itemsToClickhouse).map((item) => ({
        ...item.event,
        properties: toDots(item.event.properties),
      })),
      format: 'JSONEachRow',
    });

    return Array.from(itemsToClickhouse).map((item) => item.index);
  }

  public async findMany(
    callback: Parameters<RedisBuffer<IClickhouseEvent>['findMany']>[0]
  ) {
    return this.getQueue(-1)
      .then((queue) => {
        return queue.filter(callback).map((item) => transformEvent(item.event));
      })
      .catch(() => {
        return [];
      });
  }

  public async find(
    callback: Parameters<RedisBuffer<IClickhouseEvent>['find']>[0]
  ) {
    return this.getQueue(-1)
      .then((queue) => {
        const match = queue.find(callback);
        return match ? transformEvent(match.event) : null;
      })
      .catch(() => {
        return null;
      });
  }
}
