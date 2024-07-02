import SuperJSON from 'superjson';

import { toDots } from '@openpanel/common';
import { redis, redisPub } from '@openpanel/redis';
import type { Redis } from '@openpanel/redis';

import { ch, chQuery } from './clickhouse-client';
import { transformEvent } from './services/event.service';
import type { IClickhouseEvent } from './services/event.service';
import type { IClickhouseProfile } from './services/profile.service';
import { transformProfile } from './services/profile.service';

type QueueItem<T> = {
  event: T;
  index: number;
};

const DELETE = '__DELETE__';
type OnComplete<T> =
  | ((data: T[]) => Promise<unknown>)
  | ((data: T[]) => unknown);
abstract class RedisBuffer<T> {
  private redis: Redis;
  private prefix = 'op:buffer';
  private table: string;
  private batchSize: number;
  private beforeFlush: (data: QueueItem<T>[]) => Promise<number[]>;
  private onCompleted?: OnComplete<T>;

  constructor({
    redis,
    table,
    batchSize,
    beforeFlush,
    onCompleted,
  }: {
    redis: Redis;
    table: string;
    batchSize: number;
    beforeFlush: (data: QueueItem<T>[]) => Promise<number[]>;
    onCompleted?: OnComplete<T>;
  }) {
    this.redis = redis;
    this.table = table;
    this.batchSize = batchSize;
    this.beforeFlush = beforeFlush;
    this.onCompleted = onCompleted;
  }

  public getKey() {
    return this.prefix + ':' + this.table;
  }

  public async insert(value: T) {
    await this.redis.rpush(this.getKey(), JSON.stringify(value));

    const length = await this.redis.llen(this.getKey());
    if (length >= this.batchSize) {
      this.flush();
    }
  }

  public async flush() {
    try {
      const data = await this.getQueue(this.batchSize);
      const indexes = await this.beforeFlush(data);
      await this.trimIndexes(indexes);
      const savedEvents = indexes
        .map((index) => data[index]?.event)
        .filter((event): event is T => event !== null);
      if (this.onCompleted) {
        return await this.onCompleted(savedEvents);
      }
      return indexes;
    } catch (e) {
      console.log('Failed');
    }
  }

  private async trimIndexes(indexes: number[]) {
    const multi = this.redis.multi();
    indexes.forEach((index) => {
      multi.lset(this.getKey(), index, DELETE);
    });
    multi.lrem(this.getKey(), 0, DELETE);
    await multi.exec();
  }

  public async getQueue(limit: number): Promise<QueueItem<T>[]> {
    const queue = await this.redis.lrange(this.getKey(), 0, limit);
    return queue
      .map((item, index) => ({
        event: this.transformQueueItem(item),
        index,
      }))
      .filter((item): item is QueueItem<T> => item.event !== null);
  }

  private transformQueueItem(item: string): T | null {
    try {
      return JSON.parse(item);
    } catch (e) {
      return null;
    }
  }

  public abstract find(
    callback: (item: QueueItem<T>) => boolean
  ): Promise<unknown>;

  public abstract findMany(
    callback: (item: QueueItem<T>) => boolean
  ): Promise<unknown[]>;
}

class EventBuffer extends RedisBuffer<IClickhouseEvent> {
  constructor({
    redis,
    table,
    batchSize,
    beforeFlush,
    onCompleted,
  }: {
    redis: Redis;
    table: string;
    batchSize: number;
    beforeFlush: (data: QueueItem<IClickhouseEvent>[]) => Promise<number[]>;
    onCompleted?: OnComplete<IClickhouseEvent>;
  }) {
    super({ redis, table, batchSize, beforeFlush, onCompleted });
  }

  public async findMany(
    callback: (item: QueueItem<IClickhouseEvent>) => boolean
  ) {
    return this.getQueue(-1)
      .then((queue) => {
        return queue.filter(callback).map((item) => transformEvent(item.event));
      })
      .catch(() => {
        return [];
      });
  }

  public async find(callback: (item: QueueItem<IClickhouseEvent>) => boolean) {
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

class ProfileBuffer extends RedisBuffer<IClickhouseProfile> {
  constructor({
    redis,
    table,
    batchSize,
    beforeFlush,
    onCompleted,
  }: {
    redis: Redis;
    table: string;
    batchSize: number;
    beforeFlush: (data: QueueItem<IClickhouseProfile>[]) => Promise<number[]>;
    onCompleted?: OnComplete<IClickhouseProfile>;
  }) {
    super({ redis, table, batchSize, beforeFlush, onCompleted });
  }

  public async findMany(
    callback: (item: QueueItem<IClickhouseProfile>) => boolean
  ) {
    return this.getQueue(-1)
      .then((queue) => {
        return queue
          .filter(callback)
          .map((item) => transformProfile(item.event));
      })
      .catch(() => {
        return [];
      });
  }

  public async find(
    callback: (item: QueueItem<IClickhouseProfile>) => boolean
  ) {
    return this.getQueue(-1)
      .then((queue) => {
        const match = queue.find(callback);
        return match ? transformProfile(match.event) : null;
      })
      .catch(() => {
        return null;
      });
  }
}

export const profileBuffer = new ProfileBuffer({
  table: 'profiles',
  redis: redis,
  batchSize: process.env.BATCH_SIZE ? parseInt(process.env.BATCH_SIZE) : 50,
  async beforeFlush(queue) {
    const itemsToClickhouse = new Map<string, QueueItem<IClickhouseProfile>>();

    // Sort by last event first
    queue.sort((a, b) => b.index - a.index);

    queue.forEach((item) => {
      itemsToClickhouse.set(item.event.project_id + item.event.id, item);
    });

    const cleanedQueue = Array.from(itemsToClickhouse.values());

    try {
      const profiles = await chQuery<IClickhouseProfile>(
        `SELECT 
          *
        FROM profiles
        WHERE 
            (id, project_id) IN (${cleanedQueue.map((item) => `('${item.event.id}', '${item.event.project_id}')`).join(',')})
        ORDER BY
            created_at DESC`
      );

      await ch.insert({
        table: 'profiles',
        values: cleanedQueue.map((item) => {
          const profile = profiles.find(
            (p) =>
              p.id === item.event.id && p.project_id === item.event.project_id
          );

          return {
            id: item.event.id,
            first_name: item.event.first_name ?? profile?.first_name ?? '',
            last_name: item.event.last_name ?? profile?.last_name ?? '',
            email: item.event.email ?? profile?.email ?? '',
            avatar: item.event.avatar ?? profile?.avatar ?? '',
            properties: toDots({
              ...(profile?.properties ?? {}),
              ...(item.event.properties ?? {}),
            }),
            project_id: item.event.project_id ?? profile?.project_id ?? '',
            created_at: new Date(),
            is_external: item.event.is_external,
          };
        }),
        clickhouse_settings: {
          date_time_input_format: 'best_effort',
        },
        format: 'JSONEachRow',
      });
      return queue.map((item) => item.index);
    } catch (e) {
      console.log('====== FAILED TO INSERT PROFILES FROM BUFFER ======');
      console.log(e);
      console.log('====== JSON ==============================');
      console.log(JSON.stringify(queue, null, 2));
      console.log('====== END ===============================');

      try {
        await redis.rpush('op:buffer:error:events', JSON.stringify(queue));
      } catch (e) {
        console.log('>>> Failed to push to error buffer');
      }
      return [];
    }
  },
});

export const eventBuffer = new EventBuffer({
  table: 'events',
  redis: redis,
  batchSize: process.env.BATCH_SIZE ? parseInt(process.env.BATCH_SIZE) : 50,
  onCompleted(savedEvents) {
    for (const event of savedEvents) {
      redisPub.publish('event', SuperJSON.stringify(transformEvent(event)));
      redis.set(
        `live:event:${event.project_id}:${event.profile_id}`,
        '',
        'EX',
        60 * 5
      );
    }
    return savedEvents.map((event) => event.id);
  },
  async beforeFlush(queue) {
    const itemsToClickhouse = new Set<QueueItem<IClickhouseEvent>>();
    // console.log('queue', queue);

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

    try {
      await ch.insert({
        table: 'events',
        values: Array.from(itemsToClickhouse).map((item) => ({
          ...item.event,
          properties: toDots(item.event.properties),
        })),
        format: 'JSONEachRow',
      });
      return Array.from(itemsToClickhouse).map((item) => item.index);
    } catch (e) {
      console.log('====== FAILED TO INSERT FROM BUFFER ======');
      console.log(e);
      console.log('====== JSON ==============================');
      console.log(JSON.stringify(Array.from(itemsToClickhouse), null, 2));
      console.log('====== END ===============================');

      try {
        await redis.rpush(
          'op:buffer:error:events',
          JSON.stringify(Array.from(itemsToClickhouse))
        );
      } catch (e) {
        console.log('>>> Failed to push to error buffer');
      }
      return [];
    }
  },
});
