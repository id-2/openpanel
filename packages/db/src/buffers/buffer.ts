import type { Redis } from '@openpanel/redis';

export const DELETE = '__DELETE__';

export type QueueItem<T> = {
  event: T;
  index: number;
};

export type OnCompleted<T> =
  | ((data: T[]) => Promise<unknown>)
  | ((data: T[]) => unknown);

export abstract class RedisBuffer<T> {
  private prefix = 'op:buffer';
  private onCompleted?: OnCompleted<T>;

  public redis: Redis;
  public abstract table: string;
  public abstract batchSize(): number | Promise<number>;
  public abstract processQueue(data: QueueItem<T>[]): Promise<number[]>;
  public abstract find(
    callback: (item: QueueItem<T>) => boolean
  ): Promise<unknown>;
  public abstract findMany(
    callback: (item: QueueItem<T>) => boolean
  ): Promise<unknown[]>;

  constructor({
    redis,
    onCompleted,
  }: {
    redis: Redis;
    onCompleted?: OnCompleted<T>;
  }) {
    this.redis = redis;
    this.onCompleted = onCompleted;
  }

  public getKey() {
    return this.prefix + ':' + this.table;
  }

  public async insert(value: T) {
    await this.redis.rpush(this.getKey(), JSON.stringify(value));

    const length = await this.redis.llen(this.getKey());
    if (length >= (await this.batchSize())) {
      this.flush();
    }
  }

  public async flush() {
    try {
      const data = await this.getQueue(await this.batchSize());
      try {
        const indexes = await this.processQueue(data);
        await this.trimIndexes(indexes);
        const savedEvents = indexes
          .map((index) => data[index]?.event)
          .filter((event): event is T => event !== null);
        if (this.onCompleted) {
          return await this.onCompleted(savedEvents);
        }
        return indexes;
      } catch (e) {
        console.log(
          `[${this.getKey()}] Failed to processQueue while flushing:`,
          e
        );
        const timestamp = new Date().getTime();
        await this.redis.hset(`${this.getKey()}:failed:${timestamp}`, {
          error: e instanceof Error ? e.message : 'Unknown error',
          data: JSON.stringify(data.map((item) => item.event)),
          retries: 0,
        });
      }
    } catch (e) {
      console.log(`[${this.getKey()}] Failed to getQueue while flushing:`, e);
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
}
