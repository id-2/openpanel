import SuperJSON from 'superjson';

import { deepMergeObjects } from '@openpanel/common';
import { redis, redisPub } from '@openpanel/redis';

import { ch } from '../clickhouse-client';
import { transformEvent } from '../services/event.service';
import type {
  IClickhouseEvent,
  IServiceCreateEventPayload,
} from '../services/event.service';
import type {
  Find,
  FindMany,
  OnCompleted,
  ProcessQueue,
  QueueItem,
} from './buffer';
import { RedisBuffer } from './buffer';

const sortOldestFirst = (
  a: QueueItem<IClickhouseEvent>,
  b: QueueItem<IClickhouseEvent>
) =>
  new Date(a.event.created_at).getTime() -
  new Date(b.event.created_at).getTime();

export class EventBuffer extends RedisBuffer<IClickhouseEvent> {
  constructor() {
    super({
      table: 'events',
      redis,
    });
  }

  public onCompleted?: OnCompleted<IClickhouseEvent> | undefined = async (
    savedEvents
  ) => {
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
  };

  public processQueue: ProcessQueue<IClickhouseEvent> = async (queue) => {
    const itemsToClickhouse = new Set<QueueItem<IClickhouseEvent>>();

    // Sort data by created_at
    // oldest first
    queue.sort(sortOldestFirst);

    // All events thats not a screen_view can be sent to clickhouse
    // We only need screen_views since we want to calculate the duration of each screen
    // To do this we need a minimum of 2 screen_views
    queue
      .filter((item) => item.event.name !== 'screen_view')
      .forEach((item) => {
        // If its a server event and we have a previous event with data we merge the data
        // This will give us more information from the server event
        if (item.event.profile_id && item.event.device === 'server') {
          const lastEventWithData = queue
            .slice(0, item.index)
            .findLast(
              (event) =>
                event.event.profile_id === item.event.profile_id &&
                item.event.path !== ''
            );

          if (lastEventWithData) {
            const event = deepMergeObjects<IClickhouseEvent>(
              lastEventWithData?.event || {},
              item.event
            );

            event.properties.__properties_from = lastEventWithData.event.id;

            return itemsToClickhouse.add({
              ...item,
              event,
            });
          }
        }

        itemsToClickhouse.add(item);
      });

    // Group screen_view events by session_id
    const grouped = queue
      .filter((item) => item.event.name === 'screen_view')
      .reduce(
        (acc, item) => {
          if (!item.event.session_id) {
            return acc;
          }

          const exists = acc[item.event.session_id];
          if (exists) {
            return {
              ...acc,
              [item.event.session_id]: [...exists, item],
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

      screenViews
        .slice()
        .sort(sortOldestFirst)
        .forEach((item, index) => {
          const nextScreenView = screenViews[index + 1];
          // if nextScreenView does not exists we can't calculate the duration (last event in session)
          if (nextScreenView) {
            const duration =
              new Date(nextScreenView.event.created_at).getTime() -
              new Date(item.event.created_at).getTime();
            const event = {
              ...item.event,
              duration,
            };
            event.properties.__duration_from = nextScreenView.event.id;
            itemsToClickhouse.add({
              ...item,
              event,
            });
            // push last event in session if we have a session_end event
          } else if (hasSessionEnd) {
            itemsToClickhouse.add(item);
          }
        });
    } // for of end

    await ch.insert({
      table: 'events',
      values: Array.from(itemsToClickhouse).map((item) => item.event),
      format: 'JSONEachRow',
    });

    return Array.from(itemsToClickhouse).map((item) => item.index);
  };

  public findMany: FindMany<IClickhouseEvent, IServiceCreateEventPayload> =
    async (callback) => {
      return this.getQueue(-1)
        .then((queue) => {
          return queue
            .filter(callback)
            .map((item) => transformEvent(item.event));
        })
        .catch(() => {
          return [];
        });
    };

  public find: Find<IClickhouseEvent, IServiceCreateEventPayload> = async (
    callback
  ) => {
    return this.getQueue(-1)
      .then((queue) => {
        const match = queue.find(callback);
        return match ? transformEvent(match.event) : null;
      })
      .catch(() => {
        return null;
      });
  };
}
