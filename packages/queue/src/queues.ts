import { Queue } from 'bullmq';

import type { IServiceCreateEventPayload } from '@openpanel/db';
import type { PostEventPayload } from '@openpanel/sdk';

import { connection } from './connection';

export interface EventsQueuePayloadIncomingEvent {
  type: 'incomingEvent';
  payload: {
    projectId: string;
    event: PostEventPayload;
    geo: {
      country: string | undefined;
      city: string | undefined;
      region: string | undefined;
      longitude: number | undefined;
      latitude: number | undefined;
    };
    headers: {
      ua: string | undefined;
    };
    currentDeviceId: string;
    previousDeviceId: string;
    priority: boolean;
  };
}
export interface EventsQueuePayloadCreateEvent {
  type: 'createEvent';
  payload: Omit<IServiceCreateEventPayload, 'id'>;
}
export interface EventsQueuePayloadCreateSessionEnd {
  type: 'createSessionEnd';
  payload: Pick<
    IServiceCreateEventPayload,
    'deviceId' | 'sessionId' | 'profileId'
  >;
}
export type EventsQueuePayload =
  | EventsQueuePayloadCreateEvent
  | EventsQueuePayloadCreateSessionEnd
  | EventsQueuePayloadIncomingEvent;

export type CronQueuePayloadSalt = {
  type: 'salt';
  payload: undefined;
};
export type CronQueuePayloadFlushEvents = {
  type: 'flushEvents';
  payload: undefined;
};
export type CronQueuePayloadFlushProfiles = {
  type: 'flushProfiles';
  payload: undefined;
};
export type CronQueuePayload =
  | CronQueuePayloadSalt
  | CronQueuePayloadFlushEvents
  | CronQueuePayloadFlushProfiles;

export const eventsQueue = new Queue<EventsQueuePayload>('events', {
  connection,
  defaultJobOptions: {
    removeOnComplete: 10,
  },
});

export const cronQueue = new Queue<CronQueuePayload>('cron', {
  connection,
  defaultJobOptions: {
    removeOnComplete: 10,
  },
});
