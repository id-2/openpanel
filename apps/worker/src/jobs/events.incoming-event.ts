import { getReferrerWithQuery, parseReferrer } from '@/utils/parse-referrer';
import { isUserAgentSet, parseUserAgent } from '@/utils/parse-user-agent';
import { isSameDomain, parsePath } from '@/utils/url';
import type { Job } from 'bullmq';
import { omit } from 'ramda';
import { escape } from 'sqlstring';
import { v4 as uuid } from 'uuid';

import { getTime, toISOString } from '@openpanel/common';
import type { IServiceCreateEventPayload } from '@openpanel/db';
import { createEvent, getEvents } from '@openpanel/db';
import { eventBuffer } from '@openpanel/db/src/buffer';
import { findJobByPrefix } from '@openpanel/queue';
import { eventsQueue } from '@openpanel/queue/src/queues';
import type { EventsQueuePayloadIncomingEvent } from '@openpanel/queue/src/queues';
import { redis } from '@openpanel/redis';

const GLOBAL_PROPERTIES = ['__path', '__referrer'];
const SESSION_TIMEOUT = 1000 * 60 * 30;
const SESSION_END_TIMEOUT = SESSION_TIMEOUT + 1000;

export async function incomingEvent(job: Job<EventsQueuePayloadIncomingEvent>) {
  const {
    geo,
    event: body,
    headers,
    projectId,
    currentDeviceId,
    previousDeviceId,
    // TODO: Remove after 2024-09-26
    currentDeviceIdDeprecated,
    previousDeviceIdDeprecated,
  } = job.data.payload;
  let deviceId: string | null = null;

  const properties = body.properties ?? {};
  const getProperty = (name: string): string | undefined => {
    // replace thing is just for older sdks when we didn't have `__`
    // remove when kiddokitchen app (24.09.02) is not used anymore
    return (
      ((properties[name] || properties[name.replace('__', '')]) as
        | string
        | null
        | undefined) ?? undefined
    );
  };
  const { ua } = headers;
  const profileId = body.profileId ? String(body.profileId) : '';
  const createdAt = new Date(body.timestamp);
  const url = getProperty('__path');
  const { path, hash, query, origin } = parsePath(url);
  const referrer = isSameDomain(getProperty('__referrer'), url)
    ? null
    : parseReferrer(getProperty('__referrer'));
  const utmReferrer = getReferrerWithQuery(query);
  const uaInfo = ua ? parseUserAgent(ua) : null;
  const isServerEvent = ua ? !isUserAgentSet(ua) : true;

  if (isServerEvent) {
    const [event] = await getEvents(
      `SELECT * FROM events WHERE name = 'screen_view' AND profile_id = ${escape(profileId)} AND project_id = ${escape(projectId)} ORDER BY created_at DESC LIMIT 1`
    );

    const payload: Omit<IServiceCreateEventPayload, 'id'> = {
      name: body.name,
      deviceId: event?.deviceId || '',
      sessionId: event?.sessionId || '',
      profileId,
      projectId,
      properties: Object.assign({}, omit(GLOBAL_PROPERTIES, properties)),
      createdAt,
      country: event?.country || geo.country || '',
      city: event?.city || geo.city || '',
      region: event?.region || geo.region || '',
      longitude: geo.longitude,
      latitude: geo.latitude,
      os: event?.os ?? '',
      osVersion: event?.osVersion ?? '',
      browser: event?.browser ?? '',
      browserVersion: event?.browserVersion ?? '',
      device: event?.device ?? '',
      brand: event?.brand ?? '',
      model: event?.model ?? '',
      duration: 0,
      path: event?.path ?? '',
      origin: event?.origin ?? '',
      referrer: event?.referrer ?? '',
      referrerName: event?.referrerName ?? '',
      referrerType: event?.referrerType ?? '',
      profile: undefined,
      meta: undefined,
    };

    return createEvent(payload);
  }

  const sessionEnd = await getSessionEnd({
    projectId,
    currentDeviceId,
    previousDeviceId,
    currentDeviceIdDeprecated,
    previousDeviceIdDeprecated,
  });

  const createSessionStart = sessionEnd === null;

  if (createSessionStart) {
    deviceId = currentDeviceId;
    eventsQueue.add(
      'event',
      {
        type: 'createSessionEnd',
        payload: {
          deviceId,
        },
      },
      {
        delay: SESSION_END_TIMEOUT,
        jobId: `sessionEnd:${projectId}:${deviceId}:${Date.now()}`,
      }
    );
  } else {
    deviceId = sessionEnd.deviceId;
    const diff = Date.now() - sessionEnd.job.timestamp;
    sessionEnd.job.changeDelay(diff + SESSION_END_TIMEOUT);
  }

  let sessionStartEvent: IServiceCreateEventPayload | null =
    await eventBuffer.find(
      (item) =>
        item.event.name === 'session_start' &&
        item.event.device_id === deviceId &&
        item.event.project_id === projectId
    );
  if (!sessionStartEvent) {
    const res = await getEvents(
      `SELECT * FROM events WHERE name = 'session_start' AND device_id = ${escape(deviceId)} AND project_id = ${escape(projectId)} ORDER BY created_at DESC LIMIT 1`
    );
    sessionStartEvent = res[0] || null;
  }

  const payload: Omit<IServiceCreateEventPayload, 'id'> = {
    name: body.name,
    deviceId,
    profileId,
    projectId,
    sessionId: createSessionStart ? uuid() : sessionStartEvent?.sessionId ?? '',
    properties: Object.assign({}, omit(GLOBAL_PROPERTIES, properties), {
      __hash: hash,
      __query: query,
    }),
    createdAt,
    country: geo.country,
    city: geo.city,
    region: geo.region,
    longitude: geo.longitude,
    latitude: geo.latitude,
    os: uaInfo?.os ?? '',
    osVersion: uaInfo?.osVersion ?? '',
    browser: uaInfo?.browser ?? '',
    browserVersion: uaInfo?.browserVersion ?? '',
    device: uaInfo?.device ?? '',
    brand: uaInfo?.brand ?? '',
    model: uaInfo?.model ?? '',
    duration: 0,
    path: path,
    origin: origin || sessionStartEvent?.origin || '',
    referrer: referrer?.url,
    referrerName: referrer?.name || utmReferrer?.name || '',
    referrerType: referrer?.type || utmReferrer?.type || '',
    profile: undefined,
    meta: undefined,
  };

  if (createSessionStart) {
    // We do not need to queue session_start
    await createEvent({
      ...payload,
      name: 'session_start',
      // @ts-expect-error
      createdAt: toISOString(getTime(payload.createdAt) - 100),
    });
  }

  return createEvent(payload);
}

async function getSessionEnd({
  projectId,
  currentDeviceId,
  previousDeviceId,
  currentDeviceIdDeprecated,
  previousDeviceIdDeprecated,
}: {
  projectId: string;
  currentDeviceId: string;
  previousDeviceId: string;
  currentDeviceIdDeprecated: string;
  previousDeviceIdDeprecated: string;
}) {
  const sessionEndKeys = await redis.keys(
    `bull:events:sessionEnd:${projectId}:*`
  );

  const sessionEndJobCurrentDeviceId = await findJobByPrefix(
    eventsQueue,
    sessionEndKeys,
    `sessionEnd:${projectId}:${currentDeviceId}:`
  );
  if (sessionEndJobCurrentDeviceId) {
    return { deviceId: currentDeviceId, job: sessionEndJobCurrentDeviceId };
  }

  const sessionEndJobPreviousDeviceId = await findJobByPrefix(
    eventsQueue,
    sessionEndKeys,
    `sessionEnd:${projectId}:${previousDeviceId}:`
  );
  if (sessionEndJobPreviousDeviceId) {
    return { deviceId: previousDeviceId, job: sessionEndJobPreviousDeviceId };
  }

  // TODO: Remove after 2024-09-26
  const sessionEndJobCurrentDeviceIdDeprecated = await findJobByPrefix(
    eventsQueue,
    sessionEndKeys,
    `sessionEnd:${projectId}:${currentDeviceIdDeprecated}:`
  );
  if (sessionEndJobCurrentDeviceIdDeprecated) {
    return {
      deviceId: currentDeviceIdDeprecated,
      job: sessionEndJobCurrentDeviceIdDeprecated,
    };
  }

  const sessionEndJobPreviousDeviceIdDeprecated = await findJobByPrefix(
    eventsQueue,
    sessionEndKeys,
    `sessionEnd:${projectId}:${previousDeviceIdDeprecated}:`
  );
  if (sessionEndJobPreviousDeviceIdDeprecated) {
    return {
      deviceId: previousDeviceIdDeprecated,
      job: sessionEndJobPreviousDeviceIdDeprecated,
    };
  }

  // Create session
  return null;
}
