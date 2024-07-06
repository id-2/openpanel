import { getClientIp, parseIp } from '@/utils/parseIp';
import type { FastifyReply, FastifyRequest } from 'fastify';

import { generateDeviceId } from '@openpanel/common';
import { getSalts } from '@openpanel/db';
import { eventsQueue } from '@openpanel/queue';
import { redis } from '@openpanel/redis';
import type { PostEventPayload } from '@openpanel/sdk';

export async function postEvent(
  request: FastifyRequest<{
    Body: PostEventPayload;
  }>,
  reply: FastifyReply
) {
  const ip = getClientIp(request)!;
  const ua = request.headers['user-agent']!;
  const origin = request.headers.origin!;
  const projectId = request.client?.projectId;

  await redis.lpush('user-agents', JSON.stringify({ origin, ua, projectId }));

  if (!projectId) {
    reply.status(400).send('missing origin');
    return;
  }

  const [salts, geo] = await Promise.all([getSalts(), parseIp(ip)]);
  const currentDeviceId = generateDeviceId({
    salt: salts.current,
    origin: projectId,
    ip,
    ua,
  });
  const previousDeviceId = generateDeviceId({
    salt: salts.previous,
    origin: projectId,
    ip,
    ua,
  });

  // this will ensure that we don't have multiple events creating sessions
  const locked = await redis.set(
    `request:priority:${currentDeviceId}-${previousDeviceId}`,
    'locked',
    'EX',
    10,
    'NX'
  );

  eventsQueue.add('event', {
    type: 'incomingEvent',
    payload: {
      projectId: request.projectId,
      headers: {
        ua,
      },
      event: request.body,
      geo,
      currentDeviceId,
      previousDeviceId,
      priority: locked === 'OK',
    },
  });

  reply.status(202).send('ok');
}
