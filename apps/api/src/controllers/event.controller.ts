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
  const clientIp = getClientIp(request)!;
  const ip = request.headers['x-client-ip'];
  console.log({
    clientIp,
    ['X-Forwarded-For']: request.headers['X-Forwarded-For'],
    ['x-real-ip']: request.headers['x-real-ip'],
    ['x-client-ip']: request.headers['x-client-ip'],
    ['CF-Connecting-IP']: request.headers['CF-Connecting-IP'],
    Forwarded: request.headers.Forwarded,
  });

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
    },
  });

  reply.status(202).send('ok');
}
