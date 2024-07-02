import { eventBuffer, profileBuffer } from '@openpanel/db/src/buffer';

export async function flush() {
  return eventBuffer.flush();
}
export async function flushProfiles() {
  return profileBuffer.flush();
}
