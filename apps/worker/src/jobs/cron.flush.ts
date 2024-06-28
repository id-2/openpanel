import { eventBuffer } from '@openpanel/db/src/buffer';

export async function flush() {
  return eventBuffer.flush();
}
