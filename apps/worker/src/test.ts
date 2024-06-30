import { eventBuffer } from '@openpanel/db/src/buffer';

async function main() {
  console.log('Flushing buffer...');

  // eventBuffer.insert({
  //   name: 'screen_view',
  //   device_id: '6cc85f556fa5c2b91e2d763d95e62f81',
  //   profile_id: '6cc85f556fa5c2b91e2d763d95e62f81',
  //   project_id: 'nu-ska-det-fungera',
  //   session_id: 'd956a62c-68f3-4e54-9b94-5180093adbbd',
  //   path: '/',
  //   origin: 'http://localhost:8000',
  //   referrer: '',
  //   referrer_name: '',
  //   referrer_type: 'unknown',
  //   duration: 275977,
  //   // properties: '{"__title": "Create Next App", "__bounce": "false"}',
  //   created_at: '2024-06-27 20:31:57.441000',
  //   country: '',
  //   city: '',
  //   region: '',
  //   os: 'Mac OS',
  //   os_version: '10.15.7',
  //   browser: 'Chrome',
  //   browser_version: '126.0.0.0',
  //   device: 'desktop',
  //   brand: 'Apple',
  //   model: 'Macintosh',
  //   id: 'a31e7bfd-0b32-4241-a11d-97041103f36c',
  //   long: null,
  //   lat: null,
  // });

  await eventBuffer.flush();
  process.exit(0);
}

main();
