// Golden MaxMind lookups from the API's geo client (needs the .mmdb files
// packages/core's codegen downloads).
import { writeFileSync } from 'node:fs';
import {
  getAsnInfo,
  getGeoLocation,
} from '../../../packages/core/src/clients/geo';

let seed = 7;
const rand = () => {
  seed = (seed * 1_103_515_245 + 12_345) % 2_147_483_648;
  return seed / 2_147_483_648;
};
const octet = () => Math.floor(rand() * 256);
const ips = [
  '',
  '127.0.0.1',
  '::1',
  'unknown',
  '81.2.69.142',
  '8.8.8.8',
  '1.1.1.1',
  '2001:4860:4860::8888',
  '::ffff:8.8.8.8',
  '10.0.0.1',
  '192.168.1.1',
  '52.94.236.248',
  '13.107.42.14',
  '2a03:2880:f12c:83:face:b00c:0:25de',
  'fe80::1%eth0',
  '999.1.1.1',
  ' 8.8.8.8',
];
for (let i = 0; i < 2000; i++) {
  ips.push([octet(), octet(), octet(), octet()].join('.'));
}
const out: { ip: string; geo: string; asn: unknown }[] = [];
for (const ip of ips) {
  out.push({
    ip,
    geo: JSON.stringify(await getGeoLocation(ip)),
    asn: await getAsnInfo(ip),
  });
}
writeFileSync(process.argv[2], JSON.stringify(out));
console.log(
  out.length,
  out.filter((o) => o.geo !== '{}').length,
  out.filter((o) => o.asn.isHosting).length
);
