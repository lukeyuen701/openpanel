// Exports the referrer table the Go worker matches against, straight from
// the module the Bun worker runs, so the two cannot drift. `bun run gen:referrers`
// rewrites it; `bun run gen:referrers --check` fails when the committed copy is stale.
//
// The table is a plain object literal, so a lookup such as referrers['constructor']
// also finds Object.prototype members. `prototype` records what each of those
// lookups yields (a name or type of null is undefined in JavaScript), and
// `entries` keeps Object.entries order, which Array.find over Object.values follows.
import { readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import referrers from '../../../packages/core/src/shared/referrers';

const OUTPUT = join(
  import.meta.dir,
  '../internal/worker/jsport/data/referrers.json'
);
const check = process.argv.includes('--check');

const entries = Object.entries(referrers).map(([host, { type, name }]) => [
  host,
  type,
  name,
]);
const table = referrers as Record<
  string,
  { type?: unknown; name?: unknown } | undefined
>;
const prototype = Object.getOwnPropertyNames(Object.prototype)
  .filter((key) => !Object.hasOwn(referrers, key))
  .map((key) => {
    const match = table[key];
    return [
      key,
      typeof match?.name === 'string' ? match.name : null,
      typeof match?.type === 'string' ? match.type : null,
    ];
  });

const next = `${JSON.stringify({ entries, prototype }, null, 0).replaceAll('],[', '],\n[')}\n`;
let current = '';
try {
  current = readFileSync(OUTPUT, 'utf8');
} catch {
  current = '';
}
if (current !== next) {
  if (check) {
    console.error(
      'stale: internal/worker/jsport/data/referrers.json (run bun run gen:referrers in apps/ingest)'
    );
    process.exit(1);
  }
  writeFileSync(OUTPUT, next);
  console.log('wrote internal/worker/jsport/data/referrers.json');
}
