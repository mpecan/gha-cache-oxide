// Drives the real @actions/cache v1 client through forgejo-runner's
// cache proxy. Env: URL_SHARED, URL_ISOLATED (ACTIONS_CACHE_URL values
// for a run without / with a write-isolation key), BIG_MB, CACHE_PKG
// (`@actions/cache` or `actions-cache-6`, the version rust-cache@v2 ships).
import { createHash, randomBytes } from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';

const cache = await import(process.env.CACHE_PKG || '@actions/cache');

const work = fs.mkdtempSync('/tmp/oxide-e2e-');
process.chdir(work);
process.env.RUNNER_TEMP = path.join(work, 'tmp');
fs.mkdirSync(process.env.RUNNER_TEMP);
process.env.ACTIONS_RUNTIME_TOKEN = 'unused-by-proxy';
const use = (url) => { process.env.ACTIONS_CACHE_URL = url; };

const sha = (f) => createHash('sha256').update(fs.readFileSync(f)).digest('hex');
function makeData(mb) {
  fs.rmSync('data', { recursive: true, force: true });
  fs.mkdirSync('data');
  const fd = fs.openSync('data/big.bin', 'w');
  for (let i = 0; i < mb; i++) fs.writeSync(fd, randomBytes(1024 * 1024));
  fs.closeSync(fd);
  fs.writeFileSync('data/small.txt', 'hello ' + Date.now());
  return { big: sha('data/big.bin'), small: sha('data/small.txt') };
}
function assert(cond, msg) { if (!cond) { console.error('FAIL:', msg); process.exit(1); } console.log('ok -', msg); }

const run = Date.now();
const key = `e2e-Rust-${run}`;
const mb = Number(process.env.BIG_MB || 150);

use(process.env.URL_SHARED);
assert((await cache.restoreCache(['data'], key)) === undefined, 'cold restore misses');
const want = makeData(mb);
let t = Date.now();
const id = await cache.saveCache(['data'], key);
assert(typeof id === 'number' && id > 0, `save returned cacheId ${id} (${mb} MiB in ${Date.now() - t} ms)`);

fs.rmSync('data', { recursive: true });
t = Date.now();
assert((await cache.restoreCache(['data'], key)) === key, `exact restore hits with the key as sent, as setup-node's primaryKey === matchedKey needs (${Date.now() - t} ms)`);
assert(sha('data/big.bin') === want.big && sha('data/small.txt') === want.small, 'restored bytes identical');

fs.rmSync('data', { recursive: true });
const hit = await cache.restoreCache(['data'], `e2e-rust-${run}-nomatch`, [`E2E-rust-${run}`]);
// The restore key equals the stored key case-insensitively, so this is
// an exact restore-key match: echoed as sent.
assert(hit === `E2E-rust-${run}`, `restore key hits case-insensitively, echoed as sent: ${hit}`);
assert(sha('data/big.bin') === want.big, 'prefix-restored bytes identical');

// Write isolation: a PR run saves under its key; shared runs must not see it.
use(process.env.URL_ISOLATED);
const isoKey = `e2e-iso-${run}`;
makeData(2);
await cache.saveCache(['data'], isoKey);
assert((await cache.restoreCache(['data'], isoKey)) === isoKey, 'isolated run reads its own entry');
use(process.env.URL_SHARED);
assert((await cache.restoreCache(['data'], isoKey)) === undefined, 'shared run cannot read isolated entry');
use(process.env.URL_ISOLATED);
const fb = await cache.restoreCache(['data'], key);
assert(fb !== undefined, `isolated run falls back to shared entry (${fb})`);

fs.rmSync(work, { recursive: true, force: true });
console.log('E2E PASS');
