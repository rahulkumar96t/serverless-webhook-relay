/**
 * Signs a payload the way GitHub or Stripe would and POSTs it to a deployed relay.
 *
 *   npx tsx scripts/send-test-webhook.ts <endpoint-url> <github|stripe> <secret> [payload.json]
 *
 * The endpoint URL is the stack output WebhookUrlTemplate with {provider} filled in.
 */
import { randomUUID } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { signGithub, signStripe } from '../src/ingest/signatures.js';

const [url, scheme, secret, payloadFile] = process.argv.slice(2);
if (!url || !secret || (scheme !== 'github' && scheme !== 'stripe')) {
  console.error('usage: send-test-webhook.ts <endpoint-url> <github|stripe> <secret> [payload.json]');
  process.exit(2);
}

const defaultPayload =
  scheme === 'github'
    ? { ref: 'refs/heads/main', after: randomUUID().replaceAll('-', ''), repository: { full_name: 'example/repo' } }
    : { id: `evt_test_${randomUUID().slice(0, 8)}`, type: 'invoice.paid', data: { object: { amount_paid: 1200 } } };
const body = payloadFile ? readFileSync(payloadFile, 'utf8') : JSON.stringify(defaultPayload);

const headers: Record<string, string> = { 'content-type': 'application/json' };
if (scheme === 'github') {
  headers['x-github-event'] = 'push';
  headers['x-github-delivery'] = randomUUID();
  headers['x-hub-signature-256'] = signGithub(body, secret);
} else {
  headers['stripe-signature'] = signStripe(body, secret, Math.floor(Date.now() / 1000));
}

const response = await fetch(url, { method: 'POST', headers, body });
console.log(response.status, await response.text());
