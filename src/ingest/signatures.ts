import { createHmac, timingSafeEqual } from 'node:crypto';

/**
 * HMAC signature schemes used by common webhook senders. Both are verified over the exact raw
 * request bytes; re-serialising parsed JSON would change whitespace and break the signature.
 * Each verifier accepts several secrets so a secret can be rotated without dropping deliveries.
 */

export type VerificationFailure =
  | 'missing_signature'
  | 'malformed_signature'
  | 'signature_mismatch'
  | 'timestamp_out_of_tolerance';

export type Verification = { ok: true } | { ok: false; reason: VerificationFailure };

const ok: Verification = { ok: true };
const fail = (reason: VerificationFailure): Verification => ({ ok: false, reason });

const HEX_SHA256 = /^[0-9a-f]{64}$/i;

function hmacSha256(secret: string, ...parts: Buffer[]): Buffer {
  const mac = createHmac('sha256', secret);
  for (const part of parts) mac.update(part);
  return mac.digest();
}

/** Constant-time comparison; buffers of different length are unequal without leaking timing. */
function safeEqual(a: Buffer, b: Buffer): boolean {
  return a.length === b.length && timingSafeEqual(a, b);
}

/**
 * GitHub style: `X-Hub-Signature-256: sha256=<hex HMAC-SHA256(secret, body)>`.
 * The scheme has no timestamp, so replay protection comes from de-duplicating on the delivery id.
 */
export function verifyGithubSignature(body: Buffer, header: string | undefined, secrets: string[]): Verification {
  if (!header) return fail('missing_signature');
  const match = /^sha256=([0-9a-f]{64})$/i.exec(header.trim());
  if (!match?.[1]) return fail('malformed_signature');
  const provided = Buffer.from(match[1], 'hex');

  for (const secret of secrets) {
    if (safeEqual(hmacSha256(secret, body), provided)) return ok;
  }
  return fail('signature_mismatch');
}

export interface StripeVerifyOptions {
  /** Maximum accepted distance between the signed timestamp and now. */
  toleranceSeconds: number;
  now: Date;
}

/**
 * Stripe style: `Stripe-Signature: t=<unix seconds>,v1=<hex>[,v1=<hex>...]` where each v1 is
 * HMAC-SHA256(secret, "<t>.<body>"). The timestamp is part of the signed content, so checking it
 * against a tolerance window bounds how long a captured request can be replayed.
 * Unknown keys (such as the legacy v0) are ignored.
 */
export function verifyStripeSignature(
  body: Buffer,
  header: string | undefined,
  secrets: string[],
  options: StripeVerifyOptions,
): Verification {
  if (!header) return fail('missing_signature');

  let timestamp: string | undefined;
  const candidates: Buffer[] = [];
  for (const part of header.split(',')) {
    const separator = part.indexOf('=');
    if (separator <= 0) continue;
    const key = part.slice(0, separator).trim();
    const value = part.slice(separator + 1).trim();
    if (key === 't') timestamp = value;
    else if (key === 'v1' && HEX_SHA256.test(value)) candidates.push(Buffer.from(value, 'hex'));
  }
  if (!timestamp || !/^\d{1,12}$/.test(timestamp) || candidates.length === 0) {
    return fail('malformed_signature');
  }

  const signedPrefix = Buffer.from(`${timestamp}.`, 'utf8');
  const matched = secrets.some((secret) => {
    const expected = hmacSha256(secret, signedPrefix, body);
    return candidates.some((candidate) => safeEqual(expected, candidate));
  });
  // Only authenticated requests learn about the time window.
  if (!matched) return fail('signature_mismatch');

  const ageSeconds = Math.abs(options.now.getTime() / 1000 - Number(timestamp));
  if (ageSeconds > options.toleranceSeconds) return fail('timestamp_out_of_tolerance');
  return ok;
}

/** Produces a GitHub-style header value. Used by tests and the local test-sender script. */
export function signGithub(body: Buffer | string, secret: string): string {
  return `sha256=${hmacSha256(secret, Buffer.from(body)).toString('hex')}`;
}

/** Produces a Stripe-style header value for the given unix timestamp (seconds). */
export function signStripe(body: Buffer | string, secret: string, timestampSeconds: number): string {
  const signature = hmacSha256(secret, Buffer.from(`${timestampSeconds}.`), Buffer.from(body)).toString('hex');
  return `t=${timestampSeconds},v1=${signature}`;
}
