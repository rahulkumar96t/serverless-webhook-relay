import { describe, expect, it } from 'vitest';
import { parseSecret } from '../src/ingest/secrets.js';
import { signGithub, signStripe, verifyGithubSignature, verifyStripeSignature } from '../src/ingest/signatures.js';

const SECRET = 'It is a secret to everybody';
const body = Buffer.from('{"action":"opened","number":1}');

describe('GitHub-style signatures', () => {
  it('accepts a correct signature', () => {
    expect(verifyGithubSignature(body, signGithub(body, SECRET), [SECRET])).toEqual({ ok: true });
  });

  it('matches the documented GitHub test vector', () => {
    // Example from GitHub's "Validating webhook deliveries" documentation.
    const header = 'sha256=757107ea0eb2509fc211221cce984b8a37570b6d7586c22c46f4379c8b043e17';
    expect(verifyGithubSignature(Buffer.from('Hello, World!'), header, ["It's a Secret to Everybody"])).toEqual({
      ok: true,
    });
  });

  it('accepts upper-case hex and surrounding whitespace', () => {
    const header = ` ${signGithub(body, SECRET).replace(/[0-9a-f]{64}$/, (hex) => hex.toUpperCase())} `;
    expect(verifyGithubSignature(body, header, [SECRET]).ok).toBe(true);
  });

  it('rejects a body modified by a single byte', () => {
    const header = signGithub(body, SECRET);
    const tampered = Buffer.from(body);
    tampered[2] = tampered[2]! ^ 0x01;
    expect(verifyGithubSignature(tampered, header, [SECRET])).toEqual({ ok: false, reason: 'signature_mismatch' });
  });

  it('rejects a signature made with another secret', () => {
    expect(verifyGithubSignature(body, signGithub(body, 'other'), [SECRET])).toEqual({
      ok: false,
      reason: 'signature_mismatch',
    });
  });

  it('reports a missing header', () => {
    expect(verifyGithubSignature(body, undefined, [SECRET])).toEqual({ ok: false, reason: 'missing_signature' });
    expect(verifyGithubSignature(body, '', [SECRET])).toEqual({ ok: false, reason: 'missing_signature' });
  });

  it.each([
    ['legacy sha1 header', `sha1=${'a'.repeat(40)}`],
    ['no algorithm prefix', 'a'.repeat(64)],
    ['too short', `sha256=${'a'.repeat(63)}`],
    ['too long', `sha256=${'a'.repeat(65)}`],
    ['not hex', `sha256=${'z'.repeat(64)}`],
  ])('reports a malformed header: %s', (_label, header) => {
    expect(verifyGithubSignature(body, header, [SECRET])).toEqual({ ok: false, reason: 'malformed_signature' });
  });

  it('accepts any of several secrets (rotation)', () => {
    expect(verifyGithubSignature(body, signGithub(body, 'old'), ['new', 'old']).ok).toBe(true);
    expect(verifyGithubSignature(body, signGithub(body, 'new'), ['new', 'old']).ok).toBe(true);
  });

  it('verifies an empty body', () => {
    const empty = Buffer.alloc(0);
    expect(verifyGithubSignature(empty, signGithub(empty, SECRET), [SECRET]).ok).toBe(true);
  });

  it('works on raw bytes, including non-UTF-8 payloads', () => {
    const binary = Buffer.from([0xff, 0xfe, 0x00, 0x80]);
    expect(verifyGithubSignature(binary, signGithub(binary, SECRET), [SECRET]).ok).toBe(true);
  });
});

describe('Stripe-style signatures', () => {
  const now = new Date('2026-01-15T12:00:00Z');
  const nowSeconds = now.getTime() / 1000;
  const options = { toleranceSeconds: 300, now };

  it('accepts a fresh, correctly signed request', () => {
    expect(verifyStripeSignature(body, signStripe(body, SECRET, nowSeconds), [SECRET], options)).toEqual({ ok: true });
  });

  it('accepts timestamps exactly at the tolerance boundary, in both directions', () => {
    for (const offset of [-300, 300]) {
      const header = signStripe(body, SECRET, nowSeconds + offset);
      expect(verifyStripeSignature(body, header, [SECRET], options).ok).toBe(true);
    }
  });

  it('rejects a replay outside the tolerance window', () => {
    const header = signStripe(body, SECRET, nowSeconds - 301);
    expect(verifyStripeSignature(body, header, [SECRET], options)).toEqual({
      ok: false,
      reason: 'timestamp_out_of_tolerance',
    });
  });

  it('rejects timestamps too far in the future', () => {
    const header = signStripe(body, SECRET, nowSeconds + 3600);
    expect(verifyStripeSignature(body, header, [SECRET], options).ok).toBe(false);
  });

  it('checks the signature before the timestamp, so unauthenticated callers learn nothing about the window', () => {
    const header = signStripe(body, 'wrong', nowSeconds - 10_000);
    expect(verifyStripeSignature(body, header, [SECRET], options)).toEqual({ ok: false, reason: 'signature_mismatch' });
  });

  it('binds the timestamp into the signature', () => {
    const [, v1] = signStripe(body, SECRET, nowSeconds).split(',');
    const forged = `t=${nowSeconds + 1},${v1}`;
    expect(verifyStripeSignature(body, forged, [SECRET], options)).toEqual({ ok: false, reason: 'signature_mismatch' });
  });

  it('accepts the header when any of several v1 signatures matches and ignores v0', () => {
    const valid = signStripe(body, SECRET, nowSeconds).split(',')[1];
    const header = `t=${nowSeconds}, v0=${'0'.repeat(64)}, v1=${'1'.repeat(64)}, ${valid}`;
    expect(verifyStripeSignature(body, header, [SECRET], options).ok).toBe(true);
  });

  it('rejects a tampered body', () => {
    const header = signStripe(body, SECRET, nowSeconds);
    expect(verifyStripeSignature(Buffer.from(`${body} `), header, [SECRET], options).ok).toBe(false);
  });

  it.each([
    ['no timestamp', `v1=${'a'.repeat(64)}`],
    ['no v1', 't=1700000000,v0=abc'],
    ['non-numeric timestamp', `t=soon,v1=${'a'.repeat(64)}`],
    ['short v1', 't=1700000000,v1=abcd'],
    ['garbage', 'not a header'],
  ])('reports a malformed header: %s', (_label, header) => {
    expect(verifyStripeSignature(body, header, [SECRET], options)).toEqual({
      ok: false,
      reason: 'malformed_signature',
    });
  });

  it('reports a missing header', () => {
    expect(verifyStripeSignature(body, undefined, [SECRET], options)).toEqual({
      ok: false,
      reason: 'missing_signature',
    });
  });

  it('supports rotation across secrets', () => {
    const header = signStripe(body, 'whsec_previous', nowSeconds);
    expect(verifyStripeSignature(body, header, ['whsec_current', 'whsec_previous'], options).ok).toBe(true);
  });
});

describe('parseSecret', () => {
  it('reads plain strings and JSON arrays', () => {
    expect(parseSecret('abc')).toEqual(['abc']);
    expect(parseSecret('  abc \n')).toEqual(['abc']);
    expect(parseSecret('["new","old"]')).toEqual(['new', 'old']);
    expect(parseSecret('["new", "", 7]')).toEqual(['new']);
  });

  it('treats non-JSON text starting with a bracket as a literal secret', () => {
    expect(parseSecret('[not json')).toEqual(['[not json']);
  });

  it('returns nothing for empty values', () => {
    expect(parseSecret(undefined)).toEqual([]);
    expect(parseSecret('   ')).toEqual([]);
    expect(parseSecret('[]')).toEqual([]);
  });
});
