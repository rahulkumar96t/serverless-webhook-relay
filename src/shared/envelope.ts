import { z } from 'zod';

/**
 * The message the ingest function puts on the queue. It carries a pointer to the raw payload in
 * S3 (claim-check pattern) rather than the payload itself, so message size never depends on the
 * webhook size and the archived body is exactly the bytes that were signed.
 */
export const EnvelopeSchema = z.object({
  version: z.literal(1),
  provider: z.string().min(1),
  deliveryId: z.string().min(1),
  eventType: z.string().min(1),
  receivedAt: z.iso.datetime(),
  bucket: z.string().min(1),
  key: z.string().min(1),
  sizeBytes: z.number().int().nonnegative(),
  contentType: z.string(),
});

export type Envelope = z.infer<typeof EnvelopeSchema>;

/** Prefix under which raw payloads are stored. IAM grants are scoped to it. */
export const RAW_PREFIX = 'raw/';

export function rawObjectKey(provider: string, deliveryId: string, receivedAt: Date): string {
  const day = receivedAt.toISOString().slice(0, 10).replaceAll('-', '/');
  return `${RAW_PREFIX}${provider}/${day}/${encodeURIComponent(deliveryId)}`;
}
