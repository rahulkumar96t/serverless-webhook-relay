import { z } from 'zod';
import { type Verification, verifyGithubSignature, verifyStripeSignature } from './signatures.js';

/**
 * A provider is a named webhook source (the `{provider}` path segment) bound to a signature
 * scheme and a secret. Several providers may share a scheme, e.g. two GitHub organisations
 * with different secrets.
 */
export const ProviderConfigSchema = z.discriminatedUnion('scheme', [
  z.object({ scheme: z.literal('github'), secretId: z.string().min(1) }),
  z.object({
    scheme: z.literal('stripe'),
    secretId: z.string().min(1),
    toleranceSeconds: z.number().int().positive().default(300),
  }),
]);

export const ProvidersSchema = z.record(z.string().regex(/^[a-z0-9-]{1,32}$/), ProviderConfigSchema);

export type ProviderConfig = z.infer<typeof ProviderConfigSchema>;
export type Providers = z.infer<typeof ProvidersSchema>;

export interface InboundRequest {
  body: Buffer;
  /** Lower-cased header names (API Gateway HTTP APIs deliver them lower-cased). */
  headers: Record<string, string | undefined>;
}

export interface DeliveryMetadata {
  deliveryId: string;
  eventType: string;
}

export type MetadataResult = { ok: true; value: DeliveryMetadata } | { ok: false; error: string };

export function verifyRequest(
  config: ProviderConfig,
  request: InboundRequest,
  secrets: string[],
  now: Date,
): Verification {
  switch (config.scheme) {
    case 'github':
      return verifyGithubSignature(request.body, request.headers['x-hub-signature-256'], secrets);
    case 'stripe':
      return verifyStripeSignature(request.body, request.headers['stripe-signature'], secrets, {
        toleranceSeconds: config.toleranceSeconds,
        now,
      });
  }
}

/**
 * Extracts the delivery id (the de-duplication key) and the event type. Only called after the
 * signature has been verified, so the body can be trusted to be what the sender produced.
 */
export function extractMetadata(config: ProviderConfig, request: InboundRequest): MetadataResult {
  switch (config.scheme) {
    case 'github': {
      const deliveryId = request.headers['x-github-delivery'];
      const eventType = request.headers['x-github-event'];
      if (!deliveryId || !eventType) {
        return { ok: false, error: 'missing X-GitHub-Delivery or X-GitHub-Event header' };
      }
      return { ok: true, value: { deliveryId, eventType } };
    }
    case 'stripe': {
      // Stripe has no delivery header; the event id is stable across retries of the same event.
      let parsed: unknown;
      try {
        parsed = JSON.parse(request.body.toString('utf8'));
      } catch {
        return { ok: false, error: 'body is not valid JSON' };
      }
      const event = z.object({ id: z.string().min(1), type: z.string().min(1) }).safeParse(parsed);
      if (!event.success) return { ok: false, error: 'event JSON lacks string "id" and "type"' };
      return { ok: true, value: { deliveryId: event.data.id, eventType: event.data.type } };
    }
  }
}
