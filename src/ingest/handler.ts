import {
  ConditionalCheckFailedException,
  DeleteItemCommand,
  DynamoDBClient,
  PutItemCommand,
  UpdateItemCommand,
} from '@aws-sdk/client-dynamodb';
import { PutObjectCommand, S3Client } from '@aws-sdk/client-s3';
import { SecretsManagerClient } from '@aws-sdk/client-secrets-manager';
import { SendMessageCommand, SQSClient } from '@aws-sdk/client-sqs';
import type { APIGatewayProxyEventV2, APIGatewayProxyStructuredResultV2 } from 'aws-lambda';
import { z } from 'zod';
import { type Envelope, rawObjectKey } from '../shared/envelope.js';
import { createLogger, type Logger } from '../shared/logger.js';
import { extractMetadata, type InboundRequest, type Providers, ProvidersSchema, verifyRequest } from './providers.js';
import { SecretCache } from './secrets.js';

export interface IngestConfig {
  providers: Providers;
  tableName: string;
  bucketName: string;
  queueUrl: string;
  /** Requests with a larger (decoded) body are refused with 413. */
  maxBodyBytes: number;
  /** How long a delivery id is remembered for de-duplication. */
  dedupeTtlSeconds: number;
  /**
   * A claim still `pending` after this long belongs to an attempt that died (or failed and could
   * not release it). A retry may take it over. Must exceed the function timeout.
   */
  staleClaimSeconds: number;
}

export interface IngestDeps {
  config: IngestConfig;
  dynamo: DynamoDBClient;
  s3: S3Client;
  sqs: SQSClient;
  secrets: SecretCache;
  now?: () => Date;
  logger?: Logger;
}

type Result = APIGatewayProxyStructuredResultV2;

const json = (statusCode: number, body: Record<string, unknown>): Result => ({
  statusCode,
  headers: { 'content-type': 'application/json' },
  body: JSON.stringify(body),
});

/**
 * POST /webhooks/{provider}
 *
 * 1. size check, 2. HMAC verification, 3. claim the delivery id in DynamoDB (conditional put),
 * 4. archive the raw body in S3, 5. enqueue a pointer on SQS, 6. mark the claim `enqueued`.
 * Any failure after the claim releases it and answers 500, so the sender's retry is processed.
 */
export function createIngestHandler(deps: IngestDeps): (event: APIGatewayProxyEventV2) => Promise<Result> {
  const { config, dynamo, s3, sqs, secrets } = deps;
  const now = deps.now ?? (() => new Date());
  const baseLogger = deps.logger ?? createLogger({ component: 'ingest' });

  return async (event) => {
    const provider = event.pathParameters?.provider ?? '';
    const log = baseLogger.child({ provider, requestId: event.requestContext?.requestId });
    const providerConfig = Object.hasOwn(config.providers, provider) ? config.providers[provider] : undefined;
    if (!providerConfig) {
      log.warn('unknown provider');
      return json(404, { error: 'unknown_provider' });
    }

    const declaredLength = Number(lowerCaseHeaders(event.headers)['content-length'] ?? 0);
    if (declaredLength > config.maxBodyBytes) {
      log.warn('payload too large', { declaredLength, limit: config.maxBodyBytes });
      return json(413, { error: 'payload_too_large', limitBytes: config.maxBodyBytes });
    }
    const request: InboundRequest = { body: decodeBody(event), headers: lowerCaseHeaders(event.headers) };
    if (request.body.length > config.maxBodyBytes) {
      log.warn('payload too large', { sizeBytes: request.body.length, limit: config.maxBodyBytes });
      return json(413, { error: 'payload_too_large', limitBytes: config.maxBodyBytes });
    }

    try {
      const receivedAt = now();
      const verdict = verifyRequest(providerConfig, request, await secrets.get(providerConfig.secretId), receivedAt);
      if (!verdict.ok) {
        // The reason is logged for operators but not returned to an unauthenticated caller.
        log.warn('signature rejected', { reason: verdict.reason });
        return json(401, { error: 'invalid_signature' });
      }

      const metadata = extractMetadata(providerConfig, request);
      if (!metadata.ok) {
        log.warn('malformed delivery', { error: metadata.error });
        return json(400, { error: 'malformed_delivery', detail: metadata.error });
      }
      const { deliveryId, eventType } = metadata.value;
      const deliveryLog = log.child({ deliveryId, eventType });

      const claim = await claimDelivery(dynamo, config, provider, deliveryId, receivedAt);
      if (claim.outcome === 'duplicate') {
        deliveryLog.info('duplicate delivery ignored');
        return json(200, { status: 'duplicate', deliveryId });
      }
      if (claim.outcome === 'in_progress') {
        // Another attempt holds a fresh claim. Ask the sender to retry later rather than
        // acknowledging a delivery that may still fail.
        deliveryLog.info('delivery already in progress');
        return json(409, { error: 'delivery_in_progress', deliveryId });
      }

      try {
        const key = rawObjectKey(provider, deliveryId, receivedAt);
        const contentType = request.headers['content-type'] ?? 'application/octet-stream';
        await s3.send(
          new PutObjectCommand({
            Bucket: config.bucketName,
            Key: key,
            Body: request.body,
            ContentType: contentType,
            Metadata: { provider, 'delivery-id': deliveryId, 'event-type': eventType },
          }),
        );

        const envelope: Envelope = {
          version: 1,
          provider,
          deliveryId,
          eventType,
          receivedAt: receivedAt.toISOString(),
          bucket: config.bucketName,
          key,
          sizeBytes: request.body.length,
          contentType,
        };
        await sqs.send(
          new SendMessageCommand({
            QueueUrl: config.queueUrl,
            MessageBody: JSON.stringify(envelope),
            MessageAttributes: {
              provider: { DataType: 'String', StringValue: provider },
              eventType: { DataType: 'String', StringValue: eventType },
            },
          }),
        );
      } catch (error) {
        await releaseClaim(dynamo, config, provider, deliveryId, claim.claimedAt, deliveryLog);
        throw error;
      }

      await markEnqueued(dynamo, config, provider, deliveryId, deliveryLog);
      deliveryLog.info('delivery accepted', { sizeBytes: request.body.length });
      return json(202, { status: 'accepted', deliveryId });
    } catch (error) {
      log.error('ingest failed', { error });
      return json(500, { error: 'internal_error' });
    }
  };
}

type Claim = { outcome: 'claimed'; claimedAt: number } | { outcome: 'duplicate' } | { outcome: 'in_progress' };

/**
 * Conditional put on the delivery key. Succeeds when the id is new, or when an earlier attempt
 * left a `pending` claim that is older than `staleClaimSeconds`.
 */
async function claimDelivery(
  dynamo: DynamoDBClient,
  config: IngestConfig,
  provider: string,
  deliveryId: string,
  receivedAt: Date,
): Promise<Claim> {
  const claimedAt = receivedAt.getTime();
  try {
    await dynamo.send(
      new PutItemCommand({
        TableName: config.tableName,
        Item: {
          pk: { S: deliveryKey(provider, deliveryId) },
          provider: { S: provider },
          deliveryId: { S: deliveryId },
          status: { S: 'pending' },
          claimedAt: { N: String(claimedAt) },
          expiresAt: { N: String(Math.floor(claimedAt / 1000) + config.dedupeTtlSeconds) },
        },
        ConditionExpression: 'attribute_not_exists(pk) OR (#status = :pending AND claimedAt < :staleBefore)',
        ExpressionAttributeNames: { '#status': 'status' },
        ExpressionAttributeValues: {
          ':pending': { S: 'pending' },
          ':staleBefore': { N: String(claimedAt - config.staleClaimSeconds * 1000) },
        },
        ReturnValuesOnConditionCheckFailure: 'ALL_OLD',
      }),
    );
    return { outcome: 'claimed', claimedAt };
  } catch (error) {
    if (error instanceof ConditionalCheckFailedException) {
      return error.Item?.status?.S === 'pending' ? { outcome: 'in_progress' } : { outcome: 'duplicate' };
    }
    throw error;
  }
}

/** Best effort: delete our own claim so the sender's retry is not treated as a duplicate. */
async function releaseClaim(
  dynamo: DynamoDBClient,
  config: IngestConfig,
  provider: string,
  deliveryId: string,
  claimedAt: number,
  log: Logger,
): Promise<void> {
  try {
    await dynamo.send(
      new DeleteItemCommand({
        TableName: config.tableName,
        Key: { pk: { S: deliveryKey(provider, deliveryId) } },
        ConditionExpression: '#status = :pending AND claimedAt = :claimedAt',
        ExpressionAttributeNames: { '#status': 'status' },
        ExpressionAttributeValues: { ':pending': { S: 'pending' }, ':claimedAt': { N: String(claimedAt) } },
      }),
    );
  } catch (error) {
    // The claim expires into "stale" after staleClaimSeconds, so a later retry still gets through.
    log.error('could not release claim', { error });
  }
}

async function markEnqueued(
  dynamo: DynamoDBClient,
  config: IngestConfig,
  provider: string,
  deliveryId: string,
  log: Logger,
): Promise<void> {
  try {
    await dynamo.send(
      new UpdateItemCommand({
        TableName: config.tableName,
        Key: { pk: { S: deliveryKey(provider, deliveryId) } },
        UpdateExpression: 'SET #status = :enqueued',
        ExpressionAttributeNames: { '#status': 'status' },
        ExpressionAttributeValues: { ':enqueued': { S: 'enqueued' } },
      }),
    );
  } catch (error) {
    // The message is already on the queue, so the delivery succeeded. The worst case is that a
    // retry after staleClaimSeconds enqueues it again; downstream is at-least-once anyway.
    log.warn('could not mark delivery enqueued', { error });
  }
}

function deliveryKey(provider: string, deliveryId: string): string {
  return `${provider}#${deliveryId}`;
}

function decodeBody(event: APIGatewayProxyEventV2): Buffer {
  if (!event.body) return Buffer.alloc(0);
  return event.isBase64Encoded ? Buffer.from(event.body, 'base64') : Buffer.from(event.body, 'utf8');
}

function lowerCaseHeaders(headers: APIGatewayProxyEventV2['headers'] | undefined): Record<string, string | undefined> {
  const out: Record<string, string | undefined> = {};
  for (const [name, value] of Object.entries(headers ?? {})) out[name.toLowerCase()] = value;
  return out;
}

const EnvSchema = z.object({
  PROVIDERS: z.string().min(2),
  DEDUPE_TABLE_NAME: z.string().min(1),
  RAW_BUCKET_NAME: z.string().min(1),
  QUEUE_URL: z.string().url(),
  MAX_BODY_BYTES: z.coerce.number().int().positive().default(1024 * 1024),
  DEDUPE_TTL_SECONDS: z.coerce.number().int().positive().default(7 * 24 * 3600),
  STALE_CLAIM_SECONDS: z.coerce.number().int().positive().default(120),
});

export function configFromEnv(env: NodeJS.ProcessEnv): IngestConfig {
  const parsed = EnvSchema.parse(env);
  return {
    providers: ProvidersSchema.parse(JSON.parse(parsed.PROVIDERS)),
    tableName: parsed.DEDUPE_TABLE_NAME,
    bucketName: parsed.RAW_BUCKET_NAME,
    queueUrl: parsed.QUEUE_URL,
    maxBodyBytes: parsed.MAX_BODY_BYTES,
    dedupeTtlSeconds: parsed.DEDUPE_TTL_SECONDS,
    staleClaimSeconds: parsed.STALE_CLAIM_SECONDS,
  };
}

let instance: ((event: APIGatewayProxyEventV2) => Promise<Result>) | undefined;

/** Lambda entry point. Clients and config are created once per execution environment. */
export const handler = (event: APIGatewayProxyEventV2): Promise<Result> => {
  instance ??= createIngestHandler({
    config: configFromEnv(process.env),
    dynamo: new DynamoDBClient({}),
    s3: new S3Client({}),
    sqs: new SQSClient({}),
    secrets: new SecretCache(new SecretsManagerClient({})),
  });
  return instance(event);
};
