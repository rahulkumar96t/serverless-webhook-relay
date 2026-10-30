import { EventBridgeClient, PutEventsCommand, type PutEventsRequestEntry } from '@aws-sdk/client-eventbridge';
import { GetObjectCommand, S3Client } from '@aws-sdk/client-s3';
import type { SQSBatchResponse, SQSEvent, SQSRecord } from 'aws-lambda';
import { z } from 'zod';
import { type Envelope, EnvelopeSchema } from '../shared/envelope.js';
import { createLogger, type Logger } from '../shared/logger.js';

export interface ConsumerConfig {
  eventBusName: string;
  /** Events are published with source `<sourcePrefix>.<provider>`. */
  sourcePrefix: string;
  /**
   * EventBridge rejects entries above 256 KB. Payloads that would push the detail past this
   * budget are left out; the event still carries the S3 location of the raw body.
   */
  maxDetailBytes: number;
}

export interface ConsumerDeps {
  config: ConsumerConfig;
  s3: S3Client;
  eventBridge: EventBridgeClient;
  logger?: Logger;
}

/** PutEvents accepts at most 10 entries per call. */
const PUT_EVENTS_BATCH = 10;

export interface RelayedEventDetail {
  deliveryId: string;
  provider: string;
  receivedAt: string;
  raw: { bucket: string; key: string; sizeBytes: number; contentType: string };
  payload?: unknown;
  payloadOmitted?: 'too_large' | 'not_json';
}

/**
 * SQS -> EventBridge. Each record is handled independently and failures are reported per
 * message (ReportBatchItemFailures), so one bad message is retried, and eventually dead-lettered,
 * without re-driving the rest of the batch.
 */
export function createConsumerHandler(deps: ConsumerDeps): (event: SQSEvent) => Promise<SQSBatchResponse> {
  const { config, s3, eventBridge } = deps;
  const logger = deps.logger ?? createLogger({ component: 'consumer' });

  const toEntry = async (record: SQSRecord): Promise<PutEventsRequestEntry> => {
    const envelope = parseEnvelope(record.body);
    const object = await s3.send(new GetObjectCommand({ Bucket: envelope.bucket, Key: envelope.key }));
    if (!object.Body) throw new Error(`empty object s3://${envelope.bucket}/${envelope.key}`);
    const body = await object.Body.transformToString('utf8');
    return {
      EventBusName: config.eventBusName,
      Source: `${config.sourcePrefix}.${envelope.provider}`,
      DetailType: envelope.eventType,
      Time: new Date(envelope.receivedAt),
      Detail: JSON.stringify(buildDetail(envelope, body, config.maxDetailBytes)),
    };
  };

  return async (event) => {
    const failed = new Set<string>();
    const prepared: Array<{ messageId: string; entry: PutEventsRequestEntry }> = [];

    const settled = await Promise.allSettled(event.Records.map(toEntry));
    settled.forEach((outcome, index) => {
      const record = event.Records[index] as SQSRecord;
      if (outcome.status === 'fulfilled') {
        prepared.push({ messageId: record.messageId, entry: outcome.value });
      } else {
        failed.add(record.messageId);
        logger.error('could not prepare event', {
          messageId: record.messageId,
          receiveCount: record.attributes?.ApproximateReceiveCount,
          error: outcome.reason,
        });
      }
    });

    for (let start = 0; start < prepared.length; start += PUT_EVENTS_BATCH) {
      const chunk = prepared.slice(start, start + PUT_EVENTS_BATCH);
      try {
        const response = await eventBridge.send(new PutEventsCommand({ Entries: chunk.map((item) => item.entry) }));
        // Result entries are positional: entry i answers request entry i.
        chunk.forEach((item, index) => {
          const result = response.Entries?.[index];
          if (!result || result.ErrorCode) {
            failed.add(item.messageId);
            logger.error('event rejected by EventBridge', {
              messageId: item.messageId,
              errorCode: result?.ErrorCode,
              errorMessage: result?.ErrorMessage,
            });
          }
        });
      } catch (error) {
        chunk.forEach((item) => failed.add(item.messageId));
        logger.error('PutEvents failed', { error, entries: chunk.length });
      }
    }

    logger.info('batch processed', { received: event.Records.length, failed: failed.size });
    return { batchItemFailures: [...failed].map((itemIdentifier) => ({ itemIdentifier })) };
  };
}

export function parseEnvelope(body: string): Envelope {
  let parsed: unknown;
  try {
    parsed = JSON.parse(body);
  } catch {
    throw new Error('message body is not JSON');
  }
  const result = EnvelopeSchema.safeParse(parsed);
  if (!result.success) throw new Error(`invalid envelope: ${z.prettifyError(result.error)}`);
  return result.data;
}

export function buildDetail(envelope: Envelope, body: string, maxDetailBytes: number): RelayedEventDetail {
  const detail: RelayedEventDetail = {
    deliveryId: envelope.deliveryId,
    provider: envelope.provider,
    receivedAt: envelope.receivedAt,
    raw: {
      bucket: envelope.bucket,
      key: envelope.key,
      sizeBytes: envelope.sizeBytes,
      contentType: envelope.contentType,
    },
  };

  let payload: unknown;
  try {
    payload = JSON.parse(body);
  } catch {
    return { ...detail, payloadOmitted: 'not_json' };
  }
  const withPayload = { ...detail, payload };
  if (Buffer.byteLength(JSON.stringify(withPayload)) > maxDetailBytes) {
    return { ...detail, payloadOmitted: 'too_large' };
  }
  return withPayload;
}

const EnvSchema = z.object({
  EVENT_BUS_NAME: z.string().min(1),
  EVENT_SOURCE_PREFIX: z.string().min(1).default('webhook-relay'),
  MAX_DETAIL_BYTES: z.coerce.number().int().positive().max(256 * 1024).default(240 * 1024),
});

export function configFromEnv(env: NodeJS.ProcessEnv): ConsumerConfig {
  const parsed = EnvSchema.parse(env);
  return {
    eventBusName: parsed.EVENT_BUS_NAME,
    sourcePrefix: parsed.EVENT_SOURCE_PREFIX,
    maxDetailBytes: parsed.MAX_DETAIL_BYTES,
  };
}

let instance: ((event: SQSEvent) => Promise<SQSBatchResponse>) | undefined;

/** Lambda entry point. */
export const handler = (event: SQSEvent): Promise<SQSBatchResponse> => {
  instance ??= createConsumerHandler({
    config: configFromEnv(process.env),
    s3: new S3Client({}),
    eventBridge: new EventBridgeClient({}),
  });
  return instance(event);
};
