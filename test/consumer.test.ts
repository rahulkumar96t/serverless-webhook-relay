import { EventBridgeClient, PutEventsCommand } from '@aws-sdk/client-eventbridge';
import { GetObjectCommand, type GetObjectCommandOutput, S3Client } from '@aws-sdk/client-s3';
import type { SQSEvent, SQSRecord } from 'aws-lambda';
import { mockClient } from 'aws-sdk-client-mock';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { buildDetail, createConsumerHandler, parseEnvelope } from '../src/consumer/handler.js';
import type { Envelope } from '../src/shared/envelope.js';

const s3 = mockClient(S3Client);
const eventBridge = mockClient(EventBridgeClient);

const config = { eventBusName: 'relay-bus', sourcePrefix: 'webhook-relay', maxDetailBytes: 2_048 };

function makeHandler() {
  return createConsumerHandler({ config, s3: new S3Client({}), eventBridge: new EventBridgeClient({}) });
}

function envelope(id: string, overrides: Partial<Envelope> = {}): Envelope {
  return {
    version: 1,
    provider: 'github',
    deliveryId: id,
    eventType: 'push',
    receivedAt: '2026-03-01T10:00:00.000Z',
    bucket: 'raw-bucket',
    key: `raw/github/2026/03/01/${id}`,
    sizeBytes: 10,
    contentType: 'application/json',
    ...overrides,
  };
}

function record(messageId: string, body: unknown): SQSRecord {
  return {
    messageId,
    receiptHandle: `rh-${messageId}`,
    body: typeof body === 'string' ? body : JSON.stringify(body),
    attributes: { ApproximateReceiveCount: '1' } as SQSRecord['attributes'],
    messageAttributes: {},
    md5OfBody: '',
    eventSource: 'aws:sqs',
    eventSourceARN: 'arn:aws:sqs:eu-central-1:123456789012:deliveries',
    awsRegion: 'eu-central-1',
  };
}

const batch = (...records: SQSRecord[]): SQSEvent => ({ Records: records });

/** The handler only needs transformToString from the SDK's streaming body. */
function objectBody(text: string): GetObjectCommandOutput {
  return { $metadata: {}, Body: { transformToString: async () => text } } as unknown as GetObjectCommandOutput;
}

beforeEach(() => {
  s3.reset();
  eventBridge.reset();
  s3.on(GetObjectCommand).callsFake((input: { Key: string }) =>
    objectBody(JSON.stringify({ ref: 'refs/heads/main', key: input.Key })),
  );
  eventBridge.on(PutEventsCommand).callsFake((input: { Entries: unknown[] }) => ({
    FailedEntryCount: 0,
    Entries: input.Entries.map((_, index) => ({ EventId: `e-${index}` })),
  }));
  for (const level of ['debug', 'info', 'warn', 'error'] as const) {
    vi.spyOn(console, level).mockImplementation(() => undefined);
  }
});

afterEach(() => {
  vi.restoreAllMocks();
});

describe('consumer', () => {
  it('publishes each delivery to the bus with source, detail-type and the parsed payload', async () => {
    const response = await makeHandler()(batch(record('m1', envelope('d1')), record('m2', envelope('d2'))));

    expect(response).toEqual({ batchItemFailures: [] });
    const calls = eventBridge.commandCalls(PutEventsCommand);
    expect(calls).toHaveLength(1);
    const entries = calls[0]?.args[0].input.Entries ?? [];
    expect(entries).toHaveLength(2);
    expect(entries[0]).toMatchObject({
      EventBusName: 'relay-bus',
      Source: 'webhook-relay.github',
      DetailType: 'push',
      Time: new Date('2026-03-01T10:00:00.000Z'),
    });
    expect(JSON.parse(entries[0]?.Detail ?? '')).toEqual({
      deliveryId: 'd1',
      provider: 'github',
      receivedAt: '2026-03-01T10:00:00.000Z',
      raw: { bucket: 'raw-bucket', key: 'raw/github/2026/03/01/d1', sizeBytes: 10, contentType: 'application/json' },
      payload: { ref: 'refs/heads/main', key: 'raw/github/2026/03/01/d1' },
    });
    expect(s3.commandCalls(GetObjectCommand)[0]?.args[0].input).toEqual({
      Bucket: 'raw-bucket',
      Key: 'raw/github/2026/03/01/d1',
    });
  });

  it('reports only the entries EventBridge rejected as batch item failures', async () => {
    eventBridge.on(PutEventsCommand).resolves({
      FailedEntryCount: 1,
      Entries: [{ EventId: 'e-0' }, { ErrorCode: 'InternalFailure', ErrorMessage: 'try again' }, { EventId: 'e-2' }],
    });
    const response = await makeHandler()(
      batch(record('m1', envelope('d1')), record('m2', envelope('d2')), record('m3', envelope('d3'))),
    );
    expect(response).toEqual({ batchItemFailures: [{ itemIdentifier: 'm2' }] });
  });

  it('fails a poison message without blocking the rest of the batch', async () => {
    const response = await makeHandler()(
      batch(
        record('m1', envelope('d1')),
        record('bad-json', '{not json'),
        record('bad-shape', { version: 2, provider: 'github' }),
        record('m4', envelope('d4')),
      ),
    );
    expect(response.batchItemFailures.map((failure) => failure.itemIdentifier).sort()).toEqual([
      'bad-json',
      'bad-shape',
    ]);
    expect(eventBridge.commandCalls(PutEventsCommand)[0]?.args[0].input.Entries).toHaveLength(2);
  });

  it('fails a record whose raw payload cannot be read', async () => {
    s3.on(GetObjectCommand, { Key: 'raw/github/2026/03/01/d2' }).rejects(new Error('NoSuchKey'));
    const response = await makeHandler()(batch(record('m1', envelope('d1')), record('m2', envelope('d2'))));
    expect(response).toEqual({ batchItemFailures: [{ itemIdentifier: 'm2' }] });
  });

  it('fails every record of a PutEvents call that throws', async () => {
    eventBridge.on(PutEventsCommand).rejects(new Error('throttled'));
    const response = await makeHandler()(batch(record('m1', envelope('d1')), record('m2', envelope('d2'))));
    expect(response.batchItemFailures.map((failure) => failure.itemIdentifier)).toEqual(['m1', 'm2']);
  });

  it('treats a missing result entry as a failure', async () => {
    eventBridge.on(PutEventsCommand).resolves({ FailedEntryCount: 0, Entries: [{ EventId: 'e-0' }] });
    const response = await makeHandler()(batch(record('m1', envelope('d1')), record('m2', envelope('d2'))));
    expect(response).toEqual({ batchItemFailures: [{ itemIdentifier: 'm2' }] });
  });

  it('splits more than ten entries into several PutEvents calls', async () => {
    const records = Array.from({ length: 12 }, (_, index) => record(`m${index}`, envelope(`d${index}`)));
    const response = await makeHandler()(batch(...records));
    expect(response.batchItemFailures).toEqual([]);
    expect(eventBridge.commandCalls(PutEventsCommand).map((call) => call.args[0].input.Entries?.length)).toEqual([
      10, 2,
    ]);
  });

  it('maps failures in the second chunk back to the right messages', async () => {
    eventBridge
      .on(PutEventsCommand)
      .resolvesOnce({ FailedEntryCount: 0, Entries: Array.from({ length: 10 }, () => ({ EventId: 'ok' })) })
      .resolvesOnce({ FailedEntryCount: 1, Entries: [{ EventId: 'ok' }, { ErrorCode: 'ThrottlingException' }] });
    const records = Array.from({ length: 12 }, (_, index) => record(`m${index}`, envelope(`d${index}`)));
    const response = await makeHandler()(batch(...records));
    expect(response).toEqual({ batchItemFailures: [{ itemIdentifier: 'm11' }] });
  });
});

describe('event detail', () => {
  it('omits payloads that would exceed the EventBridge size budget but keeps the S3 pointer', () => {
    const detail = buildDetail(envelope('big'), JSON.stringify({ data: 'x'.repeat(5_000) }), 2_048);
    expect(detail.payloadOmitted).toBe('too_large');
    expect(detail.payload).toBeUndefined();
    expect(detail.raw.key).toBe('raw/github/2026/03/01/big');
  });

  it('omits non-JSON payloads', () => {
    expect(buildDetail(envelope('txt'), 'plain text', 2_048).payloadOmitted).toBe('not_json');
  });

  it('validates envelopes', () => {
    expect(parseEnvelope(JSON.stringify(envelope('ok'))).deliveryId).toBe('ok');
    expect(() => parseEnvelope(JSON.stringify({ ...envelope('x'), receivedAt: 'yesterday' }))).toThrow(/invalid envelope/);
    expect(() => parseEnvelope('[]')).toThrow(/invalid envelope/);
  });
});
