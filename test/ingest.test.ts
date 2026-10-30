import {
  ConditionalCheckFailedException,
  DeleteItemCommand,
  DynamoDBClient,
  PutItemCommand,
  UpdateItemCommand,
} from '@aws-sdk/client-dynamodb';
import { PutObjectCommand, S3Client } from '@aws-sdk/client-s3';
import { GetSecretValueCommand, SecretsManagerClient } from '@aws-sdk/client-secrets-manager';
import { SendMessageCommand, SQSClient } from '@aws-sdk/client-sqs';
import type { APIGatewayProxyEventV2 } from 'aws-lambda';
import { mockClient } from 'aws-sdk-client-mock';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { configFromEnv, createIngestHandler, type IngestConfig } from '../src/ingest/handler.js';
import { SecretCache } from '../src/ingest/secrets.js';
import { signGithub, signStripe } from '../src/ingest/signatures.js';
import type { Envelope } from '../src/shared/envelope.js';

const NOW = new Date('2026-03-01T10:00:00.000Z');
const GITHUB_SECRET = 'gh-secret';
const STRIPE_SECRET = 'whsec_test';

const config: IngestConfig = {
  providers: {
    github: { scheme: 'github', secretId: 'arn:secret:github' },
    stripe: { scheme: 'stripe', secretId: 'arn:secret:stripe', toleranceSeconds: 300 },
  },
  tableName: 'deliveries',
  bucketName: 'raw-bucket',
  queueUrl: 'https://sqs.eu-central-1.amazonaws.com/123456789012/deliveries',
  maxBodyBytes: 1024,
  dedupeTtlSeconds: 7 * 24 * 3600,
  staleClaimSeconds: 120,
};

const dynamo = mockClient(DynamoDBClient);
const s3 = mockClient(S3Client);
const sqs = mockClient(SQSClient);
const secretsManager = mockClient(SecretsManagerClient);

let logs: Array<{ level: string; record: Record<string, unknown> }>;
let calls: string[];

function makeHandler() {
  return createIngestHandler({
    config,
    dynamo: new DynamoDBClient({}),
    s3: new S3Client({}),
    sqs: new SQSClient({}),
    secrets: new SecretCache(new SecretsManagerClient({})),
    now: () => NOW,
  });
}

function event(
  provider: string,
  body: string | Buffer,
  headers: Record<string, string>,
  options: { base64?: boolean } = {},
): APIGatewayProxyEventV2 {
  const raw = Buffer.from(body);
  return {
    version: '2.0',
    routeKey: 'POST /webhooks/{provider}',
    rawPath: `/webhooks/${provider}`,
    rawQueryString: '',
    headers: { 'content-type': 'application/json', ...headers },
    pathParameters: { provider },
    requestContext: { requestId: 'req-1' } as APIGatewayProxyEventV2['requestContext'],
    body: options.base64 ? raw.toString('base64') : raw.toString('utf8'),
    isBase64Encoded: options.base64 ?? false,
  };
}

function githubEvent(body: string, overrides: Record<string, string> = {}) {
  return event('github', body, {
    'x-github-delivery': 'd-123',
    'x-github-event': 'push',
    'x-hub-signature-256': signGithub(body, GITHUB_SECRET),
    ...overrides,
  });
}

const parse = (body: string | undefined) => JSON.parse(body ?? '{}') as Record<string, unknown>;

beforeEach(() => {
  dynamo.reset();
  s3.reset();
  sqs.reset();
  secretsManager.reset();
  secretsManager.on(GetSecretValueCommand, { SecretId: 'arn:secret:github' }).resolves({ SecretString: GITHUB_SECRET });
  secretsManager.on(GetSecretValueCommand, { SecretId: 'arn:secret:stripe' }).resolves({ SecretString: STRIPE_SECRET });
  calls = [];
  const record = <T>(name: string, output: T) => () => {
    calls.push(name);
    return output;
  };
  dynamo.on(PutItemCommand).callsFake(record('claim', {}));
  dynamo.on(UpdateItemCommand).callsFake(record('mark-enqueued', {}));
  dynamo.on(DeleteItemCommand).callsFake(record('release', {}));
  s3.on(PutObjectCommand).callsFake(record('archive', {}));
  sqs.on(SendMessageCommand).callsFake(record('enqueue', { MessageId: 'm-1' }));

  logs = [];
  for (const level of ['debug', 'info', 'warn', 'error'] as const) {
    vi.spyOn(console, level).mockImplementation((record: Record<string, unknown>) => {
      logs.push({ level, record });
    });
  }
});

afterEach(() => {
  vi.restoreAllMocks();
});

describe('accepted deliveries', () => {
  it('claims, archives, enqueues and marks a GitHub delivery, in that order', async () => {
    const body = '{"ref":"refs/heads/main","after":"abc"}';
    const response = await makeHandler()(githubEvent(body));

    expect(response.statusCode).toBe(202);
    expect(parse(response.body)).toEqual({ status: 'accepted', deliveryId: 'd-123' });

    const [claim] = dynamo.commandCalls(PutItemCommand);
    expect(claim?.args[0].input).toMatchObject({
      TableName: 'deliveries',
      Item: {
        pk: { S: 'github#d-123' },
        status: { S: 'pending' },
        claimedAt: { N: String(NOW.getTime()) },
        expiresAt: { N: String(NOW.getTime() / 1000 + 7 * 24 * 3600) },
      },
      ConditionExpression: 'attribute_not_exists(pk) OR (#status = :pending AND claimedAt < :staleBefore)',
      ExpressionAttributeValues: {
        ':pending': { S: 'pending' },
        ':staleBefore': { N: String(NOW.getTime() - 120_000) },
      },
      ReturnValuesOnConditionCheckFailure: 'ALL_OLD',
    });

    const [put] = s3.commandCalls(PutObjectCommand);
    expect(put?.args[0].input).toMatchObject({
      Bucket: 'raw-bucket',
      Key: 'raw/github/2026/03/01/d-123',
      ContentType: 'application/json',
      Metadata: { provider: 'github', 'delivery-id': 'd-123', 'event-type': 'push' },
    });
    expect(Buffer.from(put?.args[0].input.Body as Buffer).toString()).toBe(body);

    const [send] = sqs.commandCalls(SendMessageCommand);
    const envelope = JSON.parse(send?.args[0].input.MessageBody ?? '') as Envelope;
    expect(envelope).toEqual({
      version: 1,
      provider: 'github',
      deliveryId: 'd-123',
      eventType: 'push',
      receivedAt: NOW.toISOString(),
      bucket: 'raw-bucket',
      key: 'raw/github/2026/03/01/d-123',
      sizeBytes: Buffer.byteLength(body),
      contentType: 'application/json',
    });
    expect(send?.args[0].input.MessageAttributes?.eventType?.StringValue).toBe('push');

    const [mark] = dynamo.commandCalls(UpdateItemCommand);
    expect(mark?.args[0].input.ExpressionAttributeValues).toEqual({ ':enqueued': { S: 'enqueued' } });

    expect(calls).toEqual(['claim', 'archive', 'enqueue', 'mark-enqueued']);
    expect(logs.some(({ record }) => record.msg === 'delivery accepted' && record.deliveryId === 'd-123')).toBe(true);
  });

  it('decodes base64 bodies and uses the Stripe event id as delivery id', async () => {
    const body = JSON.stringify({ id: 'evt_1', type: 'invoice.paid', data: { object: { amount_paid: 1200 } } });
    const response = await makeHandler()(
      event('stripe', body, { 'stripe-signature': signStripe(body, STRIPE_SECRET, NOW.getTime() / 1000) }, { base64: true }),
    );
    expect(response.statusCode).toBe(202);
    expect(parse(response.body).deliveryId).toBe('evt_1');
    const envelope = JSON.parse(sqs.commandCalls(SendMessageCommand)[0]?.args[0].input.MessageBody ?? '') as Envelope;
    expect(envelope).toMatchObject({ provider: 'stripe', deliveryId: 'evt_1', eventType: 'invoice.paid' });
  });

  it('caches secrets across requests and supports rotated secret lists', async () => {
    secretsManager
      .on(GetSecretValueCommand, { SecretId: 'arn:secret:github' })
      .resolves({ SecretString: JSON.stringify(['brand-new', GITHUB_SECRET]) });
    const handler = makeHandler();
    expect((await handler(githubEvent('{"a":1}'))).statusCode).toBe(202);
    expect((await handler(githubEvent('{"a":2}', { 'x-github-delivery': 'd-124' }))).statusCode).toBe(202);
    expect(secretsManager.commandCalls(GetSecretValueCommand)).toHaveLength(1);
  });
});

describe('rejected requests', () => {
  it('answers 404 for an unknown provider without touching AWS', async () => {
    const response = await makeHandler()(event('gitlab', '{}', {}));
    expect(response.statusCode).toBe(404);
    expect(secretsManager.calls()).toHaveLength(0);
    expect(dynamo.calls()).toHaveLength(0);
  });

  it('does not treat inherited object keys as providers', async () => {
    expect((await makeHandler()(event('constructor', '{}', {}))).statusCode).toBe(404);
  });

  it('refuses an oversize payload announced by Content-Length before reading secrets', async () => {
    const response = await makeHandler()(githubEvent('{}', { 'content-length': '5000' }));
    expect(response.statusCode).toBe(413);
    expect(parse(response.body)).toEqual({ error: 'payload_too_large', limitBytes: 1024 });
    expect(secretsManager.calls()).toHaveLength(0);
  });

  it('refuses an oversize body even when Content-Length is missing or wrong', async () => {
    const body = JSON.stringify({ blob: 'x'.repeat(2000) });
    const response = await makeHandler()(githubEvent(body, { 'content-length': '10' }));
    expect(response.statusCode).toBe(413);
    expect(dynamo.calls()).toHaveLength(0);
  });

  it('accepts a body exactly at the size limit', async () => {
    const body = JSON.stringify({ blob: 'x'.repeat(1024 - 11) });
    expect(Buffer.byteLength(body)).toBe(1024);
    expect((await makeHandler()(githubEvent(body))).statusCode).toBe(202);
  });

  it('answers 401 for a bad signature and logs the reason without returning it', async () => {
    const response = await makeHandler()(githubEvent('{"a":1}', { 'x-hub-signature-256': signGithub('{"a":2}', GITHUB_SECRET) }));
    expect(response.statusCode).toBe(401);
    expect(parse(response.body)).toEqual({ error: 'invalid_signature' });
    expect(dynamo.calls()).toHaveLength(0);
    expect(s3.calls()).toHaveLength(0);
    expect(sqs.calls()).toHaveLength(0);
    expect(logs).toContainEqual({
      level: 'warn',
      record: expect.objectContaining({ msg: 'signature rejected', reason: 'signature_mismatch', provider: 'github' }),
    });
  });

  it('answers 401 for a missing signature header', async () => {
    const response = await makeHandler()(
      event('github', '{}', { 'x-github-delivery': 'd-1', 'x-github-event': 'push' }),
    );
    expect(response.statusCode).toBe(401);
  });

  it('answers 401 for a replayed Stripe request outside the tolerance window', async () => {
    const body = JSON.stringify({ id: 'evt_old', type: 'charge.succeeded' });
    const tenMinutesAgo = NOW.getTime() / 1000 - 600;
    const response = await makeHandler()(
      event('stripe', body, { 'stripe-signature': signStripe(body, STRIPE_SECRET, tenMinutesAgo) }),
    );
    expect(response.statusCode).toBe(401);
    expect(logs.some(({ record }) => record.reason === 'timestamp_out_of_tolerance')).toBe(true);
    expect(dynamo.calls()).toHaveLength(0);
  });

  it('answers 400 when a signed GitHub request lacks the delivery header', async () => {
    const body = '{}';
    const response = await makeHandler()(
      event('github', body, { 'x-github-event': 'push', 'x-hub-signature-256': signGithub(body, GITHUB_SECRET) }),
    );
    expect(response.statusCode).toBe(400);
    expect(parse(response.body).error).toBe('malformed_delivery');
  });

  it('answers 400 when a signed Stripe body is not an event', async () => {
    for (const body of ['not json', '{"type":"x"}']) {
      const response = await makeHandler()(
        event('stripe', body, { 'stripe-signature': signStripe(body, STRIPE_SECRET, NOW.getTime() / 1000) }),
      );
      expect(response.statusCode).toBe(400);
    }
  });
});

describe('de-duplication', () => {
  it('acknowledges a delivery that was already enqueued without enqueueing it again', async () => {
    dynamo.on(PutItemCommand).rejects(
      new ConditionalCheckFailedException({
        message: 'The conditional request failed',
        $metadata: {},
        Item: { pk: { S: 'github#d-123' }, status: { S: 'enqueued' } },
      }),
    );
    const response = await makeHandler()(githubEvent('{"a":1}'));
    expect(response.statusCode).toBe(200);
    expect(parse(response.body)).toEqual({ status: 'duplicate', deliveryId: 'd-123' });
    expect(s3.calls()).toHaveLength(0);
    expect(sqs.calls()).toHaveLength(0);
  });

  it('answers 409 while another attempt holds a fresh claim, so the sender retries later', async () => {
    dynamo.on(PutItemCommand).rejects(
      new ConditionalCheckFailedException({
        message: 'The conditional request failed',
        $metadata: {},
        Item: { pk: { S: 'github#d-123' }, status: { S: 'pending' } },
      }),
    );
    const response = await makeHandler()(githubEvent('{"a":1}'));
    expect(response.statusCode).toBe(409);
    expect(sqs.calls()).toHaveLength(0);
  });

  it('keys claims by provider, so equal ids from different providers do not collide', async () => {
    const body = JSON.stringify({ id: 'd-123', type: 'charge.succeeded' });
    await makeHandler()(event('stripe', body, { 'stripe-signature': signStripe(body, STRIPE_SECRET, NOW.getTime() / 1000) }));
    expect(dynamo.commandCalls(PutItemCommand)[0]?.args[0].input.Item?.pk).toEqual({ S: 'stripe#d-123' });
  });
});

describe('failure handling', () => {
  it('releases its claim and answers 500 when archiving fails', async () => {
    s3.on(PutObjectCommand).rejects(new Error('S3 unavailable'));
    const response = await makeHandler()(githubEvent('{"a":1}'));

    expect(response.statusCode).toBe(500);
    expect(calls).toEqual(['claim', 'release']);
    const [release] = dynamo.commandCalls(DeleteItemCommand);
    expect(release?.args[0].input).toMatchObject({
      Key: { pk: { S: 'github#d-123' } },
      ConditionExpression: '#status = :pending AND claimedAt = :claimedAt',
      ExpressionAttributeValues: { ':pending': { S: 'pending' }, ':claimedAt': { N: String(NOW.getTime()) } },
    });
  });

  it('releases its claim and answers 500 when enqueueing fails', async () => {
    sqs.on(SendMessageCommand).rejects(new Error('throttled'));
    const response = await makeHandler()(githubEvent('{"a":1}'));
    expect(response.statusCode).toBe(500);
    expect(calls).toEqual(['claim', 'archive', 'release']);
  });

  it('still answers 500 (and logs) when the claim cannot be released', async () => {
    sqs.on(SendMessageCommand).rejects(new Error('throttled'));
    dynamo.on(DeleteItemCommand).rejects(new Error('DynamoDB unavailable'));
    const response = await makeHandler()(githubEvent('{"a":1}'));
    expect(response.statusCode).toBe(500);
    expect(logs.some(({ level, record }) => level === 'error' && record.msg === 'could not release claim')).toBe(true);
  });

  it('answers 202 when the message is enqueued but the claim cannot be marked', async () => {
    dynamo.on(UpdateItemCommand).rejects(new Error('DynamoDB unavailable'));
    const response = await makeHandler()(githubEvent('{"a":1}'));
    expect(response.statusCode).toBe(202);
    expect(logs.some(({ level, record }) => level === 'warn' && record.msg === 'could not mark delivery enqueued')).toBe(
      true,
    );
  });

  it('answers 500 when the signing secret cannot be loaded', async () => {
    secretsManager.on(GetSecretValueCommand).rejects(new Error('AccessDenied'));
    const response = await makeHandler()(githubEvent('{"a":1}'));
    expect(response.statusCode).toBe(500);
    expect(parse(response.body)).toEqual({ error: 'internal_error' });
    expect(dynamo.calls()).toHaveLength(0);
  });

  it('answers 500 when the claim write fails for a reason other than the condition', async () => {
    dynamo.on(PutItemCommand).rejects(new Error('ProvisionedThroughputExceeded'));
    const response = await makeHandler()(githubEvent('{"a":1}'));
    expect(response.statusCode).toBe(500);
    expect(s3.calls()).toHaveLength(0);
  });
});

describe('configFromEnv', () => {
  it('parses the environment the stack provides', () => {
    const parsed = configFromEnv({
      PROVIDERS: JSON.stringify({ github: { scheme: 'github', secretId: 'arn:1' }, pay: { scheme: 'stripe', secretId: 'arn:2' } }),
      DEDUPE_TABLE_NAME: 't',
      RAW_BUCKET_NAME: 'b',
      QUEUE_URL: 'https://sqs.eu-central-1.amazonaws.com/1/q',
    });
    expect(parsed.providers.pay).toEqual({ scheme: 'stripe', secretId: 'arn:2', toleranceSeconds: 300 });
    expect(parsed.maxBodyBytes).toBe(1024 * 1024);
    expect(parsed.staleClaimSeconds).toBe(120);
  });

  it('rejects unknown schemes and bad provider names', () => {
    const base = { DEDUPE_TABLE_NAME: 't', RAW_BUCKET_NAME: 'b', QUEUE_URL: 'https://sqs.example.com/q' };
    expect(() => configFromEnv({ ...base, PROVIDERS: '{"x":{"scheme":"md5","secretId":"s"}}' })).toThrow();
    expect(() => configFromEnv({ ...base, PROVIDERS: '{"Bad Name":{"scheme":"github","secretId":"s"}}' })).toThrow();
  });
});
