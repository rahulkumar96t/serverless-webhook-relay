import { readFileSync } from 'node:fs';
import { App } from 'aws-cdk-lib';
import { Match, Template } from 'aws-cdk-lib/assertions';
import { beforeAll, describe, expect, it } from 'vitest';
import { WebhookRelayStack } from '../lib/webhook-relay-stack.js';

// Synthesise with the same feature flags the CDK CLI reads from cdk.json.
const cdkContext = JSON.parse(readFileSync(new URL('../cdk.json', import.meta.url), 'utf8')).context as Record<
  string,
  unknown
>;

function synth(props: ConstructorParameters<typeof WebhookRelayStack>[2] = {}): Template {
  const app = new App({ context: cdkContext });
  return Template.fromStack(new WebhookRelayStack(app, 'TestRelay', props));
}

type PolicyStatement = { Action: string | string[]; Resource: unknown; Effect: string };

function statementsOf(template: Template, logicalIdPrefix: string): PolicyStatement[] {
  const policies = template.findResources('AWS::IAM::Policy');
  const match = Object.entries(policies).find(([id]) => id.startsWith(logicalIdPrefix));
  if (!match) throw new Error(`no policy starting with ${logicalIdPrefix}`);
  return (match[1] as { Properties: { PolicyDocument: { Statement: PolicyStatement[] } } }).Properties.PolicyDocument
    .Statement;
}

const actionsOf = (statements: PolicyStatement[]) =>
  statements.flatMap((statement) => (Array.isArray(statement.Action) ? statement.Action : [statement.Action])).sort();

let template: Template;

beforeAll(() => {
  template = synth({ alarmEmail: 'alerts@example.com' });
});

describe('HTTP API', () => {
  it('exposes exactly one route: POST /webhooks/{provider}', () => {
    template.resourceCountIs('AWS::ApiGatewayV2::Route', 1);
    template.hasResourceProperties('AWS::ApiGatewayV2::Route', { RouteKey: 'POST /webhooks/{provider}' });
    template.hasResourceProperties('AWS::ApiGatewayV2::Integration', {
      IntegrationType: 'AWS_PROXY',
      PayloadFormatVersion: '2.0',
    });
  });

  it('throttles the stage and writes JSON access logs', () => {
    template.hasResourceProperties('AWS::ApiGatewayV2::Stage', {
      StageName: '$default',
      AutoDeploy: true,
      DefaultRouteSettings: { ThrottlingRateLimit: 50, ThrottlingBurstLimit: 100 },
      AccessLogSettings: {
        DestinationArn: Match.anyValue(),
        Format: Match.stringLikeRegexp('"requestId":"\\$context.requestId"'),
      },
    });
  });
});

describe('functions', () => {
  it('runs both functions on Node.js 24 / arm64 with JSON logs and bounded timeouts', () => {
    const functions = template.findResources('AWS::Lambda::Function');
    expect(Object.keys(functions)).toHaveLength(2);
    for (const fn of Object.values(functions) as Array<{ Properties: Record<string, unknown> }>) {
      expect(fn.Properties).toMatchObject({
        Runtime: 'nodejs24.x',
        Architectures: ['arm64'],
        LoggingConfig: { LogFormat: 'JSON', ApplicationLogLevel: 'INFO' },
      });
      expect(fn.Properties.Timeout).toBeLessThanOrEqual(30);
    }
  });

  it('passes each provider its scheme and secret ARN', () => {
    const ingest = Object.values(template.findResources('AWS::Lambda::Function')).find((fn) =>
      JSON.stringify(fn).includes('PROVIDERS'),
    ) as { Properties: { Environment: { Variables: Record<string, unknown> } } };
    const providers = JSON.stringify(ingest.Properties.Environment.Variables.PROVIDERS);
    expect(providers).toContain('\\"github\\":{\\"scheme\\":\\"github\\",\\"secretId\\":\\"');
    expect(providers).toContain('\\"toleranceSeconds\\":300');
    template.resourceCountIs('AWS::SecretsManager::Secret', 2);
  });

  it('consumes the queue with partial batch failure reporting', () => {
    template.hasResourceProperties('AWS::Lambda::EventSourceMapping', {
      BatchSize: 10,
      MaximumBatchingWindowInSeconds: 1,
      FunctionResponseTypes: ['ReportBatchItemFailures'],
    });
  });
});

describe('queueing and storage', () => {
  it('dead-letters a message after five failed receives', () => {
    template.hasResourceProperties('AWS::SQS::Queue', {
      VisibilityTimeout: 180,
      SqsManagedSseEnabled: true,
      RedrivePolicy: { deadLetterTargetArn: Match.anyValue(), maxReceiveCount: 5 },
    });
    template.hasResourceProperties('AWS::SQS::Queue', { MessageRetentionPeriod: 14 * 24 * 3600 });
  });

  it('refuses non-TLS access to every queue', () => {
    const policies = Object.values(template.findResources('AWS::SQS::QueuePolicy'));
    expect(policies.length).toBeGreaterThanOrEqual(4);
    for (const policy of policies) {
      expect(JSON.stringify(policy)).toContain('"aws:SecureTransport":"false"');
    }
  });

  it('de-duplicates with a TTL-enabled table keyed by delivery', () => {
    template.hasResourceProperties('AWS::DynamoDB::GlobalTable', {
      KeySchema: [{ AttributeName: 'pk', KeyType: 'HASH' }],
      BillingMode: 'PAY_PER_REQUEST',
      TimeToLiveSpecification: { AttributeName: 'expiresAt', Enabled: true },
    });
  });

  it('keeps raw payloads private, encrypted, TLS-only and expiring', () => {
    template.hasResourceProperties('AWS::S3::Bucket', {
      PublicAccessBlockConfiguration: {
        BlockPublicAcls: true,
        BlockPublicPolicy: true,
        IgnorePublicAcls: true,
        RestrictPublicBuckets: true,
      },
      BucketEncryption: {
        ServerSideEncryptionConfiguration: [{ ServerSideEncryptionByDefault: { SSEAlgorithm: 'AES256' } }],
      },
      LifecycleConfiguration: { Rules: [Match.objectLike({ Prefix: 'raw/', ExpirationInDays: 30, Status: 'Enabled' })] },
    });
    template.hasResource('AWS::S3::Bucket', { DeletionPolicy: 'Retain' });
    template.hasResourceProperties('AWS::S3::BucketPolicy', {
      PolicyDocument: {
        Statement: Match.arrayWith([
          Match.objectLike({ Effect: 'Deny', Condition: { Bool: { 'aws:SecureTransport': 'false' } } }),
        ]),
      },
    });
  });
});

describe('least-privilege IAM', () => {
  it('lets the ingest function do only what the request path needs', () => {
    expect(actionsOf(statementsOf(template, 'IngestRole'))).toEqual([
      'dynamodb:DeleteItem',
      'dynamodb:PutItem',
      'dynamodb:UpdateItem',
      'logs:CreateLogStream',
      'logs:PutLogEvents',
      's3:PutObject',
      'secretsmanager:DescribeSecret',
      'secretsmanager:GetSecretValue',
      'sqs:GetQueueAttributes',
      'sqs:GetQueueUrl',
      'sqs:SendMessage',
    ]);
  });

  it('lets the consumer function read the queue and raw objects and publish to the bus', () => {
    expect(actionsOf(statementsOf(template, 'ConsumerRole'))).toEqual([
      'events:PutEvents',
      'logs:CreateLogStream',
      'logs:PutLogEvents',
      's3:GetObject',
      'sqs:ChangeMessageVisibility',
      'sqs:DeleteMessage',
      'sqs:GetQueueAttributes',
      'sqs:GetQueueUrl',
      'sqs:ReceiveMessage',
    ]);
  });

  it('scopes object access to the raw/ prefix', () => {
    const s3Statements = [...statementsOf(template, 'IngestRole'), ...statementsOf(template, 'ConsumerRole')].filter(
      (statement) => String(statement.Action).startsWith('s3:'),
    );
    expect(s3Statements).toHaveLength(2);
    for (const statement of s3Statements) {
      expect(JSON.stringify(statement.Resource)).toContain('/raw/*');
    }
  });

  it('uses no wildcard actions or resources and no managed policies', () => {
    for (const policy of Object.values(template.findResources('AWS::IAM::Policy'))) {
      for (const statement of (policy as { Properties: { PolicyDocument: { Statement: PolicyStatement[] } } })
        .Properties.PolicyDocument.Statement) {
        const actions = Array.isArray(statement.Action) ? statement.Action : [statement.Action];
        expect(actions.some((action) => action === '*' || action.endsWith(':*'))).toBe(false);
        expect(statement.Resource).not.toBe('*');
      }
    }
    for (const role of Object.values(template.findResources('AWS::IAM::Role'))) {
      expect((role as { Properties: Record<string, unknown> }).Properties.ManagedPolicyArns).toBeUndefined();
    }
  });
});

describe('event bus', () => {
  it('archives relayed events for replay', () => {
    template.hasResourceProperties('AWS::Events::Archive', {
      EventPattern: { source: [{ prefix: 'webhook-relay' }] },
      RetentionDays: 30,
    });
  });

  it('includes an example rule with a retry policy and dead-letter queue', () => {
    template.hasResourceProperties('AWS::Events::Rule', {
      EventPattern: {
        source: ['webhook-relay.github'],
        'detail-type': ['push'],
        detail: { payload: { ref: ['refs/heads/main'] } },
      },
      Targets: [
        Match.objectLike({
          RetryPolicy: { MaximumRetryAttempts: 8, MaximumEventAgeInSeconds: 6 * 3600 },
          DeadLetterConfig: { Arn: Match.anyValue() },
        }),
      ],
    });
  });
});

describe('alarms', () => {
  it('alarms on a non-empty DLQ within a minute and notifies the topic', () => {
    template.hasResourceProperties('AWS::CloudWatch::Alarm', {
      MetricName: 'ApproximateNumberOfMessagesVisible',
      Namespace: 'AWS/SQS',
      Statistic: 'Maximum',
      Period: 60,
      Threshold: 1,
      ComparisonOperator: 'GreaterThanOrEqualToThreshold',
      AlarmActions: [Match.anyValue()],
    });
  });

  it('alarms on function errors, API 5xx, queue age and signature rejection spikes', () => {
    template.resourceCountIs('AWS::CloudWatch::Alarm', 6);
    for (const [metric, namespace] of [
      ['Errors', 'AWS/Lambda'],
      ['5xx', 'AWS/ApiGateway'],
      ['ApproximateAgeOfOldestMessage', 'AWS/SQS'],
      ['SignatureRejected', 'WebhookRelay'],
    ]) {
      template.hasResourceProperties('AWS::CloudWatch::Alarm', { MetricName: metric, Namespace: namespace });
    }
    template.hasResourceProperties('AWS::Logs::MetricFilter', {
      FilterPattern: '{ $.message.msg = "signature rejected" }',
    });
  });

  it('subscribes the configured address to the alarm topic', () => {
    template.hasResourceProperties('AWS::SNS::Subscription', { Protocol: 'email', Endpoint: 'alerts@example.com' });
  });
});

describe('configuration', () => {
  it('creates one secret per configured provider', () => {
    const custom = synth({ providers: [{ name: 'acme-ci', scheme: 'github' }] });
    custom.resourceCountIs('AWS::SecretsManager::Secret', 1);
    custom.resourceCountIs('AWS::SNS::Subscription', 0);
    custom.hasOutput('AcmeCiSecretArn', {});
  });

  it('rejects provider names that cannot be path segments', () => {
    expect(() => synth({ providers: [{ name: 'Not Valid', scheme: 'github' }] })).toThrow(/invalid provider name/);
  });
});
