import { fileURLToPath } from 'node:url';
import { CfnOutput, Duration, RemovalPolicy, Stack, type StackProps } from 'aws-cdk-lib';
import { AccessLogFormat } from 'aws-cdk-lib/aws-apigateway';
import { HttpApi, HttpMethod, HttpStage, LogGroupLogDestination } from 'aws-cdk-lib/aws-apigatewayv2';
import { HttpLambdaIntegration } from 'aws-cdk-lib/aws-apigatewayv2-integrations';
import { Alarm, ComparisonOperator, type IMetric, TreatMissingData } from 'aws-cdk-lib/aws-cloudwatch';
import { SnsAction } from 'aws-cdk-lib/aws-cloudwatch-actions';
import { AttributeType, Billing, TableV2 } from 'aws-cdk-lib/aws-dynamodb';
import { EventBus, Match, Rule } from 'aws-cdk-lib/aws-events';
import { SqsQueue } from 'aws-cdk-lib/aws-events-targets';
import { PolicyStatement, Role, ServicePrincipal } from 'aws-cdk-lib/aws-iam';
import { ApplicationLogLevel, Architecture, LoggingFormat, Runtime, SystemLogLevel } from 'aws-cdk-lib/aws-lambda';
import { SqsEventSource } from 'aws-cdk-lib/aws-lambda-event-sources';
import { NodejsFunction, type NodejsFunctionProps } from 'aws-cdk-lib/aws-lambda-nodejs';
import { FilterPattern, LogGroup, RetentionDays } from 'aws-cdk-lib/aws-logs';
import { BlockPublicAccess, Bucket, BucketEncryption, ObjectOwnership } from 'aws-cdk-lib/aws-s3';
import { Secret } from 'aws-cdk-lib/aws-secretsmanager';
import { Topic } from 'aws-cdk-lib/aws-sns';
import { EmailSubscription } from 'aws-cdk-lib/aws-sns-subscriptions';
import { Queue, QueueEncryption } from 'aws-cdk-lib/aws-sqs';
import type { Construct } from 'constructs';
import { RAW_PREFIX } from '../src/shared/envelope.js';

export interface ProviderDefinition {
  /** Path segment: POST /webhooks/<name>. Lower-case letters, digits and dashes. */
  name: string;
  scheme: 'github' | 'stripe';
  /** Stripe scheme only: accepted clock skew / replay window. */
  toleranceSeconds?: number;
}

export interface WebhookRelayStackProps extends StackProps {
  providers?: ProviderDefinition[];
  /** Subscribe this address to the alarm topic (confirmation e-mail required). */
  alarmEmail?: string;
  maxBodyBytes?: number;
  rawPayloadRetention?: Duration;
  dedupeWindow?: Duration;
  throttle?: { rateLimit: number; burstLimit: number };
}

export const DEFAULT_PROVIDERS: ProviderDefinition[] = [
  { name: 'github', scheme: 'github' },
  { name: 'stripe', scheme: 'stripe', toleranceSeconds: 300 },
];

export const EVENT_SOURCE_PREFIX = 'webhook-relay';

const INGEST_TIMEOUT = Duration.seconds(10);
const CONSUMER_TIMEOUT = Duration.seconds(30);
const MAX_RECEIVE_COUNT = 5;

const entry = (relative: string) => fileURLToPath(new URL(relative, import.meta.url));

/**
 * HTTP API -> ingest Lambda (verify, de-duplicate, archive, enqueue) -> SQS (+DLQ)
 * -> consumer Lambda (partial batch failures) -> EventBridge bus -> rules.
 */
export class WebhookRelayStack extends Stack {
  readonly api: HttpApi;
  readonly queue: Queue;
  readonly deadLetterQueue: Queue;
  readonly bus: EventBus;

  constructor(scope: Construct, id: string, props: WebhookRelayStackProps = {}) {
    super(scope, id, props);

    const providers = props.providers ?? DEFAULT_PROVIDERS;
    for (const provider of providers) {
      if (!/^[a-z0-9-]{1,32}$/.test(provider.name)) {
        throw new Error(`invalid provider name "${provider.name}": use 1-32 lower-case letters, digits or dashes`);
      }
    }

    const alarmTopic = new Topic(this, 'AlarmTopic', { displayName: 'Webhook relay alarms', enforceSSL: true });
    if (props.alarmEmail) alarmTopic.addSubscription(new EmailSubscription(props.alarmEmail));

    // --- storage -------------------------------------------------------------------------
    const rawBucket = new Bucket(this, 'RawPayloads', {
      blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
      encryption: BucketEncryption.S3_MANAGED,
      enforceSSL: true,
      objectOwnership: ObjectOwnership.BUCKET_OWNER_ENFORCED,
      lifecycleRules: [{ prefix: RAW_PREFIX, expiration: props.rawPayloadRetention ?? Duration.days(30) }],
      removalPolicy: RemovalPolicy.RETAIN,
    });

    const deliveries = new TableV2(this, 'Deliveries', {
      partitionKey: { name: 'pk', type: AttributeType.STRING },
      billing: Billing.onDemand(),
      timeToLiveAttribute: 'expiresAt',
      // Only de-duplication state lives here; it is safe to lose with the stack.
      removalPolicy: RemovalPolicy.DESTROY,
    });

    // --- queueing ------------------------------------------------------------------------
    this.deadLetterQueue = new Queue(this, 'DeadLetterQueue', {
      retentionPeriod: Duration.days(14),
      encryption: QueueEncryption.SQS_MANAGED,
      enforceSSL: true,
    });
    this.queue = new Queue(this, 'DeliveryQueue', {
      // AWS guidance: at least six times the consumer timeout, so retries are not premature.
      visibilityTimeout: Duration.seconds(CONSUMER_TIMEOUT.toSeconds() * 6),
      retentionPeriod: Duration.days(4),
      encryption: QueueEncryption.SQS_MANAGED,
      enforceSSL: true,
      deadLetterQueue: { queue: this.deadLetterQueue, maxReceiveCount: MAX_RECEIVE_COUNT },
    });

    // --- event bus -----------------------------------------------------------------------
    this.bus = new EventBus(this, 'Bus', { description: 'Verified, de-duplicated third-party webhooks' });
    this.bus.archive('Archive', {
      description: 'Replayable copy of relayed webhooks',
      eventPattern: { source: Match.prefix(EVENT_SOURCE_PREFIX) },
      retention: Duration.days(30),
    });

    // --- secrets -------------------------------------------------------------------------
    const secrets = providers.map(
      (provider) =>
        new Secret(this, `${pascal(provider.name)}SigningSecret`, {
          description: `Webhook signing secret for provider "${provider.name}" (${provider.scheme} scheme). A plain string, or a JSON array of strings during rotation.`,
          generateSecretString: { passwordLength: 48, excludePunctuation: true },
        }),
    );

    // --- functions -----------------------------------------------------------------------
    const functionDefaults: NodejsFunctionProps = {
      runtime: Runtime.NODEJS_24_X,
      architecture: Architecture.ARM_64,
      memorySize: 256,
      loggingFormat: LoggingFormat.JSON,
      applicationLogLevelV2: ApplicationLogLevel.INFO,
      systemLogLevelV2: SystemLogLevel.WARN,
      bundling: {
        minify: true,
        sourceMap: true,
        target: 'node24',
        // Bundle the pinned AWS SDK instead of relying on the runtime-provided version.
        externalModules: [],
      },
    };

    // Each function gets a role that can write only to its own log group, instead of the
    // AWS-managed basic execution policy (which allows logs:* actions on every log group).
    const functionRole = (id: string, logGroup: LogGroup) => {
      const role = new Role(this, id, { assumedBy: new ServicePrincipal('lambda.amazonaws.com') });
      logGroup.grantWrite(role);
      return role;
    };

    const ingestLogs = new LogGroup(this, 'IngestLogs', {
      retention: RetentionDays.ONE_MONTH,
      removalPolicy: RemovalPolicy.DESTROY,
    });
    const ingestFn = new NodejsFunction(this, 'IngestFunction', {
      ...functionDefaults,
      description: 'Verifies, de-duplicates, archives and enqueues webhook deliveries',
      entry: entry('../src/ingest/handler.ts'),
      handler: 'handler',
      timeout: INGEST_TIMEOUT,
      logGroup: ingestLogs,
      role: functionRole('IngestRole', ingestLogs),
      environment: {
        NODE_OPTIONS: '--enable-source-maps',
        PROVIDERS: JSON.stringify(
          Object.fromEntries(
            providers.map((provider, index) => [
              provider.name,
              {
                scheme: provider.scheme,
                secretId: secrets[index]?.secretArn,
                ...(provider.scheme === 'stripe' ? { toleranceSeconds: provider.toleranceSeconds ?? 300 } : {}),
              },
            ]),
          ),
        ),
        DEDUPE_TABLE_NAME: deliveries.tableName,
        RAW_BUCKET_NAME: rawBucket.bucketName,
        QUEUE_URL: this.queue.queueUrl,
        MAX_BODY_BYTES: String(props.maxBodyBytes ?? 1024 * 1024),
        DEDUPE_TTL_SECONDS: String((props.dedupeWindow ?? Duration.days(7)).toSeconds()),
        // Must exceed the function timeout: a pending claim older than this belongs to a dead attempt.
        STALE_CLAIM_SECONDS: String(INGEST_TIMEOUT.toSeconds() * 12),
      },
    });

    const consumerLogs = new LogGroup(this, 'ConsumerLogs', {
      retention: RetentionDays.ONE_MONTH,
      removalPolicy: RemovalPolicy.DESTROY,
    });
    const consumerFn = new NodejsFunction(this, 'ConsumerFunction', {
      ...functionDefaults,
      description: 'Publishes queued webhook deliveries to the EventBridge bus',
      entry: entry('../src/consumer/handler.ts'),
      handler: 'handler',
      timeout: CONSUMER_TIMEOUT,
      logGroup: consumerLogs,
      role: functionRole('ConsumerRole', consumerLogs),
      environment: {
        NODE_OPTIONS: '--enable-source-maps',
        EVENT_BUS_NAME: this.bus.eventBusName,
        EVENT_SOURCE_PREFIX: EVENT_SOURCE_PREFIX,
      },
    });
    consumerFn.addEventSource(
      new SqsEventSource(this.queue, {
        batchSize: 10,
        maxBatchingWindow: Duration.seconds(1),
        reportBatchItemFailures: true,
      }),
    );

    // --- least-privilege grants ----------------------------------------------------------
    deliveries.grant(ingestFn, 'dynamodb:PutItem', 'dynamodb:UpdateItem', 'dynamodb:DeleteItem');
    ingestFn.addToRolePolicy(
      new PolicyStatement({ actions: ['s3:PutObject'], resources: [rawBucket.arnForObjects(`${RAW_PREFIX}*`)] }),
    );
    this.queue.grantSendMessages(ingestFn);
    for (const secret of secrets) secret.grantRead(ingestFn);

    consumerFn.addToRolePolicy(
      new PolicyStatement({ actions: ['s3:GetObject'], resources: [rawBucket.arnForObjects(`${RAW_PREFIX}*`)] }),
    );
    this.bus.grantPutEventsTo(consumerFn);

    // --- HTTP API ------------------------------------------------------------------------
    this.api = new HttpApi(this, 'Api', {
      description: 'Public endpoint for third-party webhooks',
      createDefaultStage: false,
    });
    this.api.addRoutes({
      path: '/webhooks/{provider}',
      methods: [HttpMethod.POST],
      integration: new HttpLambdaIntegration('IngestIntegration', ingestFn),
    });
    const accessLogs = new LogGroup(this, 'ApiAccessLogs', {
      retention: RetentionDays.ONE_MONTH,
      removalPolicy: RemovalPolicy.DESTROY,
    });
    const stage = new HttpStage(this, 'DefaultStage', {
      httpApi: this.api,
      autoDeploy: true,
      throttle: props.throttle ?? { rateLimit: 50, burstLimit: 100 },
      accessLogSettings: {
        destination: new LogGroupLogDestination(accessLogs),
        format: AccessLogFormat.custom(
          JSON.stringify({
            requestId: '$context.requestId',
            sourceIp: '$context.identity.sourceIp',
            routeKey: '$context.routeKey',
            path: '$context.path',
            status: '$context.status',
            responseLength: '$context.responseLength',
            latencyMs: '$context.responseLatency',
            integrationError: '$context.integrationErrorMessage',
          }),
        ),
      },
    });

    // --- example consumer of the bus -----------------------------------------------------
    // A downstream team would own a rule like this: GitHub pushes to main, delivered to their
    // queue, with EventBridge's own retry policy and DLQ for target failures.
    const exampleTargetDlq = new Queue(this, 'ExampleTargetDeadLetterQueue', {
      retentionPeriod: Duration.days(14),
      encryption: QueueEncryption.SQS_MANAGED,
      enforceSSL: true,
    });
    const exampleQueue = new Queue(this, 'ExampleGithubPushQueue', {
      encryption: QueueEncryption.SQS_MANAGED,
      enforceSSL: true,
    });
    new Rule(this, 'ExampleGithubPushToMain', {
      eventBus: this.bus,
      description: 'Example: GitHub pushes to main, delivered to a queue',
      eventPattern: {
        source: [`${EVENT_SOURCE_PREFIX}.github`],
        detailType: ['push'],
        detail: { payload: { ref: ['refs/heads/main'] } },
      },
      targets: [
        new SqsQueue(exampleQueue, {
          deadLetterQueue: exampleTargetDlq,
          retryAttempts: 8,
          maxEventAge: Duration.hours(6),
        }),
      ],
    });

    // --- alarms --------------------------------------------------------------------------
    const signatureRejections = ingestLogs
      .addMetricFilter('SignatureRejections', {
        filterPattern: FilterPattern.stringValue('$.message.msg', '=', 'signature rejected'),
        metricNamespace: 'WebhookRelay',
        metricName: 'SignatureRejected',
        metricValue: '1',
        defaultValue: 0,
      })
      .metric({ statistic: 'Sum', period: Duration.minutes(5) });

    const alarm = (id: string, metric: IMetric, threshold: number, description: string, evaluationPeriods = 1) => {
      const created = new Alarm(this, id, {
        metric,
        threshold,
        evaluationPeriods,
        comparisonOperator: ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
        treatMissingData: TreatMissingData.NOT_BREACHING,
        alarmDescription: description,
      });
      created.addAlarmAction(new SnsAction(alarmTopic));
      created.addOkAction(new SnsAction(alarmTopic));
      return created;
    };

    alarm(
      'DeadLetterQueueNotEmpty',
      this.deadLetterQueue.metricApproximateNumberOfMessagesVisible({ statistic: 'Maximum', period: Duration.minutes(1) }),
      1,
      'Deliveries failed to publish after all retries and are parked in the DLQ.',
    );
    alarm(
      'IngestFunctionErrors',
      ingestFn.metricErrors({ statistic: 'Sum', period: Duration.minutes(5) }),
      1,
      'The ingest function crashed or timed out.',
    );
    alarm(
      'ConsumerFunctionErrors',
      consumerFn.metricErrors({ statistic: 'Sum', period: Duration.minutes(5) }),
      1,
      'The consumer function crashed or timed out (whole batches are retried).',
    );
    alarm(
      'ApiServerErrors',
      this.api.metricServerError({ statistic: 'Sum', period: Duration.minutes(5) }),
      5,
      'The webhook endpoint answered 5xx; senders will retry, but something is failing.',
    );
    alarm(
      'QueueBacklog',
      this.queue.metricApproximateAgeOfOldestMessage({ statistic: 'Maximum', period: Duration.minutes(5) }),
      600,
      'Deliveries have waited more than 10 minutes to be published.',
      2,
    );
    alarm(
      'SignatureRejectionSpike',
      signatureRejections,
      20,
      'Many requests failed signature verification: a rotated secret, or someone probing the endpoint.',
    );

    // --- outputs -------------------------------------------------------------------------
    new CfnOutput(this, 'WebhookUrlTemplate', { value: `${stage.url}webhooks/{provider}` });
    new CfnOutput(this, 'EventBusName', { value: this.bus.eventBusName });
    new CfnOutput(this, 'DeadLetterQueueUrl', { value: this.deadLetterQueue.queueUrl });
    new CfnOutput(this, 'ExampleQueueUrl', { value: exampleQueue.queueUrl });
    providers.forEach((provider, index) => {
      new CfnOutput(this, `${pascal(provider.name)}SecretArn`, { value: secrets[index]?.secretArn ?? '' });
    });
  }
}

function pascal(name: string): string {
  return name
    .split('-')
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join('');
}
