#!/usr/bin/env node
import { App } from 'aws-cdk-lib';
import { WebhookRelayStack } from '../lib/webhook-relay-stack.js';

const app = new App();

// Environment-agnostic by default, so `cdk synth` needs no AWS account or credentials.
// For a deployment, CDK_DEFAULT_ACCOUNT / CDK_DEFAULT_REGION come from the active AWS profile.
const env =
  process.env.CDK_DEFAULT_ACCOUNT && process.env.CDK_DEFAULT_REGION
    ? { account: process.env.CDK_DEFAULT_ACCOUNT, region: process.env.CDK_DEFAULT_REGION }
    : undefined;

new WebhookRelayStack(app, 'WebhookRelay', {
  env,
  alarmEmail: app.node.tryGetContext('alarmEmail') as string | undefined,
  description: 'Reference architecture: verified, de-duplicated webhook ingestion to EventBridge',
});
