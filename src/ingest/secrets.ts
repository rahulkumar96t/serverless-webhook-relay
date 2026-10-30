import { GetSecretValueCommand, type SecretsManagerClient } from '@aws-sdk/client-secrets-manager';

/**
 * Loads webhook signing secrets from Secrets Manager and caches them per Lambda instance.
 *
 * A secret value is either a plain string or a JSON array of strings. The array form supports
 * rotation: put the new secret first and keep the old one until every sender has switched.
 */
export class SecretCache {
  private readonly entries = new Map<string, { values: string[]; expiresAt: number }>();

  constructor(
    private readonly client: SecretsManagerClient,
    private readonly ttlMs = 5 * 60 * 1000,
    private readonly clock: () => number = Date.now,
  ) {}

  async get(secretId: string): Promise<string[]> {
    const cached = this.entries.get(secretId);
    if (cached && cached.expiresAt > this.clock()) return cached.values;

    const response = await this.client.send(new GetSecretValueCommand({ SecretId: secretId }));
    const values = parseSecret(response.SecretString);
    if (values.length === 0) {
      throw new Error(`secret ${secretId} is empty or not a string / array of strings`);
    }
    this.entries.set(secretId, { values, expiresAt: this.clock() + this.ttlMs });
    return values;
  }
}

export function parseSecret(raw: string | undefined): string[] {
  const text = raw?.trim();
  if (!text) return [];
  if (text.startsWith('[')) {
    try {
      const parsed: unknown = JSON.parse(text);
      if (Array.isArray(parsed)) {
        return parsed.filter((value): value is string => typeof value === 'string' && value.length > 0);
      }
    } catch {
      // Not JSON after all: fall through and treat it as a literal secret.
    }
  }
  return [text];
}
