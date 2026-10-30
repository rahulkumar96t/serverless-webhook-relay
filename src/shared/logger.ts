/**
 * Minimal structured logger. Each call passes one object to `console`; with the function's log
 * format set to JSON, Lambda writes it as the `message` object next to timestamp, level and
 * requestId, so every field is queryable in CloudWatch Logs Insights (e.g. `message.msg`,
 * `message.deliveryId`). Lambda filters by level through `applicationLogLevel`.
 */
export type LogLevel = 'debug' | 'info' | 'warn' | 'error';
export type LogFields = Record<string, unknown>;

export interface Logger {
  debug(message: string, fields?: LogFields): void;
  info(message: string, fields?: LogFields): void;
  warn(message: string, fields?: LogFields): void;
  error(message: string, fields?: LogFields): void;
  child(fields: LogFields): Logger;
}

export function createLogger(base: LogFields = {}): Logger {
  const emit = (level: LogLevel, message: string, fields: LogFields = {}) => {
    console[level]({ msg: message, ...base, ...serialiseErrors(fields) });
  };
  return {
    debug: (message, fields) => emit('debug', message, fields),
    info: (message, fields) => emit('info', message, fields),
    warn: (message, fields) => emit('warn', message, fields),
    error: (message, fields) => emit('error', message, fields),
    child: (fields) => createLogger({ ...base, ...fields }),
  };
}

/** Error objects serialise to `{}` in JSON; turn them into plain fields. */
function serialiseErrors(fields: LogFields): LogFields {
  const out: LogFields = {};
  for (const [key, value] of Object.entries(fields)) {
    out[key] = value instanceof Error ? { name: value.name, message: value.message } : value;
  }
  return out;
}
