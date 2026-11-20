// The Kafka client's `tls` / `sasl` options for an external broker, built from
// the already-validated `KafkaSecurityConfig`. The only thing that can still
// fail here is reading the CA file, and it fails with the path in the message.

import { readFileSync } from 'node:fs';
import type { ConnectionOptions } from 'node:tls';
import type { SASLOptions } from '@platformatic/kafka';
import type { KafkaSaslMechanism, KafkaSecurityConfig } from '../../../config';

/** The client names mechanisms the way the Kafka protocol does. */
const SASL_MECHANISM_NAMES = {
  plain: 'PLAIN',
  'scram-sha-256': 'SCRAM-SHA-256',
  'scram-sha-512': 'SCRAM-SHA-512',
} as const satisfies Record<KafkaSaslMechanism, SASLOptions['mechanism']>;

export interface ResolvedKafkaSecurity {
  /** Undefined is plaintext; an empty object is TLS with the system trust store. */
  tls: ConnectionOptions | undefined;
  sasl: SASLOptions | undefined;
  /** As configured, for the log line: the client's own spelling is upper case. */
  saslMechanism: KafkaSaslMechanism | null;
}

export type ReadPemFile = (path: string) => string;

const readPemFile: ReadPemFile = (path) => readFileSync(path, 'utf8');

const resolveTls = (
  ssl: KafkaSecurityConfig['ssl'],
  readFile: ReadPemFile
): ConnectionOptions | undefined => {
  if (!ssl.enabled) {
    return;
  }
  const options: ConnectionOptions = {};
  if (ssl.caPath) {
    try {
      options.ca = [readFile(ssl.caPath)];
    } catch (err) {
      throw new Error(
        `Failed to read KAFKA_SSL_CA_PATH "${ssl.caPath}": ${err instanceof Error ? err.message : String(err)}`
      );
    }
  }
  if (ssl.rejectUnauthorized !== undefined) {
    options.rejectUnauthorized = ssl.rejectUnauthorized;
  }
  return options;
};

export const resolveKafkaSecurity = (
  security: KafkaSecurityConfig,
  readFile: ReadPemFile = readPemFile
): ResolvedKafkaSecurity => ({
  tls: resolveTls(security.ssl, readFile),
  sasl: security.sasl
    ? {
        mechanism: SASL_MECHANISM_NAMES[security.sasl.mechanism],
        username: security.sasl.username,
        password: security.sasl.password,
      }
    : undefined,
  saslMechanism: security.sasl?.mechanism ?? null,
});

/** Log-safe summary: never includes credential values. */
export const describeKafkaSecurity = (
  security: ResolvedKafkaSecurity
): { ssl: boolean; sasl: KafkaSaslMechanism | null } => ({
  ssl: security.tls !== undefined,
  sasl: security.saslMechanism,
});
