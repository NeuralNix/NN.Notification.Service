import { Kafka, Producer, logLevel } from 'kafkajs';
import logger from '@/utils/logger';

const enabled = (process.env.KAFKA_ENABLED || '').toLowerCase() === 'true';
const brokers = (process.env.KAFKA_BROKERS || 'nn-kafka:9092').split(',');
const clientId = process.env.KAFKA_CLIENT_ID || 'notification-service';

const NOTIF_TOPIC = process.env.KAFKA_NOTIFICATIONS_TOPIC || 'notifications.realtime';
const EMAIL_TOPIC = process.env.KAFKA_EMAILS_OUTBOUND_TOPIC || 'emails.outbound';

let producer: Producer | null = null;

async function getProducer(): Promise<Producer | null> {
  if (!enabled) return null;
  if (producer) return producer;
  const kafka = new Kafka({ clientId, brokers, logLevel: logLevel.NOTHING });
  producer = kafka.producer({ idempotent: true, allowAutoTopicCreation: true });
  await producer.connect();
  logger.info('[kafka] Producer connected (brokers=%s)', brokers.join(','));
  return producer;
}

export async function publishRealtimeNotification(payload: {
  tenantId: string;
  userId?: string | null;
  type: string;
  title: string;
  message: string;
  severity?: 'info' | 'success' | 'warn' | 'error';
  /// Dashboards whose users may receive this as a tenant broadcast; empty or
  /// absent = unrestricted. The WebSocket Gateway filters tenant-room emits
  /// with this so realtime toasts match the bell's dashboard-access scoping.
  requiredDashboards?: string[];
  data?: Record<string, unknown>;
}) {
  const p = await getProducer();
  if (!p) {
    logger.info('[kafka:no-op] notifications.realtime → %j', payload);
    return;
  }
  await p.send({
    topic: NOTIF_TOPIC,
    messages: [{ key: payload.tenantId, value: JSON.stringify(payload) }],
  });
}

export async function publishEmailRequest(payload: {
  tenantId: string;
  to: string | string[];
  templateType: string;
  templateData?: Record<string, unknown>;
  priority?: 'low' | 'normal' | 'high';
}) {
  const p = await getProducer();
  if (!p) {
    logger.info('[kafka:no-op] emails.outbound → %j', payload);
    return;
  }
  await p.send({
    topic: EMAIL_TOPIC,
    messages: [
      {
        key: payload.tenantId,
        value: JSON.stringify({ tenantId: payload.tenantId, data: payload }),
      },
    ],
  });
}

export async function disconnectProducer() {
  if (producer) {
    await producer.disconnect();
    producer = null;
  }
}
