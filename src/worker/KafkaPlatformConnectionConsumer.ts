import { Kafka, Consumer, EachMessagePayload, logLevel } from 'kafkajs';
import logger from '@/utils/logger';
import { dispatchPlatformConnectionEvent, PlatformConnectionEvent } from './PlatformConnectionDispatcher';

const enabled = (process.env.KAFKA_ENABLED || '').toLowerCase() === 'true';
const brokers = (process.env.KAFKA_BROKERS || 'nn-kafka:9092').split(',');
const clientId = process.env.KAFKA_CLIENT_ID || 'notification-service';
// Distinct group from the payment consumer so the two topics rebalance
// independently.
const groupId = process.env.KAFKA_PLATFORM_CONSUMER_GROUP || 'notification-platform-connections';
const topic = process.env.KAFKA_PLATFORM_EVENTS_TOPIC || 'platform.connections.events';

let consumer: Consumer | null = null;
let stopped = false;

const INITIAL_RETRY_MS = 2000;
const MAX_RETRY_MS = 30000;

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function startConsumerOnce(): Promise<void> {
  const kafka = new Kafka({ clientId, brokers, logLevel: logLevel.NOTHING });
  consumer = kafka.consumer({ groupId });

  consumer.on(consumer.events.CRASH, (event: any) => {
    const reason = (event as any)?.payload?.error?.message ?? 'unknown';
    logger.error('[KafkaPlatformConsumer] crashed: %s — will reconnect', reason);
    consumer = null;
    if (!stopped) void scheduleReconnect();
  });
  consumer.on(consumer.events.DISCONNECT, () => {
    logger.warn('[KafkaPlatformConsumer] disconnected');
  });

  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: false });
  logger.info('[KafkaPlatformConsumer] subscribed to %s (group=%s)', topic, groupId);

  await consumer.run({
    eachMessage: async ({ message }: EachMessagePayload) => {
      if (!message.value) return;
      let evt: PlatformConnectionEvent;
      try {
        evt = JSON.parse(message.value.toString());
      } catch (err: any) {
        logger.error('[KafkaPlatformConsumer] invalid JSON: %s', err.message);
        return;
      }
      try {
        await dispatchPlatformConnectionEvent(evt);
      } catch (err: any) {
        // Never throw — that would re-deliver forever. Log and move on.
        logger.error('[KafkaPlatformConsumer] dispatch failed: %s', err.message);
      }
    },
  });
}

async function scheduleReconnect(): Promise<void> {
  let backoff = INITIAL_RETRY_MS;
  while (!stopped) {
    await delay(backoff);
    if (stopped) return;
    try {
      logger.info('[KafkaPlatformConsumer] reconnect attempt (brokers=%s)', brokers.join(','));
      await startConsumerOnce();
      return;
    } catch (err: any) {
      logger.error('[KafkaPlatformConsumer] reconnect failed: %s — retrying in %dms', err.message, backoff);
      try {
        if (consumer) await consumer.disconnect().catch(() => {});
      } finally {
        consumer = null;
      }
      backoff = Math.min(backoff * 2, MAX_RETRY_MS);
    }
  }
}

export async function startKafkaPlatformConnectionConsumer(): Promise<void> {
  if (!enabled) {
    logger.info('[KafkaPlatformConsumer] disabled (KAFKA_ENABLED != true)');
    return;
  }
  stopped = false;

  // Don't block process startup on Kafka availability.
  void (async () => {
    try {
      await startConsumerOnce();
    } catch (err: any) {
      logger.error('[KafkaPlatformConsumer] failed to start: %s — will retry', err.message);
      try {
        if (consumer) await consumer.disconnect().catch(() => {});
      } finally {
        consumer = null;
      }
      void scheduleReconnect();
    }
  })();
}

export async function stopKafkaPlatformConnectionConsumer(): Promise<void> {
  stopped = true;
  if (consumer) {
    await consumer.disconnect();
    consumer = null;
  }
}
