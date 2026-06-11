import 'dotenv/config';
import { createApp } from './app';
import logger from './utils/logger';
import { startKafkaPaymentConsumer, stopKafkaPaymentConsumer } from './worker/KafkaPaymentConsumer';
import { disconnectProducer } from './kafka/producer';
import prisma from './config/prisma';

const PORT = Number(process.env.PORT || 5070);

async function ensureSchema() {
  // Idempotent bootstrap for tables not yet applied via `prisma db push`.
  // Notification visibility cutoff anchors the earliest notification a
  // user is allowed to see in a given tenant. Without this row brand-new
  // users would inherit every historical broadcast notification of the
  // dashboard/org they joined.
  try {
    // HR notifications (leave reviewed, payslip issued, …) were added after
    // the enum was first pushed. ADD VALUE is idempotent and must run before
    // any 'hr' row is written.
    await prisma.$executeRawUnsafe(
      `ALTER TYPE "NotificationCategory" ADD VALUE IF NOT EXISTS 'hr';`
    );
    await prisma.$executeRawUnsafe(
      `CREATE TABLE IF NOT EXISTS "NotificationVisibilityCutoffs" (
         "tenantId" uuid NOT NULL,
         "userId" uuid NOT NULL,
         "sinceAt" timestamp(3) NOT NULL DEFAULT NOW(),
         CONSTRAINT "NotificationVisibilityCutoffs_pkey" PRIMARY KEY ("tenantId","userId")
       );`
    );
    await prisma.$executeRawUnsafe(
      `CREATE INDEX IF NOT EXISTS "NotificationVisibilityCutoffs_userId_idx"
         ON "NotificationVisibilityCutoffs" ("userId");`
    );
    // Backfill cutoffs for users that already engaged with the bell before
    // this column existed. Without this, an existing operator whose first
    // post-deploy bell touch happens AFTER fresh notifications arrive would
    // silently lose those rows because their lazy cutoff would be created
    // at "now". Backfill anchors them at epoch so prior visibility is
    // preserved. Only brand-new (tenant,user) pairs going forward get a
    // "now" cutoff via getOrCreateCutoff().
    await prisma.$executeRawUnsafe(
      `INSERT INTO "NotificationVisibilityCutoffs" ("tenantId","userId","sinceAt")
         SELECT DISTINCT n."tenantId", r."userId", TIMESTAMP 'epoch'
           FROM "NotificationReceipts" r
           JOIN "Notifications" n ON n."id" = r."notificationId"
         ON CONFLICT ("tenantId","userId") DO NOTHING;`
    );
    await prisma.$executeRawUnsafe(
      `INSERT INTO "NotificationVisibilityCutoffs" ("tenantId","userId","sinceAt")
         SELECT DISTINCT "tenantId", "userId", TIMESTAMP 'epoch'
           FROM "Notifications"
          WHERE "userId" IS NOT NULL
         ON CONFLICT ("tenantId","userId") DO NOTHING;`
    );
    logger.info('Schema bootstrap: NotificationVisibilityCutoffs ensured');
  } catch (err: any) {
    logger.warn('Schema bootstrap failed: %s', err.message);
  }
}

async function main() {
  const app = createApp();
  const server = app.listen(PORT, () => {
    logger.info('Notification Service HTTP listening on :%d', PORT);
  });

  await ensureSchema();

  // Run the Kafka consumer in the same process so a single container exposes
  // both REST and the dispatcher worker. Split into a worker-server.ts later
  // if horizontal scaling needs separate scaling profiles.
  await startKafkaPaymentConsumer();

  const shutdown = async (signal: string) => {
    logger.info('Received %s, shutting down…', signal);
    server.close();
    await stopKafkaPaymentConsumer();
    await disconnectProducer();
    await prisma.$disconnect();
    process.exit(0);
  };
  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
}

main().catch((err) => {
  logger.error('Fatal startup error: %s', err.message);
  process.exit(1);
});
