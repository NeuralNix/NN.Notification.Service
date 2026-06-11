import prisma from '@/config/prisma';
import { NotificationCategory, NotificationSeverity, Prisma } from '@prisma/client';
import logger from '@/utils/logger';

export interface CreateNotificationInput {
  tenantId: string;
  userId?: string | null;
  category: NotificationCategory;
  severity?: NotificationSeverity;
  title: string;
  message: string;
  actionUrl?: string;
  data?: Record<string, unknown>;
  sourceEventId?: string;
  sourceTopic?: string;
}

export interface ListQuery {
  tenantId: string;
  userId?: string;
  isRead?: boolean;
  category?: NotificationCategory;
  skipCount?: number;
  maxResultCount?: number;
  /// Categories this user may see as tenant broadcasts (dashboard-access
  /// scoping). null/undefined = unrestricted (admins, service principals).
  /// User-targeted rows are always visible regardless of this list.
  allowedCategories?: NotificationCategory[] | null;
}

function shouldReplaceExistingNotification(
  existing: { sourceTopic: string | null },
  input: CreateNotificationInput,
) {
  return existing.sourceTopic === 'payment.events'
    && !!input.sourceTopic
    && input.sourceTopic.startsWith('operations.')
    && !!input.sourceEventId;
}

export class NotificationService {
  async create(input: CreateNotificationInput) {
    // Idempotent on (sourceEventId, category) when sourceEventId is given,
    // so re-delivered Kafka messages don't duplicate the bell.
    if (input.sourceEventId) {
      const existing = await prisma.notification.findFirst({
        where: { tenantId: input.tenantId, sourceEventId: input.sourceEventId, category: input.category },
      });
      if (existing) {
        if (shouldReplaceExistingNotification(existing, input)) {
          logger.info(
            'Replacing fallback notification for sourceEventId=%s with sourceTopic=%s',
            input.sourceEventId,
            input.sourceTopic,
          );
          return prisma.notification.update({
            where: { id: existing.id },
            data: {
              userId: input.userId ?? null,
              severity: input.severity ?? 'info',
              title: input.title.substring(0, 200),
              message: input.message.substring(0, 2000),
              actionUrl: input.actionUrl,
              data: (input.data ?? {}) as Prisma.JsonObject,
              sourceTopic: input.sourceTopic,
            },
          });
        }
        logger.info('Notification already exists for sourceEventId=%s, skipping', input.sourceEventId);
        return existing;
      }
    }
    return prisma.notification.create({
      data: {
        tenantId: input.tenantId,
        userId: input.userId ?? null,
        category: input.category,
        severity: input.severity ?? 'info',
        title: input.title.substring(0, 200),
        message: input.message.substring(0, 2000),
        actionUrl: input.actionUrl,
        data: (input.data ?? {}) as Prisma.JsonObject,
        sourceEventId: input.sourceEventId,
        sourceTopic: input.sourceTopic,
      },
    });
  }

  /// Lazily anchor the earliest notification a (tenant,user) pair may see.
  /// First call creates the cutoff at "now"; subsequent calls return the
  /// stored sinceAt. This is the mechanism that prevents a freshly-added
  /// user from inheriting the full historical bell feed of the
  /// organization / dashboard they just joined.
  private async getOrCreateCutoff(tenantId: string, userId: string): Promise<Date> {
    const row = await prisma.notificationVisibilityCutoff.upsert({
      where: { tenantId_userId: { tenantId, userId } },
      create: { tenantId, userId },
      update: {},
      select: { sinceAt: true },
    });
    return row.sinceAt;
  }

  /// Build the visibility OR clause for a (tenant,user). Every branch is
  /// implicitly AND'd with `createdAt >= sinceAt` by the caller so users
  /// never see notifications older than their own join cutoff.
  ///
  /// Branches:
  ///   1. Tenant broadcast (userId=null) inside the active tenant.
  ///   2. User-targeted row inside the active tenant.
  ///   3. User-targeted row in a different tenant — but only if the user
  ///      has previously observed that tenant (i.e. has a cutoff row for
  ///      it). This keeps the existing cross-tenant override path for
  ///      operators that switched active org while making sure brand-new
  ///      users don't inherit notifications from "other dashboards".
  ///   Broadcast rows (branch 1) are additionally scoped by the caller's
  ///   dashboard access: a user without the Operations Dashboard never sees
  ///   estimate/invoice/job broadcasts. Targeted rows (branches 2-3) are
  ///   exempt — they were explicitly addressed to the user.
  private async buildVisibility(
    tenantId: string,
    userId: string | undefined,
    allowedCategories?: NotificationCategory[] | null,
  ): Promise<Prisma.NotificationWhereInput[]> {
    const broadcast: Prisma.NotificationWhereInput = { tenantId, userId: null };
    if (allowedCategories) broadcast.category = { in: allowedCategories };
    const branches: Prisma.NotificationWhereInput[] = [broadcast];
    if (!userId) return branches;
    branches.push({ tenantId, userId });
    const otherTenants = await prisma.notificationVisibilityCutoff.findMany({
      where: { userId, NOT: { tenantId } },
      select: { tenantId: true },
    });
    if (otherTenants.length > 0) {
      branches.push({
        userId,
        tenantId: { in: otherTenants.map((t) => t.tenantId) },
      });
    }
    return branches;
  }

  async list(q: ListQuery) {
    const skip = Math.max(0, q.skipCount ?? 0);
    const take = Math.min(200, Math.max(1, q.maxResultCount ?? 50));
    // Read state is per-user, derived from NotificationReceipt — see below.
    const sinceAt = q.userId
      ? await this.getOrCreateCutoff(q.tenantId, q.userId)
      : new Date(0);
    const visibility = await this.buildVisibility(q.tenantId, q.userId, q.allowedCategories);
    const where: Prisma.NotificationWhereInput = {
      AND: [
        { OR: visibility },
        { createdAt: { gte: sinceAt } },
      ],
      category: q.category,
    };
    if (q.isRead !== undefined && q.userId) {
      // Filter by the per-user receipt rather than the legacy row-level flag.
      where.receipts = q.isRead
        ? { some: { userId: q.userId } }
        : { none: { userId: q.userId } };
    }
    const [rows, total] = await Promise.all([
      prisma.notification.findMany({
        where,
        orderBy: { createdAt: 'desc' },
        skip,
        take,
        include: q.userId
          ? { receipts: { where: { userId: q.userId }, take: 1 } }
          : undefined,
      }),
      prisma.notification.count({ where }),
    ]);
    const items = rows.map((r) => projectForUser(r, q.userId));
    return { items, total };
  }

  async unreadCount(
    tenantId: string,
    userId: string,
    allowedCategories?: NotificationCategory[] | null,
  ) {
    const sinceAt = await this.getOrCreateCutoff(tenantId, userId);
    const visibility = await this.buildVisibility(tenantId, userId, allowedCategories);
    return prisma.notification.count({
      where: {
        AND: [
          { OR: visibility },
          { createdAt: { gte: sinceAt } },
        ],
        receipts: { none: { userId } },
      },
    });
  }

  async markRead(
    id: string,
    tenantId: string,
    userId: string,
    allowedCategories?: NotificationCategory[] | null,
  ) {
    const sinceAt = await this.getOrCreateCutoff(tenantId, userId);
    const visibility = await this.buildVisibility(tenantId, userId, allowedCategories);
    const n = await prisma.notification.findFirst({
      where: {
        id,
        AND: [
          { OR: visibility },
          { createdAt: { gte: sinceAt } },
        ],
      },
      include: { receipts: { where: { userId }, take: 1 } },
    });
    if (!n) return null;
    if (n.receipts.length > 0) return projectForUser(n, userId);
    await prisma.notificationReceipt.upsert({
      where: { notificationId_userId: { notificationId: id, userId } },
      create: { notificationId: id, userId, readAt: new Date() },
      update: {},
    });
    return projectForUser(
      { ...n, receipts: [{ notificationId: id, userId, readAt: new Date() }] as any },
      userId,
    );
  }

  async markAllRead(
    tenantId: string,
    userId: string,
    allowedCategories?: NotificationCategory[] | null,
  ) {
    // Find every visible-to-user notification that does NOT yet have a
    // receipt for this user, then create the receipts in one shot.
    const sinceAt = await this.getOrCreateCutoff(tenantId, userId);
    const visibility = await this.buildVisibility(tenantId, userId, allowedCategories);
    const targets = await prisma.notification.findMany({
      where: {
        AND: [
          { OR: visibility },
          { createdAt: { gte: sinceAt } },
        ],
        receipts: { none: { userId } },
      },
      select: { id: true },
    });
    if (targets.length === 0) return 0;
    const result = await prisma.notificationReceipt.createMany({
      data: targets.map((t) => ({ notificationId: t.id, userId })),
      skipDuplicates: true,
    });
    return result.count;
  }
}

/// Strip receipt rows from the persisted notification and project per-user
/// `isRead` / `readAt` so REST consumers (the platform bell) keep their
/// existing DTO shape.
function projectForUser(
  row: any,
  userId: string | undefined,
): any {
  const { receipts, ...rest } = row;
  if (!userId) return { ...rest, isRead: false, readAt: null };
  const receipt = Array.isArray(receipts) && receipts.length > 0 ? receipts[0] : null;
  return {
    ...rest,
    isRead: !!receipt,
    readAt: receipt ? receipt.readAt : null,
  };
}

export default new NotificationService();
