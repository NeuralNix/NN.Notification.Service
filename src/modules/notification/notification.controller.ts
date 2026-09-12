import { Request, Response } from 'express';
import service from './notification.service';
import { NotificationCategory, NotificationSeverity } from '@prisma/client';
import { publishRealtimeNotification } from '@/kafka/producer';
import { resolveAllowedCategories, requiredDashboardsFor } from '@/access/dashboardAccess';
import logger from '@/utils/logger';

function tenant(req: Request) {
  return req.user!.tenantId!;
}
function user(req: Request) {
  return req.user!.userId;
}
function bearer(req: Request): string | undefined {
  const auth = req.headers.authorization;
  return auth?.startsWith('Bearer ') ? auth.substring('Bearer '.length).trim() : undefined;
}
function allowed(req: Request) {
  return resolveAllowedCategories(req.user!, bearer(req));
}

export class NotificationController {
  async create(req: Request, res: Response) {
    // Validate before Prisma sees the value: an unknown category used to reach
    // prisma.notification.findFirst() as a raw string, throw a
    // PrismaClientValidationError out of this async handler, and take the whole
    // process down (unhandled rejection) — one bad payload from any internal
    // caller restarted the service for everyone.
    const category = String(req.body.category ?? '');
    if (!(Object.values(NotificationCategory) as string[]).includes(category)) {
      return res.status(400).json({
        message: `Unknown category '${category}'. Expected one of: ${Object.values(NotificationCategory).join(', ')}`,
      });
    }
    const severity = String(req.body.severity ?? 'info');
    if (!(Object.values(NotificationSeverity) as string[]).includes(severity)) {
      return res.status(400).json({
        message: `Unknown severity '${severity}'. Expected one of: ${Object.values(NotificationSeverity).join(', ')}`,
      });
    }
    let created;
    try {
      created = await service.create({
        tenantId: tenant(req),
        userId: req.body.userId ?? null,
        category: category as NotificationCategory,
        severity: severity as NotificationSeverity,
        title: String(req.body.title ?? ''),
        message: String(req.body.message ?? ''),
        actionUrl: req.body.actionUrl ? String(req.body.actionUrl) : undefined,
        data: req.body.data,
        sourceEventId: req.body.sourceEventId ? String(req.body.sourceEventId) : undefined,
        sourceTopic: req.body.sourceTopic ? String(req.body.sourceTopic) : undefined,
      });
    } catch (error: any) {
      logger.error('[notifications] create failed: %s', error?.message ?? error);
      return res.status(500).json({ message: 'Could not store the notification' });
    }
    try {
      const data = (req.body.data && typeof req.body.data === 'object') ? req.body.data : {};
      const eventType = typeof data.eventType === 'string' ? data.eventType : undefined;
      await publishRealtimeNotification({
        tenantId: created.tenantId,
        userId: created.userId,
        type: eventType ? `${created.category}.${eventType}` : String(created.category),
        title: created.title,
        message: created.message,
        severity: created.severity,
        requiredDashboards: requiredDashboardsFor(String(created.category)),
        data: {
          ...data,
          actionUrl: created.actionUrl,
          notificationId: created.id,
        },
      });
    } catch (error: any) {
      logger.error('[notifications] failed to publish realtime notification: %s', error.message);
    }
    res.status(201).json(created);
  }

  async list(req: Request, res: Response) {
    const isReadQ = req.query.isRead;
    const isRead =
      isReadQ === undefined || isReadQ === '' ? undefined : String(isReadQ).toLowerCase() === 'true';
    const result = await service.list({
      tenantId: tenant(req),
      userId: user(req),
      isRead,
      category: req.query.category ? (String(req.query.category) as NotificationCategory) : undefined,
      skipCount: req.query.skipCount ? Number(req.query.skipCount) : 0,
      maxResultCount: req.query.maxResultCount ? Number(req.query.maxResultCount) : 50,
      allowedCategories: await allowed(req),
    });
    res.json(result);
  }

  async unreadCount(req: Request, res: Response) {
    const count = await service.unreadCount(tenant(req), user(req), await allowed(req));
    res.json({ count });
  }

  async markRead(req: Request, res: Response) {
    const n = await service.markRead(req.params.id, tenant(req), user(req), await allowed(req));
    if (!n) return res.status(404).json({ message: 'Notification not found' });
    res.json(n);
  }

  async markAllRead(req: Request, res: Response) {
    const updated = await service.markAllRead(tenant(req), user(req), await allowed(req));
    res.json({ updated });
  }
}

export default new NotificationController();
