import { Request, Response } from 'express';
import service from './notification.service';
import { NotificationCategory } from '@prisma/client';
import { publishRealtimeNotification } from '@/kafka/producer';
import logger from '@/utils/logger';

function tenant(req: Request) {
  return req.user!.tenantId!;
}
function user(req: Request) {
  return req.user!.userId;
}

export class NotificationController {
  async create(req: Request, res: Response) {
    const created = await service.create({
      tenantId: tenant(req),
      userId: req.body.userId ?? null,
      category: req.body.category as NotificationCategory,
      severity: req.body.severity,
      title: String(req.body.title ?? ''),
      message: String(req.body.message ?? ''),
      actionUrl: req.body.actionUrl ? String(req.body.actionUrl) : undefined,
      data: req.body.data,
      sourceEventId: req.body.sourceEventId ? String(req.body.sourceEventId) : undefined,
      sourceTopic: req.body.sourceTopic ? String(req.body.sourceTopic) : undefined,
    });
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
    });
    res.json(result);
  }

  async unreadCount(req: Request, res: Response) {
    const count = await service.unreadCount(tenant(req), user(req));
    res.json({ count });
  }

  async markRead(req: Request, res: Response) {
    const n = await service.markRead(req.params.id, tenant(req), user(req));
    if (!n) return res.status(404).json({ message: 'Notification not found' });
    res.json(n);
  }

  async markAllRead(req: Request, res: Response) {
    const updated = await service.markAllRead(tenant(req), user(req));
    res.json({ updated });
  }
}

export default new NotificationController();
