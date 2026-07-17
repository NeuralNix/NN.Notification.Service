/** Agent-facing OpenAPI registry for NN.Notification.Service.
 *  In-app notifications: list/unread reads + creating a notification
 *  (broadcast when userId is omitted — scoped by dashboard access; targeted
 *  when userId is set). Creation is high-impact in the agent (notifies real
 *  users). Derived from src/modules/notification/notification.routes.ts. */
import { OpenAPIRegistry, extendZodWithOpenApi } from '@asteasolutions/zod-to-openapi';
import { z } from 'zod';

extendZodWithOpenApi(z);
export const registry = new OpenAPIRegistry();

const ok = (d: string) => ({ 200: { description: d } });

registry.registerPath({ method: 'get', path: '/api/notifications', tags: ['Notifications'],
  summary: 'List the caller’s notifications',
  request: { query: z.object({ limit: z.number().int().optional() }) },
  responses: ok('Notifications') });

registry.registerPath({ method: 'get', path: '/api/notifications/unread-count', tags: ['Notifications'],
  summary: 'Unread notification count for the caller',
  responses: ok('Count') });

// Fields verified against notification.controller.create:
// userId (null ⇒ broadcast), category, severity, title, message, actionUrl?, data?
registry.registerPath({ method: 'post', path: '/api/notifications', tags: ['Notifications'],
  summary: 'CREATE a notification — broadcast to the org (no userId) or targeted (userId)',
  request: { body: { content: { 'application/json': { schema: z.object({
    title: z.string().min(1),
    message: z.string().min(1),
    category: z.string().describe('Notification category (drives which dashboard sees a broadcast)'),
    severity: z.string().optional().describe('info | warning | critical'),
    userId: z.string().optional()
      .describe('Target user id; OMIT to broadcast to the organization'),
    actionUrl: z.string().optional().describe('Link opened when the notification is clicked'),
  }).openapi('CreateNotificationRequest') } } } },
  responses: ok('Created notification') });
