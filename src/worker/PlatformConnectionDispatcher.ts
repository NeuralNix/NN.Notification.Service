import notificationService from '@/modules/notification/notification.service';
import { publishRealtimeNotification } from '@/kafka/producer';
import { requiredDashboardsFor } from '@/access/dashboardAccess';
import logger from '@/utils/logger';

/**
 * Wire-format of a platform connect/disconnect event published by the Auth
 * Server (NN.Auth.Server.Backend → KafkaEventPublisher → topic
 * `platform.connections.events`). camelCase to match the .NET serializer.
 */
export interface PlatformConnectionEvent {
  eventId?: string;
  eventType: string; // "platform.connected" | "platform.disconnected"
  source?: string; // "auth-server"
  tenantId: string;
  platform: string; // amazon | walmart | ebay | woocommerce | shipstation | fedex | ups | usps | …
  timestamp?: string;
  data?: {
    userId?: string | null;
    integrationId?: string | null;
    environment?: string | null;
    reason?: string | null;
    metadata?: Record<string, unknown> | null;
  };
}

const PLATFORM_LABELS: Record<string, string> = {
  shipstation: 'ShipStation',
  woocommerce: 'WooCommerce',
  ebay: 'eBay',
  usps: 'USPS',
  ups: 'UPS',
  fedex: 'FedEx',
  amazon: 'Amazon',
  walmart: 'Walmart',
  veeqo: 'Veeqo',
};

const CARRIERS = new Set(['fedex', 'ups', 'usps']);
const MARKETPLACES = new Set(['amazon', 'walmart', 'ebay', 'woocommerce']);

function label(platform: string): string {
  const key = platform.toLowerCase();
  return PLATFORM_LABELS[key] ?? (platform ? platform[0].toUpperCase() + platform.slice(1) : 'Integration');
}

/** "marketplace" | "shipping carrier" | "integration" — used in the message copy. */
function kindWord(platform: string): string {
  const key = platform.toLowerCase();
  if (MARKETPLACES.has(key)) return 'marketplace';
  if (CARRIERS.has(key)) return 'shipping carrier';
  return 'integration'; // shipstation + anything else
}

/**
 * Turn a platform connect/disconnect event into a ShipExa-scoped notification.
 *
 * The notification is a tenant BROADCAST (userId = null) with category
 * `shipping`, which the access model (CATEGORY_DASHBOARDS.shipping = ['ecommerce'])
 * scopes to exactly the users who can open the ShipExa dashboard — both in the
 * bell history (persisted) and as a realtime toast (requiredDashboards).
 */
export async function dispatchPlatformConnectionEvent(evt: PlatformConnectionEvent): Promise<void> {
  const type = (evt.eventType || '').toLowerCase();
  const connected = type === 'platform.connected';
  const disconnected = type === 'platform.disconnected';
  if (!connected && !disconnected) {
    logger.info('[platform-dispatch] ignoring eventType=%s', evt.eventType);
    return;
  }
  if (!evt.tenantId || !evt.platform) {
    logger.warn('[platform-dispatch] missing tenantId/platform, skipping');
    return;
  }

  const name = label(evt.platform);
  const kind = kindWord(evt.platform);
  const reason = evt.data?.reason ? ` (${evt.data.reason})` : '';

  const title = connected ? `${name} connected` : `${name} disconnected`;
  const message = connected
    ? `${name} ${kind} was connected to ShipExa.`
    : `${name} ${kind} was disconnected from ShipExa${reason}.`;
  const severity: 'success' | 'warn' = connected ? 'success' : 'warn';
  const actionUrl = '/shipping/settings';

  const data: Record<string, unknown> = {
    platform: evt.platform.toLowerCase(),
    kind,
    event: connected ? 'connected' : 'disconnected',
    integrationId: evt.data?.integrationId ?? null,
    environment: evt.data?.environment ?? null,
    reason: evt.data?.reason ?? null,
    actorUserId: evt.data?.userId ?? null,
  };

  // 1. Persisted bell history (broadcast scoped to ShipExa via category).
  try {
    await notificationService.create({
      tenantId: evt.tenantId,
      userId: null,
      category: 'shipping',
      severity,
      title,
      message,
      actionUrl,
      data,
      sourceEventId: evt.eventId,
      sourceTopic: 'platform.connections.events',
    });
  } catch (err: any) {
    logger.error('[platform-dispatch] failed to persist notification: %s', err.message);
  }

  // 2. Realtime push via the WebSocket Gateway, scoped to the same dashboards.
  try {
    await publishRealtimeNotification({
      tenantId: evt.tenantId,
      userId: null,
      type: connected ? 'platform.connected' : 'platform.disconnected',
      title,
      message,
      severity,
      requiredDashboards: requiredDashboardsFor('shipping'),
      data: { ...data, actionUrl },
    });
  } catch (err: any) {
    logger.error('[platform-dispatch] failed to publish realtime notification: %s', err.message);
  }
}
