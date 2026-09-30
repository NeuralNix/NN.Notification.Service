// Emails a copy of each new notification to the people who get it in their
// bell and have "Send notifications as emails" on (HR → Users → Edit user
// access). It is on by default for Platform Admins — before the setting
// existed, every admin was emailed every tenant-wide notification — and off by
// default for everyone else.
//
// Who is emailed mirrors who can SEE the notification (src/access):
//   • tenant-wide (userId null): admins, plus members with a dashboard the
//     category belongs to (every member keeps HR self-service);
//   • addressed to one user: that user only. ADMIN_NOTIFICATION_EMAIL_SCOPE=all
//     also copies opted-in admins, as that setting did before.
// Each recipient gets their own email, so nobody sees who else was sent it.
//
// Pipeline: this module publishes `emails.outbound` Kafka requests →
// NN.UGP.Email.Service's KafkaEmailConsumer turns each into a pending Email row
// against the `admin-notification-alert` template → EmailWorker renders and
// sends it, through the company's own email system when it has one. Because it
// goes through the template system, the wording is editable without a deploy.
//
// Two rules this module exists to enforce:
//   1. It NEVER throws. Notification creation is the caller's real work; a
//      mail failure must not roll it back or surface to the API consumer.
//   2. It only fires for genuinely NEW notifications. The create() path is
//      idempotent on sourceEventId, so re-delivered Kafka messages must not
//      re-mail anyone.

import type { Notification } from '@prisma/client';
import { publishEmailRequest } from '@/kafka/producer';
import { listNotificationRecipients, type NotificationRecipient } from '@/clients/hrClient';
import { getOrganizationName } from '@/clients/organizationClient';
import { requiredDashboardsFor } from '@/access/dashboardAccess';
import logger from '@/utils/logger';

export const ADMIN_NOTIFICATION_TEMPLATE_SLUG = 'admin-notification-alert';

/** Whether admins are also copied on notifications addressed to one user. */
type Scope = 'all' | 'broadcast';

function flag(name: string, fallback: boolean): boolean {
  const raw = process.env[name];
  if (raw === undefined || raw.trim() === '') return fallback;
  return raw.trim().toLowerCase() === 'true';
}

function csv(name: string): string[] {
  return (process.env[name] || '')
    .split(',')
    .map((s) => s.trim().toLowerCase())
    .filter(Boolean);
}

function resolveScope(): Scope {
  return (process.env.ADMIN_NOTIFICATION_EMAIL_SCOPE || 'broadcast').trim().toLowerCase() === 'all'
    ? 'all'
    : 'broadcast';
}

// ── Per-tenant hourly cap ────────────────────────────────────────────────────
// A runaway producer (sync storm, retry loop in another service) would
// otherwise mail admins hundreds of times. The cap is per process and resets
// on restart, which is fine for a blast shield — it is not an accounting
// mechanism. 0 disables the cap.
const MAX_PER_HOUR = Number(process.env.ADMIN_NOTIFICATION_EMAIL_MAX_PER_HOUR ?? 60);
const WINDOW_MS = 60 * 60 * 1000;
const sendLog = new Map<string, number[]>();

function withinRateLimit(tenantId: string): boolean {
  if (!Number.isFinite(MAX_PER_HOUR) || MAX_PER_HOUR <= 0) return true;
  const now = Date.now();
  const recent = (sendLog.get(tenantId) ?? []).filter((t) => now - t < WINDOW_MS);
  if (recent.length >= MAX_PER_HOUR) {
    sendLog.set(tenantId, recent);
    return false;
  }
  recent.push(now);
  sendLog.set(tenantId, recent);
  return true;
}

const SEVERITY_LABELS: Record<string, string> = {
  info: 'For your information',
  success: 'Completed',
  warn: 'Needs attention',
  error: 'Action required',
};

const CATEGORY_LABELS: Record<string, string> = {
  payment: 'Payment',
  estimate: 'Estimate',
  system: 'System',
  email: 'Email',
  shipping: 'Shipping',
  inventory: 'Inventory',
  hr: 'HR',
  calldesk: 'CallDesk',
  support: 'Customer support',
};

function titleCase(value: string): string {
  return value ? value.charAt(0).toUpperCase() + value.slice(1) : value;
}

/**
 * Query parameter that tells the frontend which company a link belongs to.
 *
 * An admin can hold memberships in several companies, but one browser holds
 * ONE active company (it is an `organization_id` JWT claim in shared
 * localStorage). So a bare path in an inbox opens under whatever company that
 * browser happens to be signed into — which, for anyone with more than one, is
 * regularly the wrong one: the page loads the wrong tenant, or 404s because the
 * document belongs to the other company. Stamping the notification's own tenant
 * on the link lets the frontend notice the mismatch and offer to switch.
 *
 * Kept in sync with `COMPANY_QUERY_PARAM` in
 * NN.Platform.Frontend/app/src/lib/company-link.ts.
 */
export const COMPANY_QUERY_PARAM = 'company';

/** Add `?company=<tenant>` unless the URL already carries one. */
function withCompany(absolute: string, tenantId?: string | null): string {
  const id = (tenantId ?? '').trim();
  if (!id) return absolute;
  try {
    const url = new URL(absolute);
    if (url.searchParams.has(COMPANY_QUERY_PARAM)) return absolute;
    url.searchParams.set(COMPANY_QUERY_PARAM, id);
    return url.toString();
  } catch {
    // Unparseable URL — better an un-stamped link than a mangled one.
    return absolute;
  }
}

/**
 * Turn a notification's actionUrl into something clickable from an inbox.
 * Relative paths are only usable inside the SPA, so they need the public
 * origin prefixed; anything already absolute is passed through untouched.
 *
 * `tenantId` is stamped on as `?company=` so the recipient lands in the company
 * the notification is about — but only for links on our OWN frontend origin, so
 * an actionUrl pointing at a third party is never annotated with a tenant id.
 */
export function toAbsoluteUrl(
  actionUrl: string | null | undefined,
  tenantId?: string | null,
): string {
  const raw = (actionUrl ?? '').trim();
  if (!raw) return '';
  const base = (process.env.PUBLIC_FRONTEND_URL || '').replace(/\/$/, '');
  if (/^https?:\/\//i.test(raw)) {
    const ours = !!base && raw.toLowerCase().startsWith(base.toLowerCase());
    return ours ? withCompany(raw, tenantId) : raw;
  }
  if (!base) return '';
  return withCompany(`${base}/${raw.replace(/^\//, '')}`, tenantId);
}

/**
 * True when this notification should be emailed at all, based on the
 * operator's category / severity filters. Exported for testing.
 */
export function shouldEmail(
  n: Pick<Notification, 'category' | 'severity'>,
): boolean {
  if (!flag('ADMIN_NOTIFICATION_EMAILS_ENABLED', true)) return false;

  const categories = csv('ADMIN_NOTIFICATION_EMAIL_CATEGORIES');
  if (categories.length > 0 && !categories.includes(String(n.category).toLowerCase())) return false;

  const severities = csv('ADMIN_NOTIFICATION_EMAIL_SEVERITIES');
  if (severities.length > 0 && !severities.includes(String(n.severity).toLowerCase())) return false;

  return true;
}

/** Same rule as the bell (src/access/dashboardAccess.ts) for a tenant-wide notification. */
function canSee(r: NotificationRecipient, category: string): boolean {
  if (r.isAdmin) return true;
  const required = requiredDashboardsFor(category);
  if (required.length === 0) return true;
  const granted = new Set([...r.dashboards, 'hr']);
  return required.some((d) => granted.has(d));
}

/**
 * Who is emailed about this notification, from the opted-in members. Exported
 * for testing.
 */
export function recipientsFor(
  n: Pick<Notification, 'userId' | 'category'>,
  optedIn: NotificationRecipient[],
): NotificationRecipient[] {
  if (!n.userId) return optedIn.filter((r) => canSee(r, String(n.category)));
  // Addressed to one person (a payslip, an approved leave request, an alert
  // assigned to them): theirs alone. Copying admins is a privacy call, so it
  // stays opt-in via scope=all.
  const out = optedIn.filter((r) => r.userId === n.userId);
  if (resolveScope() === 'all') out.push(...optedIn.filter((r) => r.isAdmin && r.userId !== n.userId));
  return out;
}

/**
 * Email a freshly-created notification to everyone who should get it. Safe to
 * await: resolves rather than rejects on every failure path.
 */
export async function sendNotificationEmails(n: Notification): Promise<void> {
  try {
    if (!shouldEmail(n)) return;

    if (!withinRateLimit(n.tenantId)) {
      logger.warn(
        '[notification-email] Hourly cap (%d) reached for tenant %s — skipping email for notification %s',
        MAX_PER_HOUR, n.tenantId, n.id,
      );
      return;
    }

    const [optedIn, orgName] = await Promise.all([
      listNotificationRecipients(n.tenantId),
      getOrganizationName(n.tenantId),
    ]);
    const recipients = recipientsFor(n, optedIn);

    if (recipients.length === 0) {
      logger.info(
        '[notification-email] Nobody to email for notification %s (tenant %s, %d opted in)',
        n.id, n.tenantId, optedIn.length,
      );
      return;
    }

    const category = String(n.category);
    const severity = String(n.severity);
    const tenantName = orgName || 'Your organization';
    const categoryLabel = CATEGORY_LABELS[category] ?? titleCase(category);
    const severityLabel = SEVERITY_LABELS[severity] ?? titleCase(severity);
    const actionUrl = toAbsoluteUrl(n.actionUrl, n.tenantId);

    const templateData = {
      // The worker re-renders the template's own subject at send time, so
      // this value is what lands in the Email Log row. Without it the log
      // shows the consumer's "Notification: <slug>" placeholder.
      subject: `[${categoryLabel}] ${n.title}`,
      tenantName,
      notificationId: n.id,
      notificationTitle: n.title,
      notificationMessage: n.message,
      category,
      categoryLabel,
      severity,
      severityLabel,
      isAlert: severity === 'warn' || severity === 'error',
      actionUrl,
      occurredAt: n.createdAt.toISOString(),
      // Broadcast vs targeted, so the reader can tell whether the whole org
      // saw this or it was addressed to one person.
      audience: n.userId ? 'A single user' : 'Everyone in the organization',
    };
    // One email per person: a shared To: line would show every recipient's
    // address to everyone else, and not all of them are admins.
    for (const r of recipients) {
      await publishEmailRequest({
        tenantId: n.tenantId,
        to: [r.email],
        templateType: ADMIN_NOTIFICATION_TEMPLATE_SLUG,
        // Warn/error alerts jump the send queue ahead of routine info mail.
        priority: severity === 'error' || severity === 'warn' ? 'high' : 'normal',
        templateData,
      });
    }

    logger.info(
      '[notification-email] Queued %s for notification %s (category=%s severity=%s) to %d recipient(s)',
      ADMIN_NOTIFICATION_TEMPLATE_SLUG, n.id, category, severity, recipients.length,
    );
  } catch (err: any) {
    // Swallow: the notification itself is already persisted and is the
    // caller's contract. Mail is best-effort.
    logger.error(
      '[notification-email] Failed to queue notification email for notification %s: %s',
      n?.id, err?.message ?? err,
    );
  }
}
