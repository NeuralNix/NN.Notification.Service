// Dashboard-access scoping for notifications.
//
// Tenant-broadcast notifications (Notification.userId == null) must only be
// visible to users who can open the dashboard the notification belongs to:
// a user without the Operations Dashboard must not see estimate/invoice/job
// rows in their bell. User-targeted notifications (userId set) are exempt —
// they were explicitly addressed (e.g. "your leave request was approved").
//
// Effective access mirrors the Platform Frontend's getVisibleDomainIds():
//   - Admin / SystemAdmin / SuperAdmin (and the synthetic Service principal)
//     see everything.
//   - Regular members see the dashboards in their per-user DashboardAccess
//     list, resolved from the Auth Server. Every member additionally keeps
//     HR self-service ("hr").
//
// Dashboard access is NOT a JWT claim, so we resolve it via the Auth Server
// members endpoint using the caller's own bearer token, and cache per
// (tenant, user) to keep the bell endpoints cheap.

import axios from 'axios';
import { JwtPayload } from 'jsonwebtoken';
import { NotificationCategory } from '@prisma/client';
import logger from '../utils/logger';

const AUTH_API_URL = (process.env.AUTH_API_URL || '').replace(/\/$/, '');
const ACCESS_CACHE_TTL_MS = Number(process.env.ACCESS_CACHE_TTL_MS || 120_000);
const ACCESS_FAILURE_TTL_MS = Number(process.env.ACCESS_FAILURE_TTL_MS || 30_000);

/// Dashboards that may see each category as a tenant broadcast. A user needs
/// ANY of the listed dashboards. An empty list means unrestricted.
export const CATEGORY_DASHBOARDS: Record<NotificationCategory, string[]> = {
  estimate: ['operations'],
  payment: ['operations', 'finance'],
  system: ['operations'],
  email: ['operations'],
  shipping: ['ecommerce'],
  inventory: ['inventory'],
  hr: ['hr'],
  calldesk: ['calldesk'],
};

export const ALL_CATEGORIES = Object.keys(CATEGORY_DASHBOARDS) as NotificationCategory[];

/// Dashboards required to see this category in realtime. Forwarded on the
/// notifications.realtime payload so the WebSocket Gateway can scope tenant
/// broadcasts the same way the bell does.
export function requiredDashboardsFor(category: string): string[] {
  return CATEGORY_DASHBOARDS[category as NotificationCategory] ?? [];
}

interface AuthUser {
  userId: string;
  tenantId?: string;
  roles: string[];
  raw: JwtPayload;
}

function isPrivileged(user: AuthUser): boolean {
  const roles = [...user.roles];
  const orgRole = user.raw?.['organization_role'];
  if (typeof orgRole === 'string') roles.push(orgRole);
  return roles.some((role) => {
    const n = role.toLowerCase();
    return n === 'admin' || n === 'systemadmin' || n === 'superadmin' || n === 'service';
  });
}

interface CacheEntry {
  dashboards: string[];
  expiresAt: number;
}
const cache = new Map<string, CacheEntry>();

async function fetchDashboards(
  slug: string,
  userId: string,
  bearerToken: string,
): Promise<string[] | null> {
  if (!AUTH_API_URL) {
    logger.warn('[access] AUTH_API_URL not configured, cannot resolve dashboard access');
    return null;
  }
  try {
    const url = `${AUTH_API_URL}/api/organizations/${encodeURIComponent(slug)}/members`;
    const response = await axios.get(url, {
      headers: { Authorization: `Bearer ${bearerToken}`, Accept: 'application/json' },
      timeout: 7_000,
    });
    const members = Array.isArray(response.data) ? response.data : response.data?.data;
    if (!Array.isArray(members)) return null;
    const me = members.find((m: any) => m?.userId === userId || m?.id === userId);
    if (!me) return null;
    if (typeof me.role === 'string' && me.role.toLowerCase() === 'admin') {
      return ['*'];
    }
    const access = Array.isArray(me.dashboardAccess) ? me.dashboardAccess : [];
    return access
      .filter((d: unknown): d is string => typeof d === 'string')
      .map((d: string) => d.toLowerCase());
  } catch (error: any) {
    logger.warn(
      '[access] dashboard access lookup failed for user %s org %s: %s',
      userId,
      slug,
      error.response?.status ? `status ${error.response.status}` : error.message,
    );
    return null;
  }
}

/// Resolve the categories this user may see as tenant broadcasts.
/// Returns null when unrestricted (admins / service principals).
/// On lookup failure the user is scoped down to unrestricted categories plus
/// "hr" (fail closed: never leak, but keep self-service notifications alive).
export async function resolveAllowedCategories(
  user: AuthUser,
  bearerToken: string | undefined,
): Promise<NotificationCategory[] | null> {
  if (isPrivileged(user)) return null;

  const slug = typeof user.raw?.['organization_slug'] === 'string'
    ? (user.raw['organization_slug'] as string)
    : undefined;

  let dashboards: string[] | null = null;
  if (slug && bearerToken) {
    const key = `${user.tenantId ?? ''}:${user.userId}`;
    const hit = cache.get(key);
    if (hit && hit.expiresAt > Date.now()) {
      dashboards = hit.dashboards;
    } else {
      dashboards = await fetchDashboards(slug, user.userId, bearerToken);
      cache.set(key, {
        dashboards: dashboards ?? [],
        expiresAt: Date.now() + (dashboards ? ACCESS_CACHE_TTL_MS : ACCESS_FAILURE_TTL_MS),
      });
    }
  }

  if (dashboards?.includes('*')) return null;

  // Every member keeps HR self-service regardless of granted dashboards.
  const granted = new Set<string>([...(dashboards ?? []), 'hr']);
  return ALL_CATEGORIES.filter((category) => {
    const required = CATEGORY_DASHBOARDS[category];
    return required.length === 0 || required.some((d) => granted.has(d));
  });
}
