// Thin client for the HR backend (NN.HR), used to resolve which users are
// administrators of a tenant so admin-facing emails have recipients.
//
// "Admin" is deliberately defined the same way NN.Operational.Backend's
// AdminPaymentNotifier defines it — HR `userType == "Admin"`, active, with an
// email. Two admin alerts that disagree about who an admin is would be worse
// than either alert.
//
// ── Authentication ──
// Uses an organization-scoped service token from the Auth Server
// (`authClient.getServiceToken`). That token carries the `organization_id` and
// `organization_slug` claims HR needs, a GUID-shaped subject, and
// `token_use=service` so the Auth Server's members endpoint authorizes it by
// its organization scope.
//
// This deliberately does NOT hand-mint a token asserting SystemAdmin, which is
// what an earlier version did: that made an internal lookup indistinguishable
// from a real global administrator, and it only worked because the Auth Server
// honoured the role claim without checking it.
//
// Every failure resolves to an empty list: an admin email must never break
// notification creation.

import axios from 'axios';
import { getServiceToken } from '@/clients/authClient';
import logger from '@/utils/logger';

const HR_API_URL = (process.env.HR_API_URL || 'http://hr-backend:8080').replace(/\/$/, '');

const ADMIN_CACHE_TTL_MS = Number(process.env.HR_ADMIN_CACHE_TTL_MS || 120_000);
const ADMIN_FAILURE_TTL_MS = Number(process.env.HR_ADMIN_FAILURE_TTL_MS || 30_000);
const REQUEST_TIMEOUT_MS = Number(process.env.HR_REQUEST_TIMEOUT_MS || 15_000);

interface HrEmployee {
  email?: string | null;
  status?: string | null;
  /** Admin | PlatformUser | Employee */
  userType?: string | null;
}

interface CacheEntry {
  emails: string[];
  expiresAt: number;
}

const cache = new Map<string, CacheEntry>();

/**
 * HR list endpoints have been seen to return both a bare array and an
 * envelope; the Auth Server wraps lists as `{ data: [...] }`. Normalise
 * defensively rather than assuming one shape.
 */
function asList(payload: unknown): HrEmployee[] {
  if (Array.isArray(payload)) return payload as HrEmployee[];
  if (payload && typeof payload === 'object') {
    const inner = (payload as Record<string, unknown>).data
      ?? (payload as Record<string, unknown>).items;
    if (Array.isArray(inner)) return inner as HrEmployee[];
  }
  return [];
}

function extractAdminEmails(employees: HrEmployee[]): string[] {
  const seen = new Set<string>();
  const out: string[] = [];
  for (const e of employees) {
    if ((e.userType ?? '').toLowerCase() !== 'admin') continue;
    if ((e.status ?? '').toLowerCase() === 'inactive') continue;
    const email = (e.email ?? '').trim();
    if (!email || !email.includes('@')) continue;
    const key = email.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(email);
  }
  return out;
}

function cacheAndReturn(tenantId: string, emails: string[], ttl: number): string[] {
  cache.set(tenantId, { emails, expiresAt: Date.now() + ttl });
  return emails;
}

/**
 * Email addresses of a tenant's admin users. Cached briefly so a burst of
 * notifications doesn't hammer HR (and the Auth Server behind it) once per
 * notification.
 */
export async function listAdminEmails(tenantId: string): Promise<string[]> {
  if (!tenantId) return [];

  const cached = cache.get(tenantId);
  if (cached && cached.expiresAt > Date.now()) return cached.emails;

  const token = await getServiceToken(tenantId);
  if (!token) {
    logger.warn('[hr] No service token for tenant %s — cannot resolve admins', tenantId);
    return cacheAndReturn(tenantId, [], ADMIN_FAILURE_TTL_MS);
  }

  try {
    const res = await axios.get(`${HR_API_URL}/api/employees`, {
      headers: { Authorization: `Bearer ${token}`, Accept: 'application/json' },
      timeout: REQUEST_TIMEOUT_MS,
    });
    return cacheAndReturn(tenantId, extractAdminEmails(asList(res.data)), ADMIN_CACHE_TTL_MS);
  } catch (err: any) {
    logger.warn(
      '[hr] Failed to resolve admin emails for tenant %s (status=%s): %s',
      tenantId, err?.response?.status ?? 'n/a', err?.message ?? err,
    );
    // Short negative cache so a flapping HR isn't retried once per notification.
    return cacheAndReturn(tenantId, [], ADMIN_FAILURE_TTL_MS);
  }
}

/** Test/ops hook — drop cached recipient lists. */
export function clearAdminEmailCache(): void {
  cache.clear();
}
