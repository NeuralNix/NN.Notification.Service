// Resolves a tenant's display name and slug.
//
// Uses the Auth Server's anonymous minimal lookup
// (`GET /api/organizations/public/{id}` — id, name, slug, logoUrl only). The
// tenantId IS the organization id, so this works from a background worker with
// no user context, which is exactly the situation notification creation is in.
//
// The slug matters as much as the name: HR's employee directory keys off an
// `organization_slug` claim, so anything resolving admin recipients needs it.

import axios from 'axios';
import logger from '@/utils/logger';

const AUTH_API_URL = (process.env.AUTH_API_URL || '').replace(/\/$/, '');
const ORG_CACHE_TTL_MS = Number(process.env.ORG_CACHE_TTL_MS || 600_000);
const ORG_FAILURE_TTL_MS = Number(process.env.ORG_FAILURE_TTL_MS || 60_000);
const REQUEST_TIMEOUT_MS = Number(process.env.ORG_REQUEST_TIMEOUT_MS || 5_000);

export interface OrganizationSummary {
  name: string | null;
  slug: string | null;
}

interface CacheEntry {
  org: OrganizationSummary;
  expiresAt: number;
}

const cache = new Map<string, CacheEntry>();
const EMPTY: OrganizationSummary = { name: null, slug: null };

function str(value: unknown): string | null {
  return typeof value === 'string' && value.trim() ? value.trim() : null;
}

/**
 * Organization name + slug, with null fields when they cannot be resolved.
 * Callers supply their own fallback label rather than getting a fabricated one.
 */
export async function getOrganization(tenantId: string): Promise<OrganizationSummary> {
  if (!tenantId || !AUTH_API_URL) return EMPTY;

  const cached = cache.get(tenantId);
  if (cached && cached.expiresAt > Date.now()) return cached.org;

  try {
    const res = await axios.get(`${AUTH_API_URL}/api/organizations/public/${tenantId}`, {
      headers: { Accept: 'application/json' },
      timeout: REQUEST_TIMEOUT_MS,
    });
    const org: OrganizationSummary = {
      name: str(res.data?.name),
      slug: str(res.data?.slug),
    };
    cache.set(tenantId, { org, expiresAt: Date.now() + ORG_CACHE_TTL_MS });
    return org;
  } catch (err: any) {
    logger.warn(
      '[org] Failed to resolve organization %s: %s',
      tenantId, err?.message ?? err,
    );
    cache.set(tenantId, { org: EMPTY, expiresAt: Date.now() + ORG_FAILURE_TTL_MS });
    return EMPTY;
  }
}

/** Convenience wrapper for callers that only need the display name. */
export async function getOrganizationName(tenantId: string): Promise<string | null> {
  return (await getOrganization(tenantId)).name;
}

/** Test/ops hook — drop cached organization lookups. */
export function clearOrganizationCache(): void {
  cache.clear();
}
