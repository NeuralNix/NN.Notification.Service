// Service-to-service auth: obtains an organization-scoped machine token from
// NN.Auth.Server.Backend (`POST /api/auth/service-token`) using the shared
// service API key. Cached per tenant and refreshed before expiry.
//
// ── Why this no longer mints its own tokens ──
// This module used to sign its own JWT asserting `roles: ['SystemAdmin']` when
// the Auth Server was unreachable (and, because AUTH_SERVICE_API_KEY was blank
// in every deployment, that fallback was in fact the ONLY path ever taken).
// Two problems with that: the token claimed global admin when all it needed was
// one organization, and the Auth Server had no way to tell an internal caller
// apart from a real administrator — it just honoured the role claim.
//
// The Auth Server now issues proper service tokens: scoped to one organization,
// carrying `token_use=service` and NO role claims, short-lived, and audited at
// issuance. Local minting is gone. If the Auth Server cannot issue a token, the
// caller gets null and degrades — a silent self-signed admin credential is not
// a safe failure mode.

import axios from 'axios';
import logger from '@/utils/logger';

const AUTH_API_URL = (process.env.AUTH_API_URL || '').replace(/\/$/, '');
const SERVICE_API_KEY = process.env.AUTH_SERVICE_API_KEY || '';
const SERVICE_NAME = process.env.AUTH_SERVICE_NAME || 'notification-service';
const REQUEST_TIMEOUT_MS = Number(process.env.AUTH_REQUEST_TIMEOUT_MS || 8_000);
// Refresh this long before expiry so an in-flight request never carries a
// token that expires mid-call.
const EXPIRY_SKEW_MS = 60_000;

interface CachedToken {
  value: string;
  expiresAt: number;
}

/** Cached per tenant: a service token is scoped to exactly one organization. */
const tokenCache = new Map<string, CachedToken>();

/** Throttles the "not configured" warning so it doesn't flood the log. */
let warnedMissingConfig = 0;

function warnMissingConfigOnce(): void {
  const now = Date.now();
  if (now - warnedMissingConfig < 300_000) return;
  warnedMissingConfig = now;
  logger.warn(
    '[auth] Cannot obtain service tokens: %s%s. Service-to-service calls that need one will be skipped.',
    !AUTH_API_URL ? 'AUTH_API_URL is not set' : '',
    !SERVICE_API_KEY ? `${!AUTH_API_URL ? '; ' : ''}AUTH_SERVICE_API_KEY is empty` : '',
  );
}

/**
 * An organization-scoped service token, or null when one cannot be obtained.
 * Callers must treat null as "skip this call", never as "proceed unauthenticated".
 */
export async function getServiceToken(
  tenantId?: string,
  _userId?: string | null,
): Promise<string | null> {
  if (!tenantId) {
    logger.warn('[auth] getServiceToken called without a tenant — service tokens must be org-scoped');
    return null;
  }
  if (!AUTH_API_URL || !SERVICE_API_KEY) {
    warnMissingConfigOnce();
    return null;
  }

  const cached = tokenCache.get(tenantId);
  if (cached && cached.expiresAt - EXPIRY_SKEW_MS > Date.now()) return cached.value;

  try {
    const res = await axios.post(
      `${AUTH_API_URL}/api/auth/service-token`,
      { service: SERVICE_NAME, organizationId: tenantId },
      {
        headers: {
          'X-Service-Api-Key': SERVICE_API_KEY,
          'Content-Type': 'application/json',
        },
        timeout: REQUEST_TIMEOUT_MS,
      },
    );

    const token: string | undefined = res.data?.accessToken ?? res.data?.access_token;
    const expiresIn = Number(res.data?.expiresIn ?? res.data?.expires_in ?? 600);
    if (!token) {
      logger.warn('[auth] Service-token endpoint returned no token for tenant %s', tenantId);
      return null;
    }

    tokenCache.set(tenantId, { value: token, expiresAt: Date.now() + expiresIn * 1000 });
    return token;
  } catch (err: any) {
    const status = err?.response?.status;
    logger.warn(
      '[auth] Failed to obtain a service token for tenant %s (status=%s): %s',
      tenantId, status ?? 'n/a', err?.response?.data?.message ?? err?.message ?? err,
    );
    return null;
  }
}

/** Test/ops hook — drop cached service tokens. */
export function clearServiceTokenCache(): void {
  tokenCache.clear();
}
