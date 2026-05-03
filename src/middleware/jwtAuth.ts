// JWT auth middleware — validates the same HS256 tokens issued by NN.Auth.Server.Backend.
// Mirrors the verification used by NN.UGP.Email.Service.

import { Request, Response, NextFunction } from 'express';
import axios from 'axios';
import jwt, { JwtPayload, VerifyErrors } from 'jsonwebtoken';
import logger from '../utils/logger';

const JWT_SECRET = process.env.JWT_SECRET_KEY!;
const JWT_ISSUER = process.env.JWT_ISSUER!;
const JWT_AUDIENCE = process.env.JWT_AUDIENCE!;
const AUTH_API_URL = process.env.AUTH_API_URL || '';
const NOTIFICATION_PROXY_SECRET = (process.env.NOTIFICATION_PROXY_SECRET || '').trim();
// RFC 4122 UUID layout, version-agnostic. Auth Server issues v7 (time-
// ordered) ids in some claims; restricting to v1-v5 silently rejects them.
const UUID_RE =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

if (!JWT_SECRET) throw new Error('JWT_SECRET_KEY env variable is required');
if (!JWT_ISSUER) throw new Error('JWT_ISSUER env variable is required');
if (!JWT_AUDIENCE) throw new Error('JWT_AUDIENCE env variable is required');

const NAMEID = 'http://schemas.xmlsoap.org/ws/2005/05/identity/claims/nameidentifier';
const ROLE = 'http://schemas.microsoft.com/ws/2008/06/identity/claims/role';

function extractUserId(p: JwtPayload): string | undefined {
  return (p[NAMEID] as string | undefined) ?? (p['sub'] as string | undefined) ?? (p['userId'] as string | undefined);
}
function extractTenantId(p: JwtPayload): string | undefined {
  return (
    (p['organization_id'] as string | undefined) ??
    (p['OrganizationId'] as string | undefined) ??
    (p['tenant_id'] as string | undefined) ??
    (p['TenantId'] as string | undefined)
  );
}
function extractRoles(p: JwtPayload): string[] {
  const fromArray = (p['roles'] as string[] | undefined) ?? [];
  const single = (p[ROLE] as string | string[] | undefined) ?? (p['role'] as string | undefined);
  if (Array.isArray(single)) return [...fromArray, ...single];
  if (single) return [...fromArray, single];
  return fromArray;
}

function requestedTenantId(req: Request): string | undefined {
  const value = req.headers['x-organization-id'];
  const first = Array.isArray(value) ? value[0] : value;
  const trimmed = first?.trim();
  return trimmed && UUID_RE.test(trimmed) ? trimmed : undefined;
}

function hasTrustedProxyOverride(req: Request): boolean {
  if (!NOTIFICATION_PROXY_SECRET) {
    return false;
  }

  const value = req.headers['x-notification-proxy-secret'];
  const first = Array.isArray(value) ? value[0] : value;
  return typeof first === 'string' && first.trim() === NOTIFICATION_PROXY_SECRET;
}

function hasTenantOverrideRole(roles: string[]): boolean {
  return roles.some((role) => {
    const normalized = role.toLowerCase();
    return normalized === 'admin' || normalized === 'systemadmin' || normalized === 'superadmin';
  });
}

async function verifyOrganizationAccess(req: Request, organizationId: string): Promise<boolean> {
  if (!AUTH_API_URL) {
    logger.warn('[auth] AUTH_API_URL not configured, cannot verify organization override');
    return false;
  }

  const auth = req.headers.authorization;
  if (!auth) {
    return false;
  }

  try {
    const url = `${AUTH_API_URL.replace(/\/$/, '')}/api/user/by-organization/${organizationId}`;
    const response = await axios.get(url, {
      headers: {
        Authorization: auth,
        Accept: 'application/json',
      },
      timeout: 7_000,
    });

    const users = Array.isArray(response.data?.data) ? response.data.data : [];
    return users.some((user: { id?: string }) => user.id === req.user?.userId);
  } catch (error: any) {
    logger.warn(
      '[auth] Failed tenant override verification for user %s org %s: %s',
      req.user?.userId,
      organizationId,
      error.response?.status ? `status ${error.response.status}` : error.message,
    );
    return false;
  }
}

declare module 'express-serve-static-core' {
  interface Request {
    user?: {
      userId: string;
      tenantId?: string;
      roles: string[];
      raw: JwtPayload;
    };
  }
}

export function authenticate(req: Request, res: Response, next: NextFunction) {
  const auth = req.headers.authorization;
  if (!auth || !auth.startsWith('Bearer ')) {
    const overrideTenantId = requestedTenantId(req);
    if (req.method === 'POST' && req.path === '/' && overrideTenantId && hasTrustedProxyOverride(req)) {
      req.user = {
        userId: '00000000-0000-0000-0000-000000000001',
        tenantId: overrideTenantId,
        roles: ['Service'],
        raw: {} as JwtPayload,
      };
      return next();
    }
    return res.status(401).json({ message: 'Missing bearer token' });
  }
  const token = auth.substring('Bearer '.length).trim();

  jwt.verify(
    token,
    JWT_SECRET,
    {
      algorithms: ['HS256'],
      issuer: JWT_ISSUER,
      audience: JWT_AUDIENCE,
      clockTolerance: 300,
    },
    (err: VerifyErrors | null, decoded: any) => {
      if (err || !decoded) {
        logger.warn('JWT verification failed: %s', err?.message);
        return res.status(401).json({ message: 'Invalid token' });
      }
      const payload = decoded as JwtPayload;
      const userId = extractUserId(payload);
      if (!userId) {
        return res.status(401).json({ message: 'Token missing user id' });
      }
      req.user = {
        userId,
        tenantId: extractTenantId(payload),
        roles: extractRoles(payload),
        raw: payload,
      };
      next();
    }
  );
}

export function requireTenant(req: Request, res: Response, next: NextFunction) {
  const overrideTenantId = requestedTenantId(req);

  if (overrideTenantId && overrideTenantId === req.user?.tenantId) {
    req.user.tenantId = overrideTenantId;
    return next();
  }

  if (!overrideTenantId) {
    if (!req.user?.tenantId) return res.status(403).json({ message: 'Tenant context required' });
    return next();
  }

  if (req.user && hasTrustedProxyOverride(req)) {
    req.user.tenantId = overrideTenantId;
    return next();
  }

  if (!req.user || !hasTenantOverrideRole(req.user.roles)) {
    return res.status(403).json({ message: 'Organization override is not allowed' });
  }

  void verifyOrganizationAccess(req, overrideTenantId).then((hasAccess) => {
    if (!hasAccess) {
      return res.status(403).json({ message: 'Organization access denied' });
    }

    req.user!.tenantId = overrideTenantId;
    next();
  });
}
