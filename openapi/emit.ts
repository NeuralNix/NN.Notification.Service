/** Emit the OpenAPI document for the agent-facing surface.
 *  cd openapi && npm install && npm run emit > openapi.json
 *  Committed into NN.Agent.Backend/application/toolkit/specs/. */
import { OpenApiGeneratorV3 } from '@asteasolutions/zod-to-openapi';
import { registry } from './registry';

const generator = new OpenApiGeneratorV3(registry.definitions);
// Server URL is supplied by the environment — no host is hardcoded here.
const serverUrl = process.env.OPENAPI_SERVER_URL || process.env.PUBLIC_BASE_URL || '/';

const document = generator.generateDocument({
  openapi: '3.0.3',
  info: { title: 'NN.Notification.Service — agent surface', version: '1.0.0',
    description: 'Generated from openapi/registry.ts (zod). Agent-exposed routes only.' },
  servers: [{ url: serverUrl }],
  security: [{ Bearer: [] }],
});
(document.components as any) = { ...(document.components ?? {}),
  securitySchemes: { Bearer: { type: 'http', scheme: 'bearer', bearerFormat: 'JWT' } } };
process.stdout.write(JSON.stringify(document, null, 2) + '\n');
