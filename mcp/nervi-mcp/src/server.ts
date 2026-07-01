#!/usr/bin/env node
/**
 * Nèrvi MCP server — exposes nervi_publish (N-2) and nervi_subscribe (N-3).
 * The actual tool logic lives in handlers.ts; this file only wires the MCP
 * SDK to a live NATS bus (stdio locally, Streamable HTTP in-cluster).
 */
import http from 'node:http';
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import type { CallToolResult } from '@modelcontextprotocol/sdk/types.js';
import { z } from 'zod';
import { NatsBus } from './bus.js';
import { QUALIFIERS, ValidationError, type SignalBus } from './core.js';
import { handlePublish, handleSubscribe, type ToolResult } from './handlers.js';

async function toResult(fn: () => Promise<ToolResult>): Promise<CallToolResult> {
  try {
    return (await fn()) as CallToolResult;
  } catch (err) {
    if (err instanceof ValidationError) {
      return { content: [{ type: 'text', text: `Invalid input: ${err.message}` }], isError: true };
    }
    const message = err instanceof Error ? err.message : String(err);
    return { content: [{ type: 'text', text: `Bus error: ${message}` }], isError: true };
  }
}

export function buildServer(bus: SignalBus): McpServer {
  const server = new McpServer({ name: 'nervi-mcp', version: '0.1.0' });

  server.registerTool(
    'nervi_publish',
    {
      title: 'Publish to a Nèrvi subject',
      description:
        'Publish a message to a NATS JetStream subject on the OCCITAN stream. ' +
        'The qualifier is embedded in the message headers (Nervi-Qualifier).',
      inputSchema: {
        subject: z.string().describe('Concrete subject under occitan.* (e.g. occitan.issues.nervi)'),
        payload: z
          .union([z.string(), z.record(z.string(), z.unknown())])
          .describe('Message body — string or JSON object'),
        qualifier: z
          .enum(QUALIFIERS)
          .describe('Signal qualifier — maps to a Farga node type'),
        msg_id: z
          .string()
          .optional()
          .describe(
            'Optional NATS deduplication key (Nats-Msg-Id header). ' +
            'The broker silently drops duplicate publishes with the same id within 1 hour. ' +
            'Convention (ADR-N-002): dispatch messages use dispatch-<component>-<issue>-<date>, ' +
            'e.g. dispatch-caissa-43-20260630.',
          ),
      },
    },
    (args) => toResult(() => handlePublish(bus, args)),
  );

  server.registerTool(
    'nervi_subscribe',
    {
      title: 'Fetch pending messages from a Nèrvi subject',
      description:
        'Fetch pending messages from a NATS JetStream subject via a durable pull consumer. ' +
        'Stateless — no long-running subscription. Creates the durable consumer on first use.',
      inputSchema: {
        subject: z.string().describe('Concrete subject under occitan.* (e.g. occitan.issues.nervi)'),
        consumer_name: z.string().describe('Durable consumer identity (stable across calls)'),
        max_messages: z
          .number()
          .int()
          .min(1)
          .max(1000)
          .default(10)
          .describe('Maximum messages to fetch (default 10)'),
      },
    },
    (args) => toResult(() => handleSubscribe(bus, args)),
  );

  return server;
}

/** Read and JSON-parse a request body. */
function readJsonBody(req: http.IncomingMessage): Promise<unknown> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    req.on('data', (c) => chunks.push(c as Buffer));
    req.on('end', () => {
      const raw = Buffer.concat(chunks).toString('utf8');
      if (!raw) return resolve(undefined);
      try {
        resolve(JSON.parse(raw));
      } catch (e) {
        reject(e);
      }
    });
    req.on('error', reject);
  });
}

/**
 * Stateless Streamable HTTP transport — one fresh server+transport per POST.
 * Used by the in-cluster Deployment so agents can reach the tools over HTTP.
 */
async function startHttp(bus: SignalBus, port: number): Promise<void> {
  const httpServer = http.createServer(async (req, res) => {
    const url = (req.url ?? '').split('?')[0];

    if (req.method === 'GET' && url === '/health') {
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ status: 'ok' }));
      return;
    }

    if (req.method === 'POST' && url === '/mcp') {
      const transport = new StreamableHTTPServerTransport({ sessionIdGenerator: undefined });
      const server = buildServer(bus);
      res.on('close', () => {
        void transport.close();
        void server.close();
      });
      try {
        await server.connect(transport);
        await transport.handleRequest(req, res, await readJsonBody(req));
      } catch (err) {
        if (!res.headersSent) {
          res.writeHead(500, { 'content-type': 'application/json' });
          res.end(JSON.stringify({ error: err instanceof Error ? err.message : String(err) }));
        }
      }
      return;
    }

    res.writeHead(404, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ error: 'not found' }));
  });

  await new Promise<void>((resolve) => httpServer.listen(port, resolve));
  console.error(`nervi-mcp listening on :${port} (streamable-http)`);
}

async function main(): Promise<void> {
  const bus = await NatsBus.connect();
  const transport = process.env.NERVI_MCP_TRANSPORT ?? 'stdio';

  if (transport === 'http') {
    const port = Number(process.env.PORT ?? 8080);
    await startHttp(bus, port);
    return;
  }

  const server = buildServer(bus);
  await server.connect(new StdioServerTransport());
}

// Only run when invoked directly (not when imported by tests).
const isMain = process.argv[1] && import.meta.url === `file://${process.argv[1]}`;
if (isMain) {
  main().catch((err) => {
    console.error('nervi-mcp failed to start:', err);
    process.exit(1);
  });
}
