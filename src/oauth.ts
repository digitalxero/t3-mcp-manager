import { randomBytes, timingSafeEqual } from "node:crypto";
import type { PluginContext, PluginMcpServer } from "@t3tools/plugin-sdk";
import * as Effect from "effect/Effect";
import {
  auth,
  type OAuthClientProvider,
  type OAuthDiscoveryState,
} from "@modelcontextprotocol/sdk/client/auth.js";
import {
  OAuthClientInformationSchema,
  OAuthMetadataSchema,
  OAuthProtectedResourceMetadataSchema,
  OAuthTokensSchema,
  type OAuthClientInformationMixed,
  type OAuthTokens,
} from "@modelcontextprotocol/sdk/shared/auth.js";
import { record, text, type Server } from "./config.ts";
import type { Store } from "./store.ts";

const opaque = (value: unknown, name: string, max = 16_384): string => {
  if (typeof value !== "string" || value.length > max || value.includes("\0"))
    throw Error(`Invalid ${name}`);
  return value;
};
const random = () => randomBytes(32).toString("base64url");
const key = (id: string) => `oauth-${id.replaceAll("-", "")}`;
const equal = (a: string, b: string) => {
  const first = Buffer.from(a),
    second = Buffer.from(b);
  return first.length === second.length && timingSafeEqual(first, second);
};
const identity = (server: Server) =>
  JSON.stringify([server.id, server.project, server.url, server.auth]);
const expiry = (tokens: OAuthTokens, now: number) => {
  if (tokens.token_type.toLowerCase() !== "bearer")
    throw Error("Only OAuth bearer tokens are supported");
  if (tokens.expires_in === undefined) return null;
  const result = now + tokens.expires_in * 1000;
  if (tokens.expires_in < 0 || !Number.isFinite(result)) throw Error("Invalid OAuth token expiry");
  return result;
};
const FLOW_LIFETIME = 10 * 60 * 1000;
const MAX_BODY = 8 * 1024 * 1024;
const headers = {
  "Cache-Control": "no-store",
  "Content-Type": "text/plain; charset=utf-8",
  "X-Content-Type-Options": "nosniff",
};
const reply = (message: string, status = 200) => new Response(message, { status, headers });

interface Grant {
  version: 1;
  identity: string;
  label: string;
  redirectUrl: string;
  client: OAuthClientInformationMixed;
  tokens: OAuthTokens;
  discovery: OAuthDiscoveryState;
  expiresAt: number | null;
  routeToken: string;
  scope?: string;
}
interface Flow {
  id: string;
  revision: string;
  identity: string;
  state: string;
  redirectUrl: string;
  label: string;
  createdAt: number;
  client?: OAuthClientInformationMixed;
  discovery?: OAuthDiscoveryState;
  verifier?: string;
  authorizationUrl?: string;
  tokens?: OAuthTokens;
  scope?: string;
  clientMetadataUrl?: string;
  chooseAccount: boolean;
}
interface Session {
  owner: string;
  upstream: string;
}

function secureUrl(value: string): URL {
  const url = new URL(value);
  if (
    url.username ||
    url.password ||
    url.hash ||
    (url.protocol !== "https:" &&
      !(url.protocol === "http:" && ["localhost", "127.0.0.1", "[::1]"].includes(url.hostname)))
  )
    throw Error("OAuth requires HTTPS or a loopback HTTP URL without credentials");
  return url;
}
function discovery(value: OAuthDiscoveryState): OAuthDiscoveryState {
  const issuer = secureUrl(value.authorizationServerUrl);
  const metadata = OAuthMetadataSchema.parse(value.authorizationServerMetadata);
  const resource = OAuthProtectedResourceMetadataSchema.parse(value.resourceMetadata);
  if (
    secureUrl(metadata.issuer).href !== issuer.href ||
    !resource.authorization_servers?.some((url) => secureUrl(url).href === issuer.href)
  )
    throw Error("OAuth issuer does not match the selected authorization server");
  if (!metadata.code_challenge_methods_supported?.includes("S256"))
    throw Error("OAuth requires S256 PKCE");
  secureUrl(metadata.authorization_endpoint);
  secureUrl(metadata.token_endpoint);
  return { ...value, authorizationServerMetadata: metadata, resourceMetadata: resource };
}
function decodeGrant(value: string): Grant {
  const data = record(JSON.parse(value));
  if (
    data.version !== 1 ||
    typeof data.identity !== "string" ||
    typeof data.label !== "string" ||
    data.label.length > 120 ||
    typeof data.redirectUrl !== "string" ||
    typeof data.routeToken !== "string" ||
    !/^[A-Za-z0-9_-]{43}$/.test(data.routeToken) ||
    !(
      data.expiresAt === null ||
      (typeof data.expiresAt === "number" && Number.isFinite(data.expiresAt))
    )
  )
    throw Error("Invalid OAuth connection state");
  return {
    version: 1,
    identity: data.identity,
    label: data.label,
    redirectUrl: secureUrl(data.redirectUrl).href,
    routeToken: data.routeToken,
    ...(data.scope === undefined ? {} : { scope: text(data.scope, "OAuth scopes", 2000) }),
    expiresAt: data.expiresAt,
    client: { ...record(data.client), ...OAuthClientInformationSchema.parse(data.client) },
    tokens: OAuthTokensSchema.parse(data.tokens),
    discovery: discovery(data.discovery as OAuthDiscoveryState),
  };
}

export async function createOAuth(
  context: PluginContext,
  store: Store,
  options: {
    fetch?: typeof fetch;
    now?: () => number;
    changed?: () => Promise<void>;
  } = {},
) {
  const fetcher = options.fetch ?? fetch;
  const now = options.now ?? Date.now;
  const flows = new Map<string, Flow>();
  const sessions = new Map<string, Session>();
  const controllers = new Map<string, AbortController>();
  const queues = new Map<string, Promise<unknown>>();
  const changed = options.changed ?? (async () => {});
  const shutdown = new AbortController();
  let closed = false;
  const serial = <A>(id: string, work: () => Promise<A>): Promise<A> => {
    const next = (queues.get(id) ?? Promise.resolve()).then(work);
    const settled = next
      .catch(() => undefined)
      .finally(() => {
        if (queues.get(id) === settled) queues.delete(id);
      });
    queues.set(id, settled);
    return next;
  };
  const get = async (id: string) => {
    if (closed) throw Error("OAuth manager is closed");
    const server = await store.get(id);
    if (!server || server.auth !== "oauth" || server.transport !== "http")
      throw Error("OAuth server is unavailable");
    secureUrl(server.url);
    return server;
  };
  const load = async (server: Server): Promise<Grant | undefined> => {
    const value = await Effect.runPromise(context.secrets.get(key(server.id)));
    if (value === null) return;
    const grant = decodeGrant(value);
    if (grant.identity !== identity(server))
      throw Error("OAuth connection belongs to different server settings");
    const issuer = secureUrl(grant.discovery.authorizationServerUrl).href;
    if (
      !grant.tokens.issuer ||
      !grant.client.issuer ||
      secureUrl(grant.tokens.issuer).href !== issuer ||
      secureUrl(grant.client.issuer).href !== issuer
    )
      throw Error("OAuth credentials have a different issuer");
    return grant;
  };
  const abort = (id: string) => {
    controllers.get(id)?.abort();
    controllers.delete(id);
    for (const [session, value] of sessions) if (value.owner === id) sessions.delete(session);
  };
  const control = (id: string) => {
    let controller = controllers.get(id);
    if (!controller) {
      controller = new AbortController();
      controllers.set(id, controller);
    }
    return controller;
  };
  const save = async (server: Server, grant: Grant, revision?: string) => {
    const current = await get(server.id);
    if (
      !current.enabled ||
      identity(current) !== grant.identity ||
      (revision && current.secretKey !== revision)
    )
      throw Error("Server changed during OAuth sign-in");
    await Effect.runPromise(context.secrets.set(key(server.id), JSON.stringify(grant)));
  };
  const provider = (flow: Flow, tokens?: OAuthTokens): OAuthClientProvider => ({
    redirectUrl: flow.redirectUrl,
    ...(flow.clientMetadataUrl ? { clientMetadataUrl: flow.clientMetadataUrl } : {}),
    clientMetadata: {
      client_name: "T3 MCP Manager",
      redirect_uris: [flow.redirectUrl],
      grant_types: ["authorization_code", "refresh_token"],
      response_types: ["code"],
      token_endpoint_auth_method: flow.client?.client_secret ? "client_secret_post" : "none",
      ...(flow.scope ? { scope: flow.scope } : {}),
    },
    state: () => flow.state,
    clientInformation: () => flow.client,
    saveClientInformation: (value) => {
      flow.client = value;
    },
    tokens: () => tokens,
    saveTokens: (value) => {
      flow.tokens = value;
    },
    saveCodeVerifier: (value) => {
      flow.verifier = value;
    },
    codeVerifier: () => {
      if (!flow.verifier) throw Error("OAuth verifier is unavailable");
      return flow.verifier;
    },
    redirectToAuthorization: (url) => {
      if (tokens) throw Error("OAuth connection needs to be reconnected");
      if (flow.chooseAccount) url.searchParams.set("prompt", "select_account");
      const authorizationUrl = secureUrl(url.href).href;
      if (authorizationUrl.length > 16_384) throw Error("OAuth authorization URL is too long");
      flow.authorizationUrl = authorizationUrl;
    },
    discoveryState: () => flow.discovery,
    saveDiscoveryState: (value) => {
      const state = discovery(value);
      if (
        flow.client?.issuer &&
        secureUrl(flow.client.issuer).href !== secureUrl(state.authorizationServerUrl).href
      )
        throw Error("Registered client issuer does not match this server");
      flow.discovery = state;
    },
    invalidateCredentials: (scope) => {
      if (scope === "all" || scope === "client") delete flow.client;
      if (scope === "all" || scope === "tokens") {
        delete flow.tokens;
        tokens = undefined;
      }
      if (scope === "all" || scope === "verifier") delete flow.verifier;
      if (scope === "all" || scope === "discovery") delete flow.discovery;
    },
  });
  const authorize = (flow: Flow, serverUrl: string, tokens?: OAuthTokens, code?: string) => {
    const deadline = AbortSignal.timeout(20_000);
    const fetchFn: typeof fetch = async (input, init) => {
      secureUrl(typeof input === "string" ? input : input instanceof URL ? input.href : input.url);
      const response = await fetcher(input, {
        ...init,
        redirect: "error",
        signal: AbortSignal.any([
          shutdown.signal,
          deadline,
          ...(init?.signal ? [init.signal] : []),
        ]),
      });
      if (!response.body) return response;
      const reader = response.body.getReader(),
        chunks: Uint8Array[] = [];
      let size = 0;
      try {
        for (;;) {
          const part = await reader.read();
          if (part.done) break;
          size += part.value.byteLength;
          if (size > 256 * 1024) {
            await reader.cancel();
            throw Error("OAuth response is too large");
          }
          chunks.push(part.value);
        }
      } finally {
        reader.releaseLock();
      }
      const buffer = new Uint8Array(size);
      let offset = 0;
      for (const chunk of chunks) {
        buffer.set(chunk, offset);
        offset += chunk.byteLength;
      }
      return new Response(buffer, { status: response.status, headers: response.headers });
    };
    return auth(provider(flow, tokens), {
      serverUrl,
      ...(code === undefined ? {} : { authorizationCode: code }),
      ...(flow.scope ? { scope: flow.scope } : {}),
      fetchFn,
    });
  };

  const prune = () => {
    for (const [id, flow] of flows) if (now() - flow.createdAt >= FLOW_LIFETIME) flows.delete(id);
  };
  const finish = async (id: string, callback: string, automatic = false) =>
    serial(id, async () => {
      prune();
      const flow = flows.get(id);
      if (!flow) throw Error("OAuth sign-in expired or was cancelled");
      const url = new URL(callback),
        expected = new URL(flow.redirectUrl);
      if (
        (!automatic && url.origin !== expected.origin) ||
        url.pathname !== expected.pathname ||
        url.hash !== "" ||
        url.searchParams.getAll("state").length !== 1 ||
        !equal(url.searchParams.get("state") ?? "", flow.state)
      )
        throw Error("OAuth callback does not match this project");
      const server = await get(id);
      if (server.secretKey !== flow.revision || identity(server) !== flow.identity)
        throw Error("Server changed during OAuth sign-in");
      if (url.searchParams.has("error")) {
        flows.delete(id);
        await changed();
        throw Error("OAuth authorization was declined");
      }
      const code = url.searchParams.get("code");
      if (!code || url.searchParams.getAll("code").length !== 1)
        throw Error("OAuth authorization code is missing");
      const expectedIssuer = flow.discovery?.authorizationServerMetadata?.issuer;
      const suppliedIssuer = url.searchParams.get("iss");
      if (url.searchParams.getAll("iss").length > 1)
        throw Error("OAuth response issuer does not match");
      const requiredIssuer =
        flow.discovery?.authorizationServerMetadata &&
        "authorization_response_iss_parameter_supported" in
          flow.discovery.authorizationServerMetadata &&
        flow.discovery.authorizationServerMetadata
          .authorization_response_iss_parameter_supported === true;
      if (
        (requiredIssuer && !suppliedIssuer) ||
        (suppliedIssuer &&
          (!expectedIssuer || secureUrl(suppliedIssuer).href !== secureUrl(expectedIssuer).href))
      )
        throw Error("OAuth response issuer does not match");
      flows.delete(id);
      const result = await authorize(flow, server.url, undefined, code);
      if (result !== "AUTHORIZED" || !flow.tokens || !flow.client || !flow.discovery)
        throw Error("OAuth sign-in did not return credentials");
      const grant: Grant = {
        version: 1,
        identity: flow.identity,
        label: flow.label,
        redirectUrl: flow.redirectUrl,
        client: flow.client,
        tokens: flow.tokens,
        discovery: flow.discovery,
        expiresAt: expiry(flow.tokens, now()),
        routeToken: random(),
        ...((flow.scope ?? flow.tokens.scope) ? { scope: flow.scope ?? flow.tokens.scope } : {}),
      };
      await save(server, grant, flow.revision);
      abort(id);
      await changed();
      return { id, connected: true };
    });
  const refresh = (server: Server, force = false, rejectedToken?: string) =>
    serial(server.id, async () => {
      const grant = await load(server);
      if (!grant) throw Error("Connect an OAuth account first");
      if (
        (force && rejectedToken !== undefined && grant.tokens.access_token !== rejectedToken) ||
        (!force && (grant.expiresAt === null || grant.expiresAt > now() + 60_000))
      )
        return grant;
      const flow: Flow = {
        id: server.id,
        revision: server.secretKey,
        identity: identity(server),
        state: random(),
        redirectUrl: grant.redirectUrl,
        label: grant.label,
        createdAt: now(),
        client: grant.client,
        discovery: grant.discovery,
        chooseAccount: false,
        ...(grant.scope ? { scope: grant.scope } : {}),
      };
      const result = await authorize(flow, server.url, grant.tokens);
      if (result !== "AUTHORIZED" || !flow.tokens || !flow.client || !flow.discovery)
        throw Error("Reconnect this OAuth account");
      const tokens = {
        ...flow.tokens,
        ...(!flow.tokens.refresh_token && grant.tokens.refresh_token
          ? { refresh_token: grant.tokens.refresh_token }
          : {}),
      };
      const next: Grant = {
        ...grant,
        client: flow.client,
        tokens,
        discovery: flow.discovery,
        expiresAt: expiry(tokens, now()),
      };
      await save(server, next);
      await changed();
      return next;
    });
  return {
    start: (input: Record<string, unknown>) =>
      serial(text(input.id, "server id", 80), async () => {
        prune();
        const id = text(input.id, "server id", 80),
          server = await get(id);
        if (!server.enabled) throw Error("Enable the server before connecting OAuth");
        if (!flows.has(id) && flows.size >= 10) throw Error("At most ten sign-ins can be pending");
        const base = await Effect.runPromise(context.httpBaseUrl);
        const redirect = input["redirect-url"]
          ? secureUrl(text(input["redirect-url"], "redirect URL"))
          : new URL(base.replace(/\/$/, "") + "/oauth/callback");
        if (redirect.search || redirect.hash)
          throw Error("Callback URL cannot contain query parameters or fragments");
        const existing = await load(server).catch(() => undefined);
        const clientId = opaque(input["client-id"] ?? "", "client ID", 2000);
        const clientSecret = opaque(input["client-secret"] ?? "", "client secret");
        const chooseAccount = input["choose-account"];
        if (chooseAccount !== undefined && typeof chooseAccount !== "boolean")
          throw Error("Invalid account-selection preference");
        if (clientSecret && !clientId) throw Error("A client ID is required with a client secret");
        const clientIssuer = input.issuer
          ? secureUrl(text(input.issuer, "registered client issuer", 2000)).href
          : undefined;
        if (clientSecret && !clientIssuer)
          throw Error("A registered client issuer is required with a client secret");
        const flow: Flow = {
          id,
          revision: server.secretKey,
          identity: identity(server),
          state: random(),
          redirectUrl: redirect.href,
          label:
            text(input.label ?? "Connected account", "account label", 120) || "Connected account",
          createdAt: now(),
          chooseAccount: chooseAccount ?? true,
          ...(clientId
            ? {
                client: {
                  client_id: clientId,
                  ...(clientIssuer ? { issuer: clientIssuer } : {}),
                  ...(clientSecret ? { client_secret: clientSecret } : {}),
                },
              }
            : existing?.redirectUrl === redirect.href
              ? { client: existing.client }
              : {}),
          ...(input.scopes
            ? { scope: text(input.scopes, "scopes", 2000) }
            : existing?.scope
              ? { scope: existing.scope }
              : {}),
          ...(input["client-metadata-url"]
            ? {
                clientMetadataUrl: secureUrl(
                  text(input["client-metadata-url"], "client metadata URL"),
                ).href,
              }
            : {}),
        };
        const result = await authorize(flow, server.url);
        if (result !== "REDIRECT" || !flow.authorizationUrl || !flow.verifier)
          throw Error("OAuth sign-in did not produce an authorization URL");
        if (closed) throw Error("OAuth manager is closed");
        flows.set(id, flow);
        await changed();
        return { id, started: true };
      }),
    finish: (input: Record<string, unknown>) =>
      finish(text(input.id, "server id", 80), text(input.callback, "callback URL")),
    cancel: (id: string) =>
      serial(id, async () => {
        flows.delete(id);
        await changed();
        return { cancelled: true };
      }),
    disconnect: (id: string) =>
      serial(id, async () => {
        flows.delete(id);
        abort(id);
        await Effect.runPromise(context.secrets.remove(key(id)));
        await changed();
        return { disconnected: true };
      }),
    policyChanged: (id: string) =>
      serial(id, async () => {
        flows.delete(id);
        const server = await store.get(id);
        const grant =
          server?.auth === "oauth" ? await load(server).catch(() => undefined) : undefined;
        if (!server || !server.enabled || server.auth !== "oauth" || !grant) abort(id);
        if (!server || server.auth !== "oauth")
          await Effect.runPromise(context.secrets.remove(key(id)));
        await changed();
      }),
    status: async () => {
      prune();
      const status = [];
      for (const server of (await store.list()).servers.filter(
        (server) => server.auth === "oauth",
      )) {
        let problem = false;
        const grant = await load(server).catch(() => {
          problem = true;
          return undefined;
        });
        const flow = flows.get(server.id);
        status.push({
          id: server.id,
          label: server.label,
          project: server.project,
          connected: !!grant,
          problem,
          account: grant?.label ?? "Not connected",
          pending: !!flow,
          ...(flow?.authorizationUrl
            ? { authorizationUrl: flow.authorizationUrl, redirectUrl: flow.redirectUrl }
            : {}),
        });
      }
      return status;
    },
    resolve: async (server: Server): Promise<PluginMcpServer> => {
      const grant = await load(server);
      if (!grant) throw Error("Connect an OAuth account for this project first");
      const base = await Effect.runPromise(context.httpBaseUrl);
      return {
        transport: "http",
        url: `${base.replace(/\/$/, "")}/mcp/${server.id}`,
        headers: { Authorization: `Bearer ${grant.routeToken}` },
      };
    },
    handleHttp: async (request: Request): Promise<Response> => {
      const url = new URL(request.url),
        base = context.httpBasePath.replace(/\/$/, "");
      if (url.pathname === `${base}/oauth/callback` && request.method === "GET") {
        prune();
        const state = url.searchParams.get("state"),
          flow = state ? [...flows.values()].find((flow) => equal(flow.state, state)) : undefined;
        if (!flow) return reply("This sign-in is invalid, expired or already completed.", 400);
        try {
          await finish(flow.id, url.href, true);
          return reply("Connected. Return to MCP Manager in T3 Code.");
        } catch {
          return reply("Sign-in could not be completed. Return to MCP Manager and try again.", 400);
        }
      }
      const match = new RegExp(`^${base}/mcp/([a-f0-9-]{36})$`).exec(url.pathname);
      if (!match || url.search) return reply("Not found", 404);
      const id = match[1]!;
      let server: Server, grant: Grant;
      try {
        server = await get(id);
        const current = await load(server);
        if (
          !current ||
          !server.enabled ||
          !equal(
            request.headers.get("authorization")?.replace(/^Bearer\s+/i, "") ?? "",
            current.routeToken,
          )
        )
          return reply("Forbidden", 403);
        grant = await refresh(server);
      } catch {
        return reply("Reconnect this project's account in MCP Manager.", 403);
      }
      if (!["GET", "POST", "DELETE"].includes(request.method))
        return reply("Method not allowed", 405);
      const frontSession = request.headers.get("mcp-session-id"),
        session = frontSession ? sessions.get(frontSession) : undefined;
      if (frontSession && (!session || session.owner !== id))
        return reply("MCP session not found", 404);
      let body: ArrayBuffer | undefined;
      if (request.body) {
        const reader = request.body.getReader(),
          chunks: Uint8Array[] = [];
        let size = 0;
        try {
          for (;;) {
            const part = await reader.read();
            if (part.done) break;
            size += part.value.byteLength;
            if (size > MAX_BODY) {
              await reader.cancel();
              return reply("MCP request is too large", 413);
            }
            chunks.push(part.value);
          }
        } finally {
          reader.releaseLock();
        }
        const buffer = new Uint8Array(size);
        let offset = 0;
        for (const part of chunks) {
          buffer.set(part, offset);
          offset += part.byteLength;
        }
        body = buffer.buffer;
      }
      const additional = await store.credentials(server);
      const upstreamHeaders = new Headers(additional.headers);
      for (const name of [
        "host",
        "connection",
        "content-length",
        "cookie",
        "proxy-authorization",
        "authorization",
        "mcp-session-id",
      ])
        upstreamHeaders.delete(name);
      for (const name of ["content-type", "accept", "mcp-protocol-version", "last-event-id"]) {
        const value = request.headers.get(name);
        if (value) upstreamHeaders.set(name, value);
      }
      if (session) upstreamHeaders.set("mcp-session-id", session.upstream);
      const signal = AbortSignal.any([request.signal, control(id).signal]);
      const send = (token: string) => {
        upstreamHeaders.set("authorization", `Bearer ${token}`);
        return fetcher(server.url, {
          method: request.method,
          headers: upstreamHeaders,
          redirect: "manual",
          signal,
          ...(body === undefined ? {} : { body }),
        });
      };
      let response: Response;
      try {
        response = await send(grant.tokens.access_token);
      } catch {
        return reply("MCP server is unavailable.", 502);
      }
      if (response.status === 401) {
        await response.body?.cancel();
        try {
          grant = await refresh(server, true, grant.tokens.access_token);
          response = await send(grant.tokens.access_token);
        } catch {
          return reply("Reconnect this project's account in MCP Manager.", 403);
        }
      }
      if (response.status === 401 || response.status === 403) {
        await response.body?.cancel();
        return reply("Reconnect this project's account in MCP Manager.", 403);
      }
      if (response.status >= 300 && response.status < 400) {
        await response.body?.cancel();
        return reply("MCP endpoint redirected; update its URL in MCP Manager.", 502);
      }
      const output = new Headers({ "Cache-Control": "no-store" });
      for (const name of ["content-type", "mcp-protocol-version"]) {
        const value = response.headers.get(name);
        if (value) output.set(name, value);
      }
      const upstreamSession = response.headers.get("mcp-session-id");
      if (upstreamSession) {
        const front = session && frontSession ? frontSession : random();
        if (!sessions.has(front) && sessions.size >= 1024)
          sessions.delete(sessions.keys().next().value!);
        sessions.set(front, { owner: id, upstream: upstreamSession });
        output.set("mcp-session-id", front);
      }
      if (request.method === "DELETE" && frontSession) sessions.delete(frontSession);
      return new Response(response.body, { status: response.status, headers: output });
    },
    close: async () => {
      closed = true;
      shutdown.abort();
      flows.clear();
      sessions.clear();
      for (const controller of controllers.values()) controller.abort();
      controllers.clear();
    },
  };
}
export type OAuthManager = Awaited<ReturnType<typeof createOAuth>>;
