import * as Path from "node:path";
import type { PluginMcpContext, PluginMcpServers } from "@t3tools/plugin-sdk";

export const PROVIDERS = [
  ["claudeAgent", "Claude"],
  ["codex", "Codex"],
  ["opencode", "OpenCode"],
  ["cursor", "Cursor"],
  ["grok", "Grok"],
  ["antigravity", "Antigravity"],
  ["kiro", "Kiro"],
  ["muse", "Muse"],
  ["fx", "fx"],
] as const;
export interface Server {
  id: string;
  name: string;
  label: string;
  project: string | null;
  enabled: boolean;
  providers: Record<string, boolean>;
  transport: "stdio" | "http" | "sse";
  command: string;
  args: string[];
  url: string;
  secretKey: string;
  auth: "static" | "oauth";
}
export interface State {
  version: 2;
  servers: Server[];
}
export interface Credentials {
  environment: Record<string, string>;
  headers: Record<string, string>;
}
export const initialState = (): State => ({ version: 2, servers: [] });
export function text(value: unknown, name: string, max = 16384): string {
  if (typeof value !== "string" || value.length > max || value.includes("\0"))
    throw Error(`Invalid ${name}`);
  return value.trim();
}
export function record(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value))
    throw Error("Expected an object");
  return value as Record<string, unknown>;
}
export function stringMap(value: unknown): Record<string, string> {
  const result: Record<string, string> = {};
  for (const [key, item] of Object.entries(record(value))) {
    if (
      !key ||
      key.length > 200 ||
      /[\0\r\n]/.test(key) ||
      typeof item !== "string" ||
      item.length > 16384 ||
      item.includes("\0")
    )
      throw Error("Invalid environment or header value");
    Object.defineProperty(result, key, {
      value: item,
      enumerable: true,
      writable: true,
      configurable: true,
    });
  }
  if (Object.keys(result).length > 100) throw Error("Too many environment or header values");
  return result;
}
export function directory(value: unknown): string {
  const path = text(value, "project directory");
  if (!Path.isAbsolute(path)) throw Error("Project directory must be absolute");
  return Path.normalize(path);
}
export function parseServer(input: Record<string, unknown>, id: string, previous?: Server): Server {
  const name = text(input.name, "server name", 48);
  if (!/^[a-z][a-z0-9-]*$/.test(name))
    throw Error("Use a lowercase server name with letters, numbers and hyphens");
  const label = text(input.label ?? name, "label", 200) || name;
  const project =
    input.scope === "project"
      ? directory(input.project)
      : input.scope === "global"
        ? null
        : (() => {
            throw Error("Invalid scope");
          })();
  const transport = input.transport;
  if (transport !== "stdio" && transport !== "http" && transport !== "sse")
    throw Error("Invalid transport");
  const auth = input.auth ?? previous?.auth ?? "static";
  if (auth !== "static" && auth !== "oauth") throw Error("Invalid authentication mode");
  if (auth === "oauth" && transport !== "http") throw Error("OAuth requires Streamable HTTP");
  const command = text(input.command ?? "", "command");
  let args: unknown;
  try {
    args = typeof input.args === "string" ? JSON.parse(input.args || "[]") : (input.args ?? []);
  } catch {
    throw Error("Arguments must be a JSON array");
  }
  if (
    !Array.isArray(args) ||
    args.length > 100 ||
    args.some((item) => typeof item !== "string" || item.length > 16384 || item.includes("\0"))
  )
    throw Error("Invalid command arguments");
  if (transport === "stdio" && !command) throw Error("A command is required for stdio servers");
  let url = text(input.url ?? "", "URL");
  if (transport !== "stdio") {
    const parsed = new URL(url);
    if (
      !["http:", "https:"].includes(parsed.protocol) ||
      parsed.username ||
      parsed.password ||
      parsed.hash
    )
      throw Error("Use an HTTP(S) URL without embedded credentials or fragments");
    url = parsed.href;
  }
  const providers = { ...(previous?.providers ?? {}) };
  for (const [provider] of PROVIDERS) {
    const value = input[`provider-${provider.toLowerCase()}`];
    if (value !== undefined && typeof value !== "boolean")
      throw Error("Invalid provider selection");
    providers[provider] = value ?? providers[provider] ?? true;
  }
  return {
    id,
    name,
    label,
    project,
    enabled: previous?.enabled ?? true,
    providers,
    transport,
    command,
    args: args as string[],
    url,
    secretKey: previous?.secretKey ?? `server-${id}`,
    auth,
  };
}
export function decodeState(value: unknown): State {
  const input = record(value);
  if (
    (input.version !== 1 && input.version !== 2) ||
    !Array.isArray(input.servers) ||
    input.servers.length > 40
  )
    throw Error("Invalid MCP manager state");
  const servers = input.servers.map((value) => {
    const row = record(value);
    const id = text(row.id, "id", 80);
    if (!/^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/.test(id))
      throw Error("Invalid server id");
    const server = parseServer({ ...row, scope: row.project === null ? "global" : "project" }, id);
    if (
      typeof row.enabled !== "boolean" ||
      typeof row.secretKey !== "string" ||
      !/^server-[a-f0-9]{32}$/.test(row.secretKey)
    )
      throw Error("Invalid server metadata");
    const providers = Object.fromEntries(
      Object.entries(record(row.providers)).map(([key, enabled]) => {
        if (!/^[a-zA-Z][a-zA-Z0-9_.:-]{0,127}$/.test(key) || typeof enabled !== "boolean")
          throw Error("Invalid provider rule");
        return [key, enabled];
      }),
    );
    return { ...server, secretKey: row.secretKey as string, enabled: row.enabled, providers };
  });
  if (
    new Set(servers.map((s) => s.id)).size !== servers.length ||
    new Set(servers.map((s) => JSON.stringify([s.project, s.name]))).size !== servers.length
  )
    throw Error("Duplicate MCP server");
  return { version: 2, servers };
}
export function selectedServers(
  state: State,
  launch: PluginMcpContext,
  project: string | undefined,
): Server[] {
  const applicable = new Map(
    state.servers.filter((s) => s.project === null).map((s) => [s.name, s]),
  );
  if (project)
    for (const server of state.servers)
      if (server.project === project) applicable.set(server.name, server);
  return [...applicable.values()].filter(
    (s) =>
      s.enabled &&
      (s.providers[launch.instanceId ?? ""] ??
        s.providers[launch.provider] ??
        PROVIDERS.some(([id]) => id === launch.provider)),
  );
}
export function serverConfig(server: Server, credentials: Credentials): PluginMcpServers[string] {
  return server.transport === "stdio"
    ? {
        transport: "stdio",
        command: server.command,
        args: server.args,
        environment: credentials.environment,
      }
    : { transport: server.transport, url: server.url, headers: credentials.headers };
}
