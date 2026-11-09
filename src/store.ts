import * as Fs from "node:fs/promises";
import * as Path from "node:path";
import { randomUUID } from "node:crypto";
import type {
  PluginContext,
  PluginMcpContext,
  PluginMcpServers,
  PluginMcpServer,
} from "@t3tools/plugin-sdk";
import * as Effect from "effect/Effect";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import {
  decodeState,
  directory,
  initialState,
  parseServer,
  record,
  selectedServers,
  serverConfig,
  stringMap,
  text,
  type State,
  type Server,
  type Credentials,
} from "./config.ts";
const exec = promisify(execFile);
const emptyCredentials = (): Credentials => ({ environment: {}, headers: {} });
const ownedSecretKey = () => `server-${randomUUID().replaceAll("-", "")}`;
const isMissing = (error: unknown) =>
  error instanceof Error && "code" in error && error.code === "ENOENT";

export async function createStore(context: PluginContext) {
  await Fs.mkdir(context.dataDirectory, { recursive: true });
  const path = Path.join(context.dataDirectory, "servers.json");
  let state: State;
  try {
    state = decodeState(JSON.parse(await Fs.readFile(path, "utf8")));
  } catch (error) {
    if (!isMissing(error)) throw error;
    state = initialState();
  }
  let queue: Promise<unknown> = Promise.resolve();
  const serial = <A>(operation: () => Promise<A>): Promise<A> => {
    const next = queue.then(operation);
    queue = next.catch(() => undefined);
    return next;
  };
  const persist = async (next: State) => {
    const temporary = `${path}.${randomUUID()}.tmp`;
    try {
      const file = await Fs.open(temporary, "wx", 0o600);
      try {
        await file.writeFile(JSON.stringify(next, null, 2) + "\n");
        await file.sync();
      } finally {
        await file.close();
      }
      await Fs.rename(temporary, path);
      state = next;
    } finally {
      await Fs.rm(temporary, { force: true });
    }
  };
  const credentials = async (server: Server): Promise<Credentials> => {
    const value = await Effect.runPromise(context.secrets.get(server.secretKey));
    if (value === null) throw Error("MCP credential entry is missing");
    const parsed = record(JSON.parse(value));
    return { environment: stringMap(parsed.environment), headers: stringMap(parsed.headers) };
  };
  const cleanup = async (key: string) => {
    // State commits first; cleanup failure must not report an already committed command as failed.
    try {
      await Effect.runPromise(context.secrets.remove(key));
    } catch {
      /* Removal also clears the plugin's secret namespace. */
    }
  };
  const find = (id: unknown) => {
    const server = state.servers.find((s) => s.id === id);
    if (!server) throw Error("Unknown MCP server");
    return server;
  };
  const update = (server: Server) =>
    persist({ version: 1, servers: state.servers.map((s) => (s.id === server.id ? server : s)) });
  return {
    list: () => serial(async () => structuredClone(state)),
    save: (input: Record<string, unknown>) =>
      serial(async () => {
        const project =
          input.scope === "project" ? await Fs.realpath(directory(input.project)) : null;
        const name = text(input.name, "server name", 48);
        const previous = state.servers.find((s) => s.name === name && s.project === project);
        if (!previous && state.servers.length >= 40)
          throw Error("At most 40 server definitions are supported");
        const server = parseServer({ ...input, project }, previous?.id ?? randomUUID(), previous);
        const before = previous ? await credentials(previous) : emptyCredentials();
        const after = { ...before };
        for (const [field, target] of [
          ["environment", "environment"],
          ["headers", "headers"],
        ] as const) {
          const value = input[field];
          if (typeof value === "string" && value.trim())
            after[target] = stringMap(JSON.parse(value));
          else if (value !== undefined && typeof value !== "string")
            throw Error("Credentials must be JSON objects");
        }
        if (
          server.transport !== "stdio" &&
          Object.entries(after.headers).some(([key, value]) => /[\r\n]/.test(key + value))
        )
          throw Error("Invalid HTTP header");
        server.secretKey = ownedSecretKey();
        await Effect.runPromise(context.secrets.set(server.secretKey, JSON.stringify(after)));
        try {
          await persist({
            version: 1,
            servers: previous
              ? state.servers.map((s) => (s.id === previous.id ? server : s))
              : [...state.servers, server],
          });
        } catch (error) {
          await cleanup(server.secretKey);
          throw error;
        }
        if (previous) await cleanup(previous.secretKey);
        return { id: server.id, saved: true };
      }),
    setEnabled: (id: unknown, enabled: unknown) =>
      serial(async () => {
        if (typeof enabled !== "boolean") throw Error("Invalid enabled value");
        const server = find(id);
        await update({ ...server, enabled });
        return { id: server.id, enabled };
      }),
    setProvider: (id: unknown, provider: unknown, enabled: unknown) =>
      serial(async () => {
        const key = text(provider, "provider", 128);
        if (!/^[a-zA-Z][a-zA-Z0-9_.:-]*$/.test(key) || typeof enabled !== "boolean")
          throw Error("Invalid provider rule");
        const server = find(id);
        await update({ ...server, providers: { ...server.providers, [key]: enabled } });
        return { id: server.id, provider: key, enabled };
      }),
    remove: (id: unknown) =>
      serial(async () => {
        const server = find(id);
        await persist({ version: 1, servers: state.servers.filter((s) => s.id !== id) });
        await cleanup(server.secretKey);
        return { removed: true };
      }),
    clearCredentials: (id: unknown) =>
      serial(async () => {
        const server = find(id);
        const next = { ...server, secretKey: ownedSecretKey() };
        await Effect.runPromise(
          context.secrets.set(next.secretKey, JSON.stringify(emptyCredentials())),
        );
        try {
          await update(next);
        } catch (error) {
          await cleanup(next.secretKey);
          throw error;
        }
        await cleanup(server.secretKey);
        return { cleared: true };
      }),
    resolve: (launch: PluginMcpContext) =>
      serial(async (): Promise<PluginMcpServers> => {
        let project = launch.projectDirectory
          ? await Fs.realpath(launch.projectDirectory)
          : undefined;
        if (!project && launch.cwd) {
          const cwd = await Fs.realpath(launch.cwd);
          try {
            const result = await exec(
              "git",
              ["-C", cwd, "rev-parse", "--path-format=absolute", "--git-common-dir"],
              { timeout: 2000, maxBuffer: 16384, windowsHide: true },
            );
            const common = await Fs.realpath(result.stdout.trim());
            project = Path.basename(common) === ".git" ? Path.dirname(common) : cwd;
          } catch {
            project = cwd;
          }
          const explicit = state.servers
            .map((s) => s.project)
            .filter((p): p is string => p !== null && (cwd === p || cwd.startsWith(p + Path.sep)))
            .sort((a, b) => b.length - a.length)[0];
          project = explicit ?? project;
        }
        const result: Record<string, PluginMcpServer> = {};
        for (const server of selectedServers(state, launch, project))
          result[`mcp-manager-${server.name}`] = serverConfig(server, await credentials(server));
        return result;
      }),
  };
}
export type Store = Awaited<ReturnType<typeof createStore>>;
