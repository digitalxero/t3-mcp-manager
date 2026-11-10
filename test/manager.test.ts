import { test } from "node:test";
import assert from "node:assert/strict";
import * as Fs from "node:fs/promises";
import * as Path from "node:path";
import * as Os from "node:os";
import { execFileSync } from "node:child_process";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { PluginView, type PluginContext, type PluginInput } from "@t3tools/plugin-sdk";
import { createStore } from "../src/store.ts";
import { activateManager } from "../src/plugin.ts";

async function fixture() {
  const root = await Fs.mkdtemp(Path.join(Os.tmpdir(), "t3-mcp-manager-"));
  const secrets = new Map<string, string>();
  const context: PluginContext = {
    id: "mcp-manager",
    dataDirectory: Path.join(root, "data"),
    capabilities: ["provider-mcp-servers", "http-response-timeout", "settings-pages"],
    httpBasePath: "/api/plugins/mcp-manager",
    httpBaseUrl: Effect.succeed("http://localhost/api/plugins/mcp-manager"),
    secrets: {
      get: (key) => Effect.sync(() => secrets.get(key) ?? null),
      set: (key, value) =>
        Effect.sync(() => {
          secrets.set(key, value);
        }),
      remove: (key) =>
        Effect.sync(() => {
          secrets.delete(key);
        }),
    },
  };
  return {
    root,
    context,
    secrets,
    async [Symbol.asyncDispose]() {
      await Fs.rm(root, { recursive: true, force: true });
    },
  };
}
const definition = (extra: PluginInput = {}): PluginInput => ({
  name: "database",
  scope: "global",
  transport: "stdio",
  command: "server",
  args: "[]",
  ...extra,
});
const session = (extra = {}) => ({
  provider: "claudeAgent",
  instanceId: "claude-personal",
  ...extra,
});

test("project overrides, disabled overrides and provider instance rules survive reload", async () => {
  await using f = await fixture();
  const project = Path.join(f.root, "project");
  const other = Path.join(f.root, "other");
  await Fs.mkdir(project);
  await Fs.mkdir(other);
  let store = await createStore(f.context);
  const global = await store.save(definition({ command: "global-server" }));
  const local = await store.save(
    definition({
      scope: "project",
      project,
      command: "project-server",
      environment: '{"TOKEN":"project-secret"}',
    }),
  );
  assert.equal(
    (await store.resolve(session({ projectDirectory: other })))["mcp-manager-database"]?.transport,
    "stdio",
  );
  assert.deepEqual(
    (await store.resolve(session({ projectDirectory: project })))["mcp-manager-database"],
    {
      transport: "stdio",
      command: "project-server",
      args: [],
      environment: { TOKEN: "project-secret" },
    },
  );
  await store.setEnabled(local.id, false);
  assert.deepEqual(await store.resolve(session({ projectDirectory: project })), {});
  await store.setEnabled(local.id, true);
  await store.setProvider(local.id, "claudeAgent", false);
  assert.deepEqual(await store.resolve(session({ projectDirectory: project })), {});
  await store.setProvider(local.id, "claude-personal", true);
  store = await createStore(f.context);
  assert.ok((await store.resolve(session({ projectDirectory: project })))["mcp-manager-database"]);
  assert.deepEqual(
    await store.resolve(session({ instanceId: "claude-work", projectDirectory: project })),
    {},
  );
  await store.remove(local.id);
  assert.equal(
    (await store.resolve(session({ projectDirectory: project })))["mcp-manager-database"]
      ?.transport,
    "stdio",
  );
  assert.equal(f.secrets.size, 1);
  await store.remove(global.id);
  assert.equal(f.secrets.size, 0);
  assert.deepEqual(await (await createStore(f.context)).resolve(session()), {});
});

test("linked worktrees use their owning project while another repository stays separate", async () => {
  await using f = await fixture();
  const project = Path.join(f.root, "repo"),
    worktree = Path.join(f.root, "linked");
  execFileSync("git", ["init", "--quiet", project]);
  execFileSync("git", [
    "-C",
    project,
    "-c",
    "user.name=Test",
    "-c",
    "user.email=test@example.invalid",
    "commit",
    "--quiet",
    "--allow-empty",
    "-m",
    "test: initial",
  ]);
  execFileSync("git", ["-C", project, "worktree", "add", "--quiet", "-b", "linked", worktree]);
  const store = await createStore(f.context);
  await store.save(definition({ scope: "project", project }));
  assert.ok((await store.resolve(session({ cwd: worktree })))["mcp-manager-database"]);
  assert.deepEqual(await store.resolve(session({ cwd: f.root })), {});
});

test("credentials are write-only; update, clearing and removal do not expose them", async () => {
  await using f = await fixture();
  await using plugin = await activateManager(f.context);
  const run = (id: string, input: PluginInput) =>
    Effect.runPromise(plugin.commands.find((c) => c.id === id)!.run(input));
  const saved = (await run(
    "save",
    definition({
      transport: "http",
      url: "https://mcp.example.invalid/api",
      headers: '{"Authorization":"Bearer test-secret","X-Custom":"private"}',
    }),
  )) as { id: string };
  const native = await Effect.runPromise(plugin.mcpServers(session()));
  assert.deepEqual(native["mcp-manager-database"], {
    transport: "http",
    url: "https://mcp.example.invalid/api",
    headers: { Authorization: "Bearer test-secret", "X-Custom": "private" },
  });
  const view = await Effect.runPromise(plugin.views[0]!.read());
  Schema.decodeUnknownSync(PluginView)(view);
  assert.ok(!JSON.stringify([view, await run("list", {}), saved]).includes("test-secret"));
  const disk = await Fs.readFile(Path.join(f.context.dataDirectory, "servers.json"), "utf8");
  assert.ok(!disk.includes("test-secret"));
  await run("save", definition({ transport: "http", url: "https://mcp.example.invalid/updated" }));
  assert.equal(f.secrets.size, 1);
  assert.match(
    JSON.stringify(await Effect.runPromise(plugin.mcpServers(session()))),
    /test-secret/,
  );
  await run("clear-credentials", { id: saved.id });
  assert.ok(
    !JSON.stringify(await Effect.runPromise(plugin.mcpServers(session()))).includes("test-secret"),
  );
  await run("remove", { id: saved.id });
  assert.equal(f.secrets.size, 0);
});

test("concurrent writes serialize, rejected writes leave the committed state usable", async () => {
  await using f = await fixture();
  const store = await createStore(f.context);
  await Promise.all(
    Array.from({ length: 12 }, (_, i) => store.save(definition({ name: `server-${i}` }))),
  );
  assert.equal((await (await createStore(f.context)).list()).servers.length, 12);
  const before = await store.list();
  await assert.rejects(store.save(definition({ args: "[not-json]" })));
  await assert.rejects(store.save(definition({ transport: "http", url: "file:///etc/passwd" })));
  await assert.rejects(
    store.save(
      definition({
        transport: "http",
        url: "https://mcp.example.invalid",
        headers: '{"X-Header":"a\\r\\nb"}',
      }),
    ),
  );
  await assert.rejects(store.save(definition({ scope: "project", project: "relative/path" })));
  assert.deepEqual(await store.list(), before);
  assert.equal(f.secrets.size, 12);
  const id = before.servers[0]!.id;
  await store.setEnabled(id, false);
  assert.equal((await store.list()).servers.find((s) => s.id === id)!.enabled, false);
});

test("failed atomic replacement rolls back the new credential without replacing old policy", async () => {
  await using f = await fixture();
  const store = await createStore(f.context);
  await store.save(definition({ environment: '{"TOKEN":"original"}' }));
  const path = Path.join(f.context.dataDirectory, "servers.json");
  await Fs.rename(path, path + ".old");
  await Fs.mkdir(path);
  await assert.rejects(store.save(definition({ environment: '{"TOKEN":"replacement"}' })));
  assert.equal(f.secrets.size, 1);
  assert.match(JSON.stringify(await store.resolve(session())), /original/);
  await Fs.rmdir(path);
  await Fs.rename(path + ".old", path);
  assert.match(JSON.stringify(await (await createStore(f.context)).resolve(session())), /original/);
  assert.deepEqual((await Fs.readdir(f.context.dataDirectory)).sort(), ["servers.json"]);
});

test("corrupt state, missing credentials and incompatible hosts fail closed", async () => {
  await using f = await fixture();
  await assert.rejects(activateManager({ ...f.context, capabilities: [] }));
  const store = await createStore(f.context);
  await store.save(definition());
  f.secrets.clear();
  await assert.rejects(store.resolve(session()), /credential entry is missing/);
  await Fs.writeFile(
    Path.join(f.context.dataDirectory, "servers.json"),
    '{"version":99,"servers":[]}',
  );
  await assert.rejects(createStore(f.context), /Invalid MCP manager state/);
});

test("upgrades v0.1 settings without changing their scoped policies or credentials", async () => {
  await using f = await fixture();
  const store = await createStore(f.context);
  const saved = await store.save(definition({ environment: '{"TOKEN":"legacy-secret"}' }));
  const path = Path.join(f.context.dataDirectory, "servers.json");
  const before = JSON.parse(await Fs.readFile(path, "utf8"));
  before.version = 1;
  for (const server of before.servers) delete server.auth;
  await Fs.writeFile(path, JSON.stringify(before));
  const upgraded = await createStore(f.context);
  assert.equal((await upgraded.list()).servers[0]!.auth, "static");
  assert.match(JSON.stringify(await upgraded.resolve(session())), /legacy-secret/);
  await upgraded.setEnabled(saved.id, false);
  assert.equal(JSON.parse(await Fs.readFile(path, "utf8")).version, 2);
  assert.equal(f.secrets.size, 1);
});

test("requires standalone settings pages and contributes no inline plugin view", async () => {
  await using f = await fixture();
  await assert.rejects(
    activateManager({
      ...f.context,
      capabilities: ["provider-mcp-servers", "http-response-timeout"],
    }),
  );
  await using plugin = await activateManager(f.context);
  assert.equal(plugin.views[0]?.location, "settings-page");
  Schema.decodeUnknownSync(PluginView)(await Effect.runPromise(plugin.views[0]!.read()));
});
