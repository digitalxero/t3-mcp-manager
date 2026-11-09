import assert from "node:assert/strict";
import { readFile, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { PluginView, PluginMcpServers } from "@t3tools/plugin-sdk";
import plugin from "../dist/plugin.mjs";

const manifest = JSON.parse(await readFile(new URL("../t3-plugin.json", import.meta.url), "utf8"));
const pkg = JSON.parse(await readFile(new URL("../package.json", import.meta.url), "utf8"));
assert.equal(manifest.version, pkg.version);
assert.equal(plugin.version, pkg.version);
assert.equal(plugin.id, manifest.id);
const directory = await mkdtemp(join(tmpdir(), "t3-mcp-artifact-"));
const secrets = new Map();
try {
  const contribution = await Effect.runPromise(
    plugin.activate({
      id: plugin.id,
      dataDirectory: directory,
      capabilities: ["provider-mcp-servers"],
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
    }),
  );
  Schema.decodeUnknownSync(PluginView)(await Effect.runPromise(contribution.views[0].read()));
  const save = contribution.commands.find((command) => command.id === "save");
  await Effect.runPromise(
    save.run({
      name: "fixture",
      scope: "global",
      transport: "stdio",
      command: "fixture-server",
      args: "[]",
    }),
  );
  const servers = await Effect.runPromise(
    contribution.mcpServers({ provider: "codex", instanceId: "codex" }),
  );
  Schema.decodeUnknownSync(PluginMcpServers)(servers);
  assert.equal(servers["mcp-manager-fixture"].command, "fixture-server");
  console.log("Bundled artifact: activation, settings schema, command and MCP contribution passed");
} finally {
  await rm(directory, { recursive: true, force: true });
}
