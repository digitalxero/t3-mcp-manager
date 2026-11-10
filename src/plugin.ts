import {
  PluginError,
  type PluginContext,
  type PluginAction,
  type PluginField,
  type PluginInput,
  type PluginMcpContext,
  type PluginView,
  type T3Plugin,
} from "@t3tools/plugin-sdk";
import * as Effect from "effect/Effect";
import * as PubSub from "effect/PubSub";
import * as Stream from "effect/Stream";
import { createOAuth } from "./oauth.ts";
import { oauthBlocks } from "./oauthView.ts";
import { PROVIDERS, type Server } from "./config.ts";
import { createStore, type Store } from "./store.ts";
const VERSION = "0.2.0";
const call = <A>(operation: string, run: () => Promise<A>) =>
  Effect.tryPromise({ try: run, catch: (cause) => new PluginError({ operation, cause }) });
const action = (id: string, label: string, command: string, input: PluginInput): PluginAction => ({
  id,
  label,
  command,
  input,
});
const publicServer = (server: Server) => ({
  id: server.id,
  name: server.name,
  label: server.label,
  project: server.project,
  enabled: server.enabled,
  providers: server.providers,
  transport: server.transport,
  auth: server.auth,
});
function settings(state: Awaited<ReturnType<Store["list"]>>): PluginView {
  const fields: (typeof PluginField.Type)[] = [
    {
      name: "name",
      label: "Server name (letters, numbers, hyphens)",
      kind: "text",
      required: true,
    },
    { name: "label", label: "Display name", kind: "text" },
    {
      name: "scope",
      label: "Scope",
      kind: "select",
      value: "global",
      options: [
        { label: "All projects on this environment", value: "global" },
        { label: "One project (including its worktrees)", value: "project" },
      ],
    },
    { name: "project", label: "Absolute project directory (project scope only)", kind: "text" },
    {
      name: "transport",
      label: "Transport",
      kind: "select",
      value: "stdio",
      options: [
        { label: "Local command (stdio)", value: "stdio" },
        { label: "Streamable HTTP", value: "http" },
        { label: "Legacy SSE", value: "sse" },
      ],
    },
    { name: "command", label: "Command (stdio)", kind: "text" },
    { name: "args", label: "Arguments as a JSON array", kind: "text", value: "[]" },
    { name: "url", label: "Server URL (HTTP or SSE)", kind: "text" },
    {
      name: "environment",
      label: "Secret environment variables as JSON (blank keeps existing)",
      kind: "password",
    },
    {
      name: "headers",
      label: "Secret HTTP headers as JSON (blank keeps existing)",
      kind: "password",
    },
    {
      name: "auth",
      label: "Authentication",
      kind: "select",
      value: "static",
      options: [
        { label: "Environment variables or HTTP headers", value: "static" },
        { label: "OAuth (Streamable HTTP)", value: "oauth" },
      ],
    },
    ...PROVIDERS.map(([id, label]) => ({
      name: `provider-${id.toLowerCase()}`,
      label: `Enable for ${label}`,
      kind: "boolean" as const,
      value: true,
    })),
  ];
  return {
    title: "MCP Manager",
    blocks: [
      {
        id: "scope",
        kind: "text",
        text: "Environment-wide defaults with project overrides. Saving the same name and scope updates that definition. A project definition replaces the global definition with the same name, including when disabled. Changes apply to new provider sessions; restart an existing session to refresh its servers.",
      },
      {
        id: "support",
        kind: "text",
        text: "Claude and Cursor accept all three transports. Codex and Muse accept stdio and Streamable HTTP. ACP providers (Grok, Antigravity, Kiro and fx) require advertised transport support; stdio is the portable choice. OpenCode requires a T3-managed server. Pi is not supported. Provider switches also apply to future instances of that provider.",
      },
      {
        id: "credentials",
        kind: "text",
        text: "Commands execute on the selected environment. Keep credentials in the secret fields, never in command arguments or URLs. Existing credentials are never displayed or prefilled. Removing the plugin stops applying its configuration and removes its managed settings; provider configuration files are never rewritten.",
      },
      {
        id: "servers",
        kind: "table",
        columns: ["Name", "Scope", "Transport", "Authentication", "State", "Enabled providers"],
        rows: state.servers.map((server) => ({
          id: server.id,
          cells: [
            server.label,
            server.project ?? "All projects",
            server.transport,
            server.auth === "oauth" ? "OAuth" : "Static credentials",
            server.enabled ? "Enabled" : "Disabled",
            PROVIDERS.filter(([id]) => server.providers[id] !== false)
              .map(([, label]) => label)
              .join(", ") || "None",
          ],
          actions: [
            action("toggle", server.enabled ? "Disable" : "Enable", "set-enabled", {
              id: server.id,
              enabled: !server.enabled,
            }),
            ...PROVIDERS.map(([id, label]) =>
              action(
                `toggle-${id.toLowerCase()}`,
                `${server.providers[id] === false ? "Enable" : "Disable"} ${label}`,
                "set-provider",
                { id: server.id, provider: id, enabled: server.providers[id] === false },
              ),
            ),
            action("clear-credentials", "Clear static credentials", "clear-credentials", {
              id: server.id,
            }),
            action("remove", "Remove", "remove", { id: server.id }),
          ],
        })),
      },
      {
        id: "save",
        kind: "form",
        label: "Add or update an MCP server",
        command: "save",
        fields,
        input: {},
      },
      {
        id: "instance-rule",
        kind: "form",
        label: "Override one provider instance",
        command: "set-provider",
        fields: [
          {
            name: "id",
            label: "Server",
            kind: "select",
            options: state.servers.map((s) => ({
              label: `${s.label} (${s.project ?? "all projects"})`.slice(0, 200),
              value: s.id,
            })),
            required: true,
          },
          { name: "provider", label: "Provider instance ID", kind: "text", required: true },
          { name: "enabled", label: "Enabled", kind: "boolean", value: true },
        ],
        input: {},
      },
    ],
  };
}
export async function activateManager(context: PluginContext) {
  if (
    !context.capabilities?.includes("provider-mcp-servers") ||
    !context.capabilities?.includes("http-response-timeout") ||
    !context.capabilities?.includes("settings-pages")
  )
    throw Error(
      "This plugin requires the PoC host with provider MCP configuration and standalone settings pages",
    );
  const store = await createStore(context);
  const changes = await Effect.runPromise(PubSub.unbounded<void>());
  const oauth = await createOAuth(context, store, {
    changed: () => Effect.runPromise(PubSub.publish(changes, undefined)).then(() => undefined),
  });
  const descriptor = (
    id: string,
    title: string,
    run: (input: PluginInput) => Promise<unknown>,
    read = false,
  ) => ({
    id,
    title,
    description: title,
    permission: read ? ("orchestration:read" as const) : ("providers:manage" as const),
    agent: read,
    run: (input: PluginInput) =>
      call(id, () => run(input)).pipe(Effect.map((value) => value as import("effect/Schema").Json)),
  });
  return {
    commands: [
      descriptor(
        "list",
        "List managed MCP server policies without credentials",
        async () => ({ servers: (await store.list()).servers.map(publicServer) }),
        true,
      ),
      descriptor("save", "Save an MCP server definition and write-only credentials", (input) =>
        store.save(input).then(async (result) => {
          await oauth.policyChanged(result.id);
          return result;
        }),
      ),
      descriptor("set-enabled", "Enable or disable an MCP server", (input) =>
        store.setEnabled(input.id, input.enabled).then(async (result) => {
          await oauth.policyChanged(result.id);
          return result;
        }),
      ),
      descriptor(
        "set-provider",
        "Enable or disable an MCP server for a provider or provider instance",
        (input) => store.setProvider(input.id, input.provider, input.enabled),
      ),
      descriptor("clear-credentials", "Clear credentials for an MCP server", (input) =>
        store.clearCredentials(input.id).then(async (result) => {
          await oauth.policyChanged(String(input.id));
          return result;
        }),
      ),
      descriptor("remove", "Remove an MCP server definition and its credentials", (input) =>
        store.remove(input.id).then(async (result) => {
          await oauth.policyChanged(String(input.id));
          return result;
        }),
      ),
      descriptor(
        "oauth-start",
        "Connect an independent OAuth account for a server and project",
        (input) => oauth.start(input),
      ),
      descriptor("oauth-complete", "Complete this project's OAuth sign-in", (input) =>
        oauth.finish(input),
      ),
      descriptor("oauth-cancel", "Cancel this project's pending OAuth sign-in", (input) =>
        oauth.cancel(String(input.id)),
      ),
      descriptor("oauth-disconnect", "Disconnect only this project's OAuth account", (input) =>
        oauth.disconnect(String(input.id)),
      ),
    ],
    views: [
      {
        id: "settings",
        title: "MCP Manager",
        location: "settings-page" as const,
        permission: "providers:manage" as const,
        read: () =>
          call("settings", async () => {
            const view = settings(await store.list());
            const base = await Effect.runPromise(context.httpBaseUrl);
            return {
              ...view,
              blocks: [
                ...view.blocks,
                ...oauthBlocks(await oauth.status(), base.replace(/\/$/, "") + "/oauth/callback"),
              ],
            };
          }),
      },
    ],
    mcpServers: (context: PluginMcpContext) =>
      call("prepare-mcp", () => store.resolve(context, oauth.resolve)),
    httpResponseTimeoutMs: 3_900_000,
    handleHttp: (request: Request) => call("http", () => oauth.handleHttp(request)),
    changes: Stream.fromPubSub(changes),
    async [Symbol.asyncDispose]() {
      await oauth.close();
      await Effect.runPromise(PubSub.shutdown(changes));
    },
  };
}
export default {
  apiVersion: 1,
  id: "mcp-manager",
  name: "MCP Manager",
  version: VERSION,
  description:
    "Manage MCP servers across projects and provider instances without editing provider configuration files.",
  activate: (context) =>
    call("activate", () => activateManager(context)).pipe(
      Effect.tap((contributions) =>
        Effect.addFinalizer(() =>
          call("shutdown", () => contributions[Symbol.asyncDispose]()).pipe(
            Effect.catch(() => Effect.void),
          ),
        ),
      ),
    ),
} satisfies T3Plugin;
