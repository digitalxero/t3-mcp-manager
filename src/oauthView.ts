import type { PluginView } from "@t3tools/plugin-sdk";
import type { OAuthManager } from "./oauth.ts";

export function oauthBlocks(
  accounts: Awaited<ReturnType<OAuthManager["status"]>>,
  callback: string,
): PluginView["blocks"] {
  const blocks: Array<PluginView["blocks"][number]> = [
    {
      id: "oauth-help",
      kind: "text",
      text: `OAuth accounts belong to a server definition and its project. To use different accounts at the same MCP URL, add one project-scoped definition per project with Authentication set to OAuth. A global connection is shared only by projects that inherit that global definition. Tokens stay on this environment. Default callback URL: ${callback}`,
    },
  ];
  if (accounts.length === 0) return blocks;
  blocks.push({
    id: "oauth-accounts",
    kind: "table",
    columns: ["Server", "Project", "Account label", "Connection"],
    rows: accounts.map((account) => ({
      id: account.id,
      cells: [
        account.label,
        account.project ?? "All projects (default)",
        account.account,
        account.pending
          ? "Waiting for sign-in"
          : account.connected
            ? "Connected"
            : account.problem
              ? "Reconnect required"
              : "Not connected",
      ],
      actions: [
        {
          id: "connect",
          label: account.connected ? "Use another account" : "Sign in",
          command: "oauth-start",
          input: { id: account.id },
        },
        ...(account.pending
          ? [
              {
                id: "cancel",
                label: "Cancel sign-in",
                command: "oauth-cancel",
                input: { id: account.id },
              },
            ]
          : []),
        ...(account.connected || account.problem
          ? [
              {
                id: "disconnect",
                label: "Disconnect this account",
                command: "oauth-disconnect",
                input: { id: account.id },
              },
            ]
          : []),
      ],
    })),
  });
  blocks.push({
    id: "oauth-connect",
    kind: "form",
    label: "Connect an OAuth account",
    command: "oauth-start",
    input: {},
    fields: [
      {
        name: "id",
        label: "Server and project",
        kind: "select",
        required: true,
        options: accounts.map((account) => ({
          value: account.id,
          label: `${account.label} (${account.project ?? "all projects"})`.slice(0, 200),
        })),
      },
      { name: "label", label: "Account label (for example, Personal or Work)", kind: "text" },
      {
        name: "choose-account",
        label: "Ask the provider to choose an account",
        kind: "boolean",
        value: true,
      },
      { name: "scopes", label: "OAuth scopes (optional, space-separated)", kind: "text" },
      {
        name: "client-id",
        label: "Registered client ID (optional; otherwise register automatically)",
        kind: "text",
      },
      {
        name: "issuer",
        label: "Registered client's authorization server issuer (required with a secret)",
        kind: "text",
      },
      {
        name: "client-secret",
        label: "Registered client secret (optional, write-only)",
        kind: "password",
      },
      {
        name: "client-metadata-url",
        label: "Public client metadata document URL (optional)",
        kind: "text",
      },
      {
        name: "redirect-url",
        label:
          "Callback URL override (optional; must be routed to this environment or completed below)",
        kind: "text",
      },
    ],
  });
  for (const account of accounts)
    if (account.pending && account.authorizationUrl) {
      blocks.push({
        id: `signin-${account.id}`,
        kind: "link",
        label: `Sign in to ${account.label} (${account.project ?? "all projects"})`.slice(0, 200),
        url: account.authorizationUrl,
      });
      blocks.push({
        id: `callback-${account.id}`,
        kind: "form",
        label: "Complete sign-in from a remote browser",
        command: "oauth-complete",
        input: { id: account.id },
        fields: [
          {
            name: "callback",
            label:
              "Full callback URL (paste the address after signing in if the browser cannot reach the environment)",
            kind: "password",
            required: true,
          },
        ],
      });
    }
  return blocks;
}
