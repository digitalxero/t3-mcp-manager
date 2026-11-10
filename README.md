# T3 MCP Manager

Manage MCP servers in T3 Code without editing each provider's configuration files.
Definitions belong to the selected T3 environment: use them for every project,
override them for one project, and switch them on or off by provider or provider
instance. Web, desktop and mobile use T3's portable plugin settings view.

This is a prototype for the [repository-installed plugin host](https://github.com/pingdotgg/t3code/discussions/23574).
It requires an updated [plugin host](https://github.com/djgilcrease/t3code/tree/t3/create-plugin-system)
with `provider-mcp-servers` and `http-response-timeout` capabilities.
Stock T3 Code and older plugin-host builds do not support it; activation fails
explicitly on those builds.

## Install and configure

In **Settings → Plugins**, install `https://github.com/digitalxero/t3-mcp-manager`.
The host selects the newest stable version tag, checks for updates daily or on
request, and lets you apply an update or remove the plugin there. The repository
includes a bundled `dist/plugin.mjs`; installation runs no package manager or build
scripts. For local development, register this checkout's `dist/plugin.mjs` using
the host's local plugin configuration.

Open **Settings → Plugins → MCP Manager** on the environment that runs your
providers. Add a name, choose all projects or an absolute project directory, then
choose a local command (stdio), Streamable HTTP endpoint, or legacy SSE endpoint.
Arguments are a JSON array, for example `["--read-only"]`. A local command and its
dependencies must already be installed on that environment.

Environment variables and HTTP headers are write-only JSON objects stored through
T3's plugin secret store. For example, enter `{"Authorization":"Bearer …"}` in
the headers field. Blank fields preserve existing credentials when updating;
`{}` replaces that credential group with an empty object. **Clear credentials**
clears both groups. Keep credentials out of URLs and command arguments.

Saving the same name and scope updates that definition. Project definitions
replace global definitions with the same name, including disabled definitions;
this lets you turn off a global server for just one project. Linked Git worktrees
inherit their owning project's policy. Non-Git projects use their configured
directory, including nested working directories.

Provider buttons change the default for every instance of that provider. The
instance override form accepts a T3 provider instance ID and takes precedence
over that default. Definitions and credentials never leave their owning
environment except when passed to the selected provider's MCP client.

Changes apply when a **new provider session** is created. Restart an existing
session to refresh its servers. Removing a definition deletes its policy and
credentials. Removing the plugin stops configuring future sessions and lets the
host remove its managed data. Existing running provider sessions retain the
configuration they received until they stop. Provider configuration files and
T3's built-in MCP endpoint remain unchanged.

## Different OAuth accounts per project

For a Streamable HTTP MCP server, choose **OAuth** as its authentication mode.
Create one project-scoped definition per project, using the same server name and
URL if appropriate. In **OAuth accounts**, sign in separately for each project.
For example, connect Project A to a personal account and Project B to a work
account. A global OAuth connection is shared only by projects that inherit its
global definition; a project override never borrows that global connection.

Sign-in uses the server's OAuth discovery and PKCE. The default flow registers
an OAuth client automatically. If the server requires a registered client, enter
its client ID and optional secret in the connection form. A client secret also
requires its registered authorization-server issuer URL, so it cannot be sent to
a different issuer. Scopes, an existing
public client metadata document, and a registered callback URL can also be set
there. Account labels are names you choose, rather than verified profile names.

On the environment's own machine, the browser callback completes automatically.
In a remote browser, if the callback cannot reach the environment, copy the full
callback URL from the address bar into **Complete sign-in from a remote browser**.
Codes and state must match that project's pending sign-in, which expires after
ten minutes. The callback override supports an existing registered redirect URI;
it must reach this environment's plugin route or be completed using the paste
form. If an authorization server rejects account-selection prompts, turn off
**Ask the provider to choose an account**.

Refresh tokens, client secrets and grants stay in T3's secret store, separately
for each project definition. Providers receive a private environment-local MCP
route and do not manage these OAuth accounts in their own global auth caches.
Refreshing, switching or disconnecting Project A's connection does not change
Project B's connection. **Disconnect this account** stops using that local grant
and cancels its active requests; it does not sign the account out globally at the
authorization server. Start a new provider session after connecting or switching
accounts so its MCP route is refreshed.

Managed OAuth requires HTTPS (loopback HTTP is allowed for development), OAuth
protected-resource metadata, matching issuer metadata and S256 PKCE support.
It currently supports Streamable HTTP; stdio servers keep their own authentication,
and legacy SSE can use static headers or an external bridge. MCP Apps approval
management stays with T3 and its providers.

Version 0.2 reads existing 0.1 settings and credentials. Once settings are saved
with 0.2, older 0.1 builds cannot read the updated settings format.

## Provider support

| Provider                                     | Supported transports                         | Constraint                                                            |
| -------------------------------------------- | -------------------------------------------- | --------------------------------------------------------------------- |
| Claude                                       | stdio, Streamable HTTP, SSE                  | Native Agent SDK session configuration                                |
| Codex                                        | stdio, Streamable HTTP                       | Per-thread configuration, never process-wide MCP arguments            |
| Cursor                                       | stdio, Streamable HTTP, SSE                  | Native SDK configuration                                              |
| OpenCode                                     | stdio, HTTP/SSE through its remote client    | Requires a T3-managed server; an existing external server is rejected |
| Grok, Antigravity, Kiro, fx and ACP adapters | stdio; HTTP/SSE when advertised by the agent | Unsupported transports fail instead of being silently dropped         |
| Muse                                         | stdio, Streamable HTTP                       | Requires native `sessionMcp` support                                  |
| Pi                                           | Not yet supported                            | T3's Pi bridge currently exposes only its built-in tools              |

The manager supplies namespaced servers such as `mcp-manager-database`. It does
not bypass provider approvals or grant managed tools the permissions of T3's
built-in tools. Automatic MCP package installation and MCP Apps permission management remain
outside the manager.

Plugins are trusted extensions running in supervised processes. Crash isolation
does not make plugin code or the MCP commands it configures a hostile-code sandbox.

## Development

Node 24.10 or newer and pnpm 12.14 are required.

```sh
pnpm install --frozen-lockfile
pnpm test
pnpm run typecheck
pnpm run build
pnpm run verify
```

`Taskfile.yml` also exposes `verify`, and pinned `svu`/`chglog` release helpers.
The small vendored T3 plugin SDK is MIT-licensed; see
[`vendor/plugin-sdk/NOTICE`](vendor/plugin-sdk/NOTICE) and
[`THIRD_PARTY_LICENSES`](THIRD_PARTY_LICENSES).

The focused suite covers scope precedence, worktrees, provider/instance switches,
secret redaction, updates/removal, reload, serialized writes and failed atomic
replacement. OAuth fixtures cover separate accounts at one URL, refresh rotation,
concurrent refreshes, callback binding, remote completion, reconnect/disconnect,
registered client secrets, restart persistence and the settings upgrade. CI rebuilds the installable bundle and checks that it matches the
committed artifact. Provider protocol fixtures and worker integration are checked
in the host branch; native paid-provider sessions are not implied by those tests.

Repository policy uses squash-only merging, linear history and required checks.
GitHub does not provide GitLab's literal fast-forward-only merge mode. Conventional
Commits are checked by CI; initial pushes are not rejected by a metadata regex.
