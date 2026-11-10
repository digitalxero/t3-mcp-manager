import { test } from "node:test";
import assert from "node:assert/strict";
import * as Fs from "node:fs/promises";
import * as Path from "node:path";
import * as Os from "node:os";
import { createServer } from "node:http";
import { randomUUID, createHash } from "node:crypto";
import * as Effect from "effect/Effect";
import type { PluginContext } from "@t3tools/plugin-sdk";
import { createStore } from "../src/store.ts";
import { createOAuth } from "../src/oauth.ts";

async function fixture() {
  const root = await Fs.mkdtemp(Path.join(Os.tmpdir(), "t3-project-oauth-"));
  const secrets = new Map<string, string>();
  const clients = new Map<string, { redirect_uris: string[]; client_secret?: string }>();
  const codes = new Map<
    string,
    { client: string; account: string; challenge: string; redirect: string }
  >();
  const refreshes = new Map<string, { client: string; account: string }>();
  const access = new Map<string, string>();
  const generations = new Map<string, number>();
  const requests: Array<{ account: string; session: string | null; method: string }> = [];
  const tokenRequests: Array<{ grant: string; account: string; client: string }> = [];
  let clock = 0,
    origin = "",
    issuerOverride: string | undefined,
    redirectToken = false,
    redirectedTokens = 0;
  const issue = (client: string, account: string) => {
    const generation = (generations.get(account) ?? 0) + 1;
    generations.set(account, generation);
    const token = `access-${account}-${generation}`,
      refresh = `refresh-${account}-${generation}`;
    access.set(token, account);
    refreshes.set(refresh, { client, account });
    return {
      access_token: token,
      refresh_token: refresh,
      token_type: "Bearer",
      expires_in: 120,
      scope: "tools:read",
    };
  };
  const http = createServer(async (req, res) => {
    const json = (value: unknown, status = 200, headers: Record<string, string> = {}) =>
      res
        .writeHead(status, { "Content-Type": "application/json", ...headers })
        .end(JSON.stringify(value));
    try {
      const url = new URL(req.url!, origin);
      if (url.pathname.startsWith("/.well-known/oauth-protected-resource"))
        return json({
          resource: `${origin}/mcp`,
          authorization_servers: [origin],
          scopes_supported: ["tools:read"],
        });
      if (url.pathname === "/.well-known/oauth-authorization-server")
        return json({
          issuer: issuerOverride ?? origin,
          authorization_endpoint: `${origin}/authorize`,
          token_endpoint: `${origin}/token`,
          registration_endpoint: `${origin}/register`,
          response_types_supported: ["code"],
          grant_types_supported: ["authorization_code", "refresh_token"],
          code_challenge_methods_supported: ["S256"],
          token_endpoint_auth_methods_supported: ["none", "client_secret_post"],
          authorization_response_iss_parameter_supported: true,
        });
      const chunks: Buffer[] = [];
      for await (const chunk of req) chunks.push(Buffer.from(chunk));
      const body = Buffer.concat(chunks).toString();
      if (url.pathname === "/register") {
        const metadata = JSON.parse(body),
          client_id = `client-${clients.size + 1}`;
        clients.set(client_id, metadata);
        return json({ ...metadata, client_id });
      }
      if (url.pathname === "/authorize") {
        const client = url.searchParams.get("client_id")!,
          redirect = url.searchParams.get("redirect_uri")!,
          account = url.searchParams.get("account")!;
        assert.ok(clients.get(client)?.redirect_uris.includes(redirect));
        assert.equal(url.searchParams.get("code_challenge_method"), "S256");
        assert.equal(url.searchParams.get("resource"), `${origin}/mcp`);
        const code = randomUUID();
        codes.set(code, {
          client,
          account,
          challenge: url.searchParams.get("code_challenge")!,
          redirect,
        });
        const callback = new URL(redirect);
        callback.searchParams.set("state", url.searchParams.get("state")!);
        callback.searchParams.set("code", code);
        callback.searchParams.set("iss", origin);
        return res.writeHead(302, { Location: callback.href }).end();
      }
      if (url.pathname === "/redirected-token") {
        redirectedTokens++;
        return json(issue("manual-client", "work"));
      }
      if (url.pathname === "/token") {
        if (redirectToken)
          return res.writeHead(307, { Location: `${origin}/redirected-token` }).end();
        const params = new URLSearchParams(body),
          client = params.get("client_id")!,
          grant = params.get("grant_type")!;
        assert.equal(params.get("resource"), `${origin}/mcp`);
        const secret = clients.get(client)?.client_secret;
        if (secret) assert.equal(params.get("client_secret"), secret);
        if (grant === "authorization_code") {
          const code = params.get("code")!,
            saved = codes.get(code);
          assert.ok(saved);
          codes.delete(code);
          assert.equal(saved.client, client);
          assert.equal(saved.redirect, params.get("redirect_uri"));
          assert.equal(
            createHash("sha256").update(params.get("code_verifier")!).digest("base64url"),
            saved.challenge,
          );
          tokenRequests.push({ grant, account: saved.account, client });
          return json(issue(client, saved.account));
        }
        const refresh = params.get("refresh_token")!,
          saved = refreshes.get(refresh);
        assert.ok(saved);
        assert.equal(saved.client, client);
        refreshes.delete(refresh);
        tokenRequests.push({ grant, account: saved.account, client });
        return json(issue(client, saved.account));
      }
      if (url.pathname === "/mcp") {
        const account = access.get((req.headers.authorization ?? "").replace(/^Bearer /, ""));
        if (!account)
          return json({ error: "unauthorized" }, 401, {
            "WWW-Authenticate": `Bearer resource_metadata="${origin}/.well-known/oauth-protected-resource/mcp"`,
          });
        requests.push({
          account,
          session: req.headers["mcp-session-id"]?.toString() ?? null,
          method: req.method!,
        });
        if (req.method === "DELETE") return res.writeHead(204).end();
        return json({ account, result: "ok" }, 200, { "mcp-session-id": "same-upstream-session" });
      }
      json({ error: "not-found" }, 404);
    } catch {
      json({ error: "fixture-rejected-request" }, 400);
    }
  });
  await new Promise<void>((resolve) => http.listen(0, "127.0.0.1", resolve));
  origin = `http://127.0.0.1:${(http.address() as import("node:net").AddressInfo).port}`;
  const context: PluginContext = {
    id: "mcp-manager",
    dataDirectory: Path.join(root, "data"),
    capabilities: ["provider-mcp-servers", "http-response-timeout", "settings-pages"],
    httpBasePath: "/api/plugins/mcp-manager",
    httpBaseUrl: Effect.succeed(`${origin}/api/plugins/mcp-manager`),
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
  const store = await createStore(context),
    oauth = await createOAuth(context, store, { now: () => clock });
  const add = async (projectName: string) => {
    const project = Path.join(root, projectName);
    await Fs.mkdir(project);
    const saved = await store.save({
      name: "database",
      scope: "project",
      project,
      transport: "http",
      url: `${origin}/mcp`,
      auth: "oauth",
    });
    return { id: saved.id, project };
  };
  const authorize = async (id: string, account: string) => {
    const status = (await oauth.status()).find((status) => status.id === id)!;
    const url = new URL(status.authorizationUrl!);
    url.searchParams.set("account", account);
    const response = await fetch(url, { redirect: "manual" });
    assert.equal(response.status, 302);
    return response.headers.get("location")!;
  };
  const route = async (project: string) =>
    (
      await store.resolve(
        { provider: "codex", instanceId: "codex", projectDirectory: project },
        oauth.resolve,
      )
    )["mcp-manager-database"]!;
  const call = async (config: Awaited<ReturnType<typeof route>>, session?: string) => {
    assert.equal(config.transport, "http");
    if (config.transport !== "http") throw Error("Expected HTTP");
    return oauth.handleHttp(
      new Request(config.url, {
        method: "POST",
        headers: {
          ...config.headers,
          "Content-Type": "application/json",
          ...(session ? { "mcp-session-id": session } : {}),
        },
        body: '{"jsonrpc":"2.0","method":"initialize","id":1}',
      }),
    );
  };
  return {
    root,
    store,
    oauth,
    context,
    secrets,
    clients,
    requests,
    tokenRequests,
    access,
    origin,
    add,
    authorize,
    route,
    call,
    time(value: number) {
      clock = value;
    },
    redirectTokens(value: boolean) {
      redirectToken = value;
    },
    redirectedTokens() {
      return redirectedTokens;
    },
    issuer(value: string) {
      issuerOverride = value;
    },
    async [Symbol.asyncDispose]() {
      await oauth.close();
      http.closeAllConnections();
      await new Promise<void>((resolve) => http.close(() => resolve()));
      await Fs.rm(root, { recursive: true, force: true });
    },
  };
}

test("two projects use different OAuth accounts at the same MCP URL and cannot share transport sessions", async () => {
  await using f = await fixture();
  const a = await f.add("project-a"),
    b = await f.add("project-b");
  await f.oauth.start({ id: a.id, label: "Alice's account" });
  await f.oauth.start({ id: b.id, label: "Bob's account" });
  const pending = await f.oauth.status();
  assert.notEqual(
    new URL(pending[0]!.authorizationUrl!).searchParams.get("state"),
    new URL(pending[1]!.authorizationUrl!).searchParams.get("state"),
  );
  assert.equal(f.clients.size, 2);
  const aCallback = await f.authorize(a.id, "alice"),
    bCallback = await f.authorize(b.id, "bob");
  await assert.rejects(
    f.oauth.finish({ id: b.id, callback: aCallback }),
    /does not match this project/,
  );
  assert.equal(f.tokenRequests.length, 0);
  assert.equal((await f.oauth.handleHttp(new Request(aCallback))).status, 200);
  f.time(60_000);
  await f.oauth.finish({ id: b.id, callback: bCallback });
  await assert.rejects(
    f.oauth.finish({ id: b.id, callback: bCallback }),
    /expired or was cancelled/,
  );
  const aRoute = await f.route(a.project),
    bRoute = await f.route(b.project);
  assert.notDeepEqual(aRoute, bRoute);
  const aResponse = await f.call(aRoute),
    bResponse = await f.call(bRoute);
  assert.deepEqual(await aResponse.json(), { account: "alice", result: "ok" });
  assert.deepEqual(await bResponse.json(), { account: "bob", result: "ok" });
  const aSession = aResponse.headers.get("mcp-session-id")!,
    bSession = bResponse.headers.get("mcp-session-id")!;
  assert.notEqual(aSession, bSession);
  const count = f.requests.length;
  assert.equal((await f.call(bRoute, aSession)).status, 404);
  assert.equal(f.requests.length, count);
  f.time(61_000);
  const refreshed = await f.call(aRoute, aSession);
  assert.equal((await refreshed.json()).account, "alice");
  assert.deepEqual(
    f.tokenRequests
      .filter((request) => request.grant === "refresh_token")
      .map((request) => request.account),
    ["alice"],
  );
  assert.equal((await (await f.call(bRoute, bSession)).json()).account, "bob");
  const publicState = JSON.stringify(await f.oauth.status());
  const disk = await Fs.readFile(Path.join(f.context.dataDirectory, "servers.json"), "utf8");
  for (const value of ["access-alice", "access-bob", "refresh-alice", "refresh-bob"]) {
    assert.ok(!publicState.includes(value));
    assert.ok(!disk.includes(value));
  }
  await f.oauth.start({ id: a.id, label: "Carol's account" });
  await f.oauth.finish({ id: a.id, callback: await f.authorize(a.id, "carol") });
  assert.equal((await f.call(aRoute, aSession)).status, 403);
  assert.equal((await (await f.call(await f.route(a.project))).json()).account, "carol");
  assert.equal((await (await f.call(bRoute, bSession)).json()).account, "bob");
  await f.oauth.disconnect(a.id);
  assert.equal((await f.call(aRoute, aSession)).status, 403);
  assert.equal((await (await f.call(bRoute, bSession)).json()).account, "bob");
  await f.store.remove(b.id);
  await f.oauth.policyChanged(b.id);
  assert.equal((await f.call(bRoute, bSession)).status, 403);
  await f.store.remove(a.id);
  await f.oauth.policyChanged(a.id);
  assert.equal(f.secrets.size, 0);
});

test("simultaneous unauthorized requests refresh only the rejected project's grant once", async () => {
  await using f = await fixture();
  const a = await f.add("project-a");
  await f.oauth.start({ id: a.id });
  await f.oauth.finish({ id: a.id, callback: await f.authorize(a.id, "alice") });
  const config = await f.route(a.project);
  f.access.delete("access-alice-1");
  const responses = await Promise.all([f.call(config), f.call(config)]);
  assert.deepEqual(await Promise.all(responses.map((response) => response.json())), [
    { account: "alice", result: "ok" },
    { account: "alice", result: "ok" },
  ]);
  assert.equal(f.tokenRequests.filter((request) => request.grant === "refresh_token").length, 1);
});

test("issuer binding, callback expiry, cancellation and editing a project invalidate only its pending flow", async () => {
  await using f = await fixture();
  const a = await f.add("project-a"),
    b = await f.add("project-b");
  f.issuer("https://different-issuer.invalid");
  await assert.rejects(f.oauth.start({ id: a.id }), /issuer does not match/);
  assert.equal(f.clients.size, 0);
  f.issuer(f.origin);
  await f.oauth.start({ id: a.id });
  await f.oauth.start({ id: b.id });
  const aCallback = await f.authorize(a.id, "alice"),
    bCallback = await f.authorize(b.id, "bob");
  const wrongIssuer = new URL(aCallback);
  wrongIssuer.searchParams.set("iss", "https://different.invalid");
  await assert.rejects(
    f.oauth.finish({ id: a.id, callback: wrongIssuer.href }),
    /issuer does not match/,
  );
  await f.store.save({
    name: "database",
    scope: "project",
    project: a.project,
    transport: "http",
    url: `${f.origin}/mcp`,
    auth: "oauth",
    label: "Edited",
  });
  await assert.rejects(f.oauth.finish({ id: a.id, callback: aCallback }), /Server changed/);
  await f.oauth.cancel(a.id);
  await f.oauth.finish({ id: b.id, callback: bCallback });
  assert.equal((await f.oauth.status()).find((status) => status.id === b.id)!.connected, true);
  await f.oauth.start({ id: a.id });
  const expired = await f.authorize(a.id, "alice");
  f.time(10 * 60_000);
  await assert.rejects(f.oauth.finish({ id: a.id, callback: expired }), /expired or was cancelled/);
  const malformed = new URL(`${f.origin}/api/plugins/mcp-manager/oauth/callback`);
  malformed.searchParams.set("state", "é".repeat(43));
  assert.equal((await f.oauth.handleHttp(new Request(malformed))).status, 400);
});

test("pre-registered OAuth clients preserve opaque secrets and accept a remote-browser callback", async () => {
  await using f = await fixture();
  const a = await f.add("project-a");
  const redirect = `${f.origin}/registered-callback`;
  f.clients.set("manual-client", {
    redirect_uris: [redirect],
    client_secret: "  opaque-client-secret  ",
  });
  await assert.rejects(
    f.oauth.start({
      id: a.id,
      "client-id": "manual-client",
      "client-secret": "  opaque-client-secret  ",
      issuer: "https://different-issuer.invalid",
    }),
    /issuer does not match/,
  );
  assert.equal(f.tokenRequests.length, 0);
  await f.oauth.start({
    id: a.id,
    "client-id": "manual-client",
    issuer: f.origin,
    "client-secret": "  opaque-client-secret  ",
    "redirect-url": redirect,
    "choose-account": false,
  });
  const status = (await f.oauth.status())[0]!;
  assert.equal(new URL(status.authorizationUrl!).searchParams.has("prompt"), false);
  const callback = await f.authorize(a.id, "work");
  f.redirectTokens(true);
  await assert.rejects(f.oauth.finish({ id: a.id, callback }));
  assert.equal(f.redirectedTokens(), 0);
  f.redirectTokens(false);
  await f.oauth.start({
    id: a.id,
    "client-id": "manual-client",
    issuer: f.origin,
    "client-secret": "  opaque-client-secret  ",
    "redirect-url": redirect,
  });
  await f.oauth.finish({ id: a.id, callback: await f.authorize(a.id, "work") });
  assert.equal(f.clients.size, 1);
  assert.equal((await (await f.call(await f.route(a.project))).json()).account, "work");
});

test("OAuth grants survive a manager restart and a broken project grant can be cleared independently", async () => {
  await using f = await fixture();
  const a = await f.add("project-a"),
    b = await f.add("project-b");
  for (const [id, account] of [
    [a.id, "alice"],
    [b.id, "bob"],
  ] as const) {
    await f.oauth.start({ id });
    await f.oauth.finish({ id, callback: await f.authorize(id, account) });
  }
  const reloaded = await createOAuth(f.context, await createStore(f.context));
  try {
    assert.equal((await reloaded.status()).filter((account) => account.connected).length, 2);
    const route = await reloaded.resolve((await f.store.get(b.id))!);
    assert.equal(route.transport, "http");
    if (route.transport !== "http") throw Error("Expected HTTP");
    const response = await reloaded.handleHttp(
      new Request(route.url, {
        method: "POST",
        headers: route.headers,
        body: '{"jsonrpc":"2.0","id":1,"method":"initialize"}',
      }),
    );
    assert.equal((await response.json()).account, "bob");
    f.secrets.set(`oauth-${a.id.replaceAll("-", "")}`, "broken");
    const statuses = await reloaded.status();
    assert.equal(statuses.find((account) => account.id === a.id)!.problem, true);
    assert.equal(statuses.find((account) => account.id === b.id)!.connected, true);
    await reloaded.disconnect(a.id);
    assert.equal((await reloaded.status()).find((account) => account.id === a.id)!.problem, false);
  } finally {
    await reloaded.close();
  }
});
