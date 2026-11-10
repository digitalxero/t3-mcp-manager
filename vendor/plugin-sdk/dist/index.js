import * as Schema from "effect/Schema";
export const PLUGIN_API_VERSION = 1;
export const PluginId = Schema.String.check(Schema.isPattern(/^[a-z][a-z0-9-]{0,63}$/));
export const PluginLocalId = Schema.String.check(Schema.isPattern(/^[a-z][a-z0-9-]{0,63}$/));
const Label = Schema.String.check(Schema.isMaxLength(200));
const Text = Schema.String.check(Schema.isMaxLength(16_384));
export const PluginInput = Schema.Record(Schema.String, Schema.Json);
export const PluginPermission = Schema.Literals([
    "orchestration:read",
    "orchestration:operate",
    "settings:write",
    "providers:manage",
    "environment:maintain",
    "diagnostics:read",
    "filesystem:read",
    "filesystem:write",
    "terminal:read",
    "terminal:operate",
    "source-control:write",
    "review:write",
    "access:read",
    "access:write",
    "relay:read",
    "relay:write",
    "preview:operate",
]);
export const PluginManifest = Schema.Struct({
    apiVersion: Schema.Literal(PLUGIN_API_VERSION),
    id: PluginId,
    name: Label,
    version: Label,
    description: Schema.optionalKey(Text),
});
export const PluginAction = Schema.Struct({
    id: PluginLocalId,
    label: Label,
    command: PluginLocalId,
    input: PluginInput,
});
export const PluginField = Schema.Struct({
    name: PluginLocalId,
    label: Label,
    kind: Schema.Literals(["text", "password", "number", "boolean", "select"]),
    value: Schema.optionalKey(Schema.Union([Schema.String, Schema.Number, Schema.Boolean])),
    options: Schema.optionalKey(Schema.Array(Schema.Struct({ label: Label, value: Label })).check(Schema.isMaxLength(100))),
    required: Schema.optionalKey(Schema.Boolean),
});
export const PluginBlock = Schema.Union([
    Schema.Struct({ id: PluginLocalId, kind: Schema.Literal("text"), text: Text }),
    Schema.Struct({ id: PluginLocalId, kind: Schema.Literal("link"), label: Label, url: Text }),
    Schema.Struct({
        id: PluginLocalId,
        kind: Schema.Literal("actions"),
        actions: Schema.Array(PluginAction).check(Schema.isMaxLength(20)),
    }),
    Schema.Struct({
        id: PluginLocalId,
        kind: Schema.Literal("form"),
        label: Label,
        command: PluginLocalId,
        fields: Schema.Array(PluginField).check(Schema.isMaxLength(40)),
        input: PluginInput,
    }),
    Schema.Struct({
        id: PluginLocalId,
        kind: Schema.Literal("table"),
        columns: Schema.Array(Label).check(Schema.isMaxLength(20)),
        rows: Schema.Array(Schema.Struct({
            id: Label,
            cells: Schema.Array(Text).check(Schema.isMaxLength(20)),
            actions: Schema.Array(PluginAction).check(Schema.isMaxLength(20)),
        })).check(Schema.isMaxLength(500)),
    }),
]);
const unique = (values) => new Set(values).size === values.length;
export const PluginView = Schema.Struct({
    title: Label,
    blocks: Schema.Array(PluginBlock).check(Schema.isMaxLength(100)),
}).check(Schema.makeFilter((view) => unique(view.blocks.map((block) => block.id)) &&
    view.blocks.every((block) => {
        if (block.kind === "form")
            return (unique(block.fields.map((field) => field.name)) &&
                block.fields.every((field) => unique(field.options?.map((option) => option.value) ?? [])));
        if (block.kind === "actions")
            return unique(block.actions.map((action) => action.id));
        if (block.kind === "table")
            return (unique(block.columns) &&
                unique(block.rows.map((row) => row.id)) &&
                block.rows.every((row) => row.cells.length === block.columns.length &&
                    unique(row.actions.map((action) => action.id))));
        return true;
    })));
export const PluginViewDescriptor = Schema.Struct({
    id: PluginLocalId,
    title: Label,
    location: Schema.Literals(["settings", "settings-page", "usage"]),
    permission: PluginPermission,
});
export const PluginCommandDescriptor = Schema.Struct({
    id: PluginLocalId,
    title: Label,
    description: Text,
    permission: PluginPermission,
    /** Agent access is opt-in; secret-bearing commands should leave this false. */
    agent: Schema.Boolean,
});
/** An explicit command may disclose a secret temporarily; never return this from a view. */
export const PluginSecretResult = Schema.Struct({
    $t3: Schema.Literal("secret"),
    label: Label,
    value: Text,
});
export class PluginError extends Schema.TaggedError()("PluginError", {
    operation: Schema.String,
    cause: Schema.Defect(),
}) {
    get message() {
        return `Plugin operation failed (${this.operation}).`;
    }
}
export const PluginMcpServer = Schema.Union([
    Schema.Struct({
        transport: Schema.Literal("stdio"),
        command: Text,
        args: Schema.Array(Text).check(Schema.isMaxLength(100)),
        environment: Schema.Record(Label, Text),
    }),
    Schema.Struct({
        transport: Schema.Literals(["http", "sse"]),
        url: Text,
        headers: Schema.Record(Label, Text),
    }),
]);
export const PluginMcpServers = Schema.Record(PluginLocalId, PluginMcpServer).check(Schema.makeFilter((servers) => Object.keys(servers).length <= 100));
export const PluginMcpContext = Schema.Struct({
    provider: Label,
    instanceId: Label,
    projectDirectory: Schema.optionalKey(Text),
    cwd: Schema.optionalKey(Text),
});
export const PluginProcessInput = Schema.Struct({
    command: Text,
    args: Schema.Array(Text).check(Schema.isMaxLength(100)),
    cwd: Text,
    environment: Schema.Record(Label, Text),
    timeoutMs: Schema.Int.check(Schema.isBetween({ minimum: 1_000, maximum: 1_500_000 })),
});
export const PluginProcessResult = Schema.Struct({
    code: Schema.NullOr(Schema.Int),
    timedOut: Schema.Boolean,
    stdout: Text,
    stderr: Text,
    stdoutTruncated: Schema.Boolean,
    stderrTruncated: Schema.Boolean,
});
export const PluginHttpResponseTimeout = Schema.Int.check(Schema.isBetween({ minimum: 1_000, maximum: 3_900_000 }));
export const PluginLifecycleEvent = Schema.Struct({
    type: Schema.Literals(["worktree.created", "worktree.removing", "worktree.removed"]),
    repositoryDirectory: Text,
    worktreePath: Text,
    refName: Schema.optionalKey(Text),
});
export const PluginLifecycleResult = Schema.NullOr(Schema.Struct({ error: Text }));
export class PluginLifecycleBlocked extends Schema.TaggedError()("PluginLifecycleBlocked", { pluginId: PluginId, message: Text }) {
}
export const PluginLifecycleTimeout = Schema.Int.check(Schema.isBetween({ minimum: 1_000, maximum: 1_800_000 }));
