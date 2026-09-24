import * as NodeAssert from "node:assert/strict";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { HttpClient, HttpClientResponse } from "effect/unstable/http";
import * as Schema from "effect/Schema";
import { beforeEach } from "vite-plus/test";

import {
  OpenCodeV2Settings,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import { ServerConfig } from "../../config.ts";
import { OpenCodeV2Runtime, OpenCodeV2RuntimeError } from "../opencodeV2Runtime.ts";
import type { OpenCodeV2RuntimeShape } from "../opencodeV2Runtime.ts";
import type { ProviderAdapterShape } from "../Services/ProviderAdapter.ts";
import type { ProviderAdapterError } from "../Errors.ts";
import { isOpenCodeV2NotFound, makeOpenCodeV2Adapter } from "./OpenCodeV2Adapter.ts";

class V2Adapter extends Context.Service<V2Adapter, ProviderAdapterShape<ProviderAdapterError>>()(
  "t3/provider/Layers/OpenCodeV2Adapter.resume.test/V2Adapter",
) {}

const asThreadId = (value: string): ThreadId => ThreadId.make(value);

const v2Mock = {
  state: {
    createCalls: 0,
    createdIds: [] as Array<string>,
    createDirectories: [] as Array<string>,
    getInfoIds: [] as Array<string>,
    updatePermissionsCalls: [] as Array<{ sessionId: string }>,
    interruptCalls: [] as Array<string>,
    deleteCalls: [] as Array<string>,
    sessionDirectoryById: new Map<string, string>(),
    missingIds: new Set<string>(),
    transientIds: new Set<string>(),
  },
  reset() {
    this.state.createCalls = 0;
    this.state.createdIds.length = 0;
    this.state.createDirectories.length = 0;
    this.state.getInfoIds.length = 0;
    this.state.updatePermissionsCalls.length = 0;
    this.state.interruptCalls.length = 0;
    this.state.deleteCalls.length = 0;
    this.state.sessionDirectoryById.clear();
    this.state.missingIds.clear();
    this.state.transientIds.clear();
  },
};

const notFoundError = (sessionId: string) =>
  new OpenCodeV2RuntimeError({
    operation: "session.get",
    detail: "HTTP 404 session.get",
    cause: { status: 404, body: { name: "NotFoundError", sessionId } },
  });

const transientError = () =>
  new OpenCodeV2RuntimeError({
    operation: "session.get",
    detail: "HTTP 500 session.get",
    cause: { status: 500 },
  });

const V2RuntimeTestDouble: OpenCodeV2RuntimeShape = {
  startServerProcess: () => Effect.die(new Error("not used")),
  connectToServer: () =>
    Effect.gen(function* () {
      yield* Effect.addFinalizer(() => Effect.void);
      return {
        url: "http://127.0.0.1:9999",
        version: "2.0.15",
        exitCode: null,
        external: true,
      };
    }),
  getInfo: () => Effect.succeed({ version: "2.0.15" }),
  listModels: () => Effect.succeed([]),
  listAgents: () => Effect.succeed([]),
  listProviders: () => Effect.succeed([]),
  getDefaultModel: () => Effect.succeed(undefined),
  createSession: (input) =>
    Effect.gen(function* () {
      v2Mock.state.createCalls += 1;
      v2Mock.state.createDirectories.push(input.directory);
      return { id: v2Mock.state.createdIds.shift() ?? "ses_new" };
    }),
  switchSessionModel: () => Effect.void,
  switchSessionAgent: () => Effect.void,
  promptSession: () => Effect.succeed({ messageId: "msg_1" }),
  replyPermission: () => Effect.void,
  replyForm: () => Effect.void,
  runSessionCommand: () => Effect.die(new Error("not used")),
  cancelForm: () => Effect.void,
  compactSession: () => Effect.succeed({ messageId: "msg_compact" }),
  listSkills: () => Effect.succeed([]),
  listCommands: () => Effect.succeed([]),
  interruptSession: ({ sessionId }) =>
    Effect.gen(function* () {
      v2Mock.state.interruptCalls.push(sessionId);
    }),
  deleteSession: ({ sessionId }) =>
    Effect.gen(function* () {
      v2Mock.state.deleteCalls.push(sessionId);
    }),
  forkSession: ({ sessionId }) => Effect.succeed({ id: `${sessionId}_fork` }),
  getSessionInfo: ({ sessionId }) =>
    Effect.gen(function* () {
      v2Mock.state.getInfoIds.push(sessionId);
      if (v2Mock.state.transientIds.has(sessionId)) return yield* transientError();
      if (v2Mock.state.missingIds.has(sessionId)) return yield* notFoundError(sessionId);
      const directory = v2Mock.state.sessionDirectoryById.get(sessionId);
      return { ...(directory === undefined ? {} : { directory }) };
    }),
  updateSessionPermissions: ({ sessionId }) =>
    Effect.gen(function* () {
      v2Mock.state.updatePermissionsCalls.push({ sessionId });
    }),
  listMessages: () => Effect.succeed([]),
  getMessage: () => Effect.die(new Error("not used")),
};

const TestHttpClientLive = Layer.succeed(
  HttpClient.HttpClient,
  HttpClient.make((request) =>
    Effect.succeed(HttpClientResponse.fromWeb(request, Response.json({ version: "2.0.15" }))),
  ),
);

const v2Settings = Schema.decodeSync(OpenCodeV2Settings)({
  binaryPath: "fake-opencode",
  serverUrl: "http://127.0.0.1:9999",
});

const V2AdapterTestLayer = Layer.effect(
  V2Adapter,
  makeOpenCodeV2Adapter(v2Settings, {
    instanceId: ProviderInstanceId.make("opencodeV2"),
  }),
).pipe(
  Layer.provideMerge(Layer.succeed(OpenCodeV2Runtime, V2RuntimeTestDouble)),
  Layer.provideMerge(ServerConfig.layerTest(process.cwd(), process.cwd())),
  Layer.provideMerge(TestHttpClientLive),
  Layer.provideMerge(NodeServices.layer),
);

beforeEach(() => {
  v2Mock.reset();
});

it.layer(V2AdapterTestLayer)("OpenCodeV2AdapterResume", (it) => {
  it.effect("returns a durable resume cursor for a freshly created session", () =>
    Effect.gen(function* () {
      const adapter = yield* V2Adapter;
      const session = yield* adapter.startSession({
        provider: ProviderDriverKind.make("opencodeV2"),
        threadId: asThreadId("thread-v2-cursor"),
        runtimeMode: "full-access",
      });

      NodeAssert.deepEqual(v2Mock.state.getInfoIds, []);
      NodeAssert.equal(v2Mock.state.createCalls, 1);
      NodeAssert.deepEqual(session.resumeCursor, { schemaVersion: 1, sessionId: "ses_new" });

      yield* adapter.stopSession(asThreadId("thread-v2-cursor"));
    }),
  );

  it.effect("resumes the persisted session instead of creating a new one", () =>
    Effect.gen(function* () {
      const adapter = yield* V2Adapter;
      const threadId = asThreadId("thread-v2-resume");
      const session = yield* adapter.startSession({
        provider: ProviderDriverKind.make("opencodeV2"),
        threadId,
        runtimeMode: "full-access",
        resumeCursor: { schemaVersion: 1, sessionId: "ses_persisted" },
      });

      NodeAssert.deepEqual(v2Mock.state.getInfoIds, ["ses_persisted"]);
      NodeAssert.equal(v2Mock.state.createCalls, 0);
      NodeAssert.deepEqual(session.resumeCursor, {
        schemaVersion: 1,
        sessionId: "ses_persisted",
      });
      NodeAssert.equal(v2Mock.state.updatePermissionsCalls.length, 1);
      NodeAssert.equal(v2Mock.state.updatePermissionsCalls[0]?.sessionId, "ses_persisted");

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("falls back to a fresh session when the persisted session is gone", () =>
    Effect.gen(function* () {
      const adapter = yield* V2Adapter;
      const threadId = asThreadId("thread-v2-stale");
      v2Mock.state.missingIds.add("ses_stale");
      v2Mock.state.createdIds.push("ses_fresh");

      const session = yield* adapter.startSession({
        provider: ProviderDriverKind.make("opencodeV2"),
        threadId,
        runtimeMode: "full-access",
        resumeCursor: { schemaVersion: 1, sessionId: "ses_stale" },
      });

      NodeAssert.deepEqual(v2Mock.state.getInfoIds, ["ses_stale"]);
      NodeAssert.equal(v2Mock.state.createCalls, 1);
      NodeAssert.deepEqual(session.resumeCursor, {
        schemaVersion: 1,
        sessionId: "ses_fresh",
      });

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("propagates transient resume failures instead of starting fresh", () =>
    Effect.gen(function* () {
      const adapter = yield* V2Adapter;
      const threadId = asThreadId("thread-v2-transient");
      v2Mock.state.transientIds.add("ses_flaky");

      const exit = yield* Effect.exit(
        adapter.startSession({
          provider: ProviderDriverKind.make("opencodeV2"),
          threadId,
          runtimeMode: "full-access",
          resumeCursor: { schemaVersion: 1, sessionId: "ses_flaky" },
        }),
      );
      NodeAssert.equal(exit._tag, "Failure");
      NodeAssert.equal(v2Mock.state.createCalls, 0);
    }),
  );

  it.effect("ignores a wrong-version resume cursor", () =>
    Effect.gen(function* () {
      const adapter = yield* V2Adapter;
      const threadId = asThreadId("thread-v2-badcursor");
      const session = yield* adapter.startSession({
        provider: ProviderDriverKind.make("opencodeV2"),
        threadId,
        runtimeMode: "full-access",
        resumeCursor: { schemaVersion: 99, sessionId: "ses_persisted" },
      });

      NodeAssert.deepEqual(v2Mock.state.getInfoIds, []);
      NodeAssert.equal(v2Mock.state.createCalls, 1);
      NodeAssert.deepEqual(session.resumeCursor, { schemaVersion: 1, sessionId: "ses_new" });

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("reuses the session when its directory matches despite spelling differences", () =>
    Effect.gen(function* () {
      const adapter = yield* V2Adapter;
      const threadId = asThreadId("thread-v2-samedir");
      v2Mock.state.sessionDirectoryById.set("ses_spelled", `${process.cwd()}/`);

      const session = yield* adapter.startSession({
        provider: ProviderDriverKind.make("opencodeV2"),
        threadId,
        runtimeMode: "full-access",
        resumeCursor: { schemaVersion: 1, sessionId: "ses_spelled" },
      });

      NodeAssert.deepEqual(v2Mock.state.getInfoIds, ["ses_spelled"]);
      NodeAssert.equal(v2Mock.state.createCalls, 0);
      NodeAssert.deepEqual(session.resumeCursor, {
        schemaVersion: 1,
        sessionId: "ses_spelled",
      });

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("starts fresh in the right directory when the persisted session moved", () =>
    Effect.gen(function* () {
      const adapter = yield* V2Adapter;
      const threadId = asThreadId("thread-v2-moved");
      v2Mock.state.sessionDirectoryById.set("ses_old_dir", "/definitely/not/the/cwd");
      v2Mock.state.createdIds.push("ses_fresh_dir");

      const session = yield* adapter.startSession({
        provider: ProviderDriverKind.make("opencodeV2"),
        threadId,
        runtimeMode: "full-access",
        resumeCursor: { schemaVersion: 1, sessionId: "ses_old_dir" },
      });

      // Probed the stale-directory id, refused to adopt it (the v2 fork
      // endpoint cannot retarget directories), and minted a fresh session
      // bound to the requested cwd instead of running in the wrong folder.
      NodeAssert.deepEqual(v2Mock.state.getInfoIds, ["ses_old_dir"]);
      NodeAssert.equal(v2Mock.state.createCalls, 1);
      NodeAssert.deepEqual(v2Mock.state.createDirectories, [process.cwd()]);
      NodeAssert.deepEqual(session.resumeCursor, {
        schemaVersion: 1,
        sessionId: "ses_fresh_dir",
      });
      NodeAssert.equal(session.cwd, process.cwd());

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("stopSession keeps the native session so a later resume re-adopts it", () =>
    Effect.gen(function* () {
      const adapter = yield* V2Adapter;
      const threadId = asThreadId("thread-v2-idle");
      const first = yield* adapter.startSession({
        provider: ProviderDriverKind.make("opencodeV2"),
        threadId,
        runtimeMode: "full-access",
      });
      const nativeId = (first.resumeCursor as { sessionId: string }).sessionId;

      // The reaper stops idle sessions. That must halt local handles without
      // destroying the server-side session the cursor points at — otherwise
      // every 30-minute idle gap ends in permanent amnesia.
      yield* adapter.stopSession(threadId);
      NodeAssert.deepEqual(v2Mock.state.deleteCalls, []);
      NodeAssert.deepEqual(v2Mock.state.interruptCalls, [nativeId]);

      const second = yield* adapter.startSession({
        provider: ProviderDriverKind.make("opencodeV2"),
        threadId,
        runtimeMode: "full-access",
        resumeCursor: first.resumeCursor,
      });
      NodeAssert.deepEqual(v2Mock.state.getInfoIds, [nativeId]);
      NodeAssert.equal(v2Mock.state.createCalls, 1);
      NodeAssert.deepEqual(second.resumeCursor, first.resumeCursor);

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("isOpenCodeV2NotFound only trusts confirmed misses", () =>
    Effect.sync(() => {
      NodeAssert.equal(
        isOpenCodeV2NotFound({ status: 404, body: { name: "NotFoundError" } }),
        true,
      );
      NodeAssert.equal(
        isOpenCodeV2NotFound(
          new OpenCodeV2RuntimeError({
            operation: "session.get",
            detail: "HTTP 404 session.get",
            cause: { status: 404 },
          }),
        ),
        true,
      );
      NodeAssert.equal(isOpenCodeV2NotFound({ status: 500 }), false);
      NodeAssert.equal(isOpenCodeV2NotFound(new Error("boom")), false);
      // An explicit non-404 seals the subtree even when a nested name looks like a miss.
      NodeAssert.equal(
        isOpenCodeV2NotFound({ status: 500, cause: { name: "NotFoundError" } }),
        false,
      );
    }),
  );
});
