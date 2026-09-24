/**
 * OpenCodeV2Runtime — HTTP client + process manager for the OpenCode v2
 * background service.
 *
 * Independent tool from OpenCode v1 (`opencodeRuntime.ts`). The v2 server
 * speaks `/api/*` with `{data, location}` envelopes, `ses_*`/`msg_*` ids,
 * and `session.*` SSE events. `opencode serve` prints its URL and generated
 * password on stdout:
 *
 *   server listening on http://127.0.0.1:PORT
 *   server password <password>
 *
 * Auth is HTTP Basic `opencode:<password>` (verified against v2.0.15).
 * Transport uses `HttpClient`, following `DeviceService.hubJson`.
 *
 * Message listing pages newest-first capped at 200 per call, so the
 * runtime follows cursors (up to 1000 messages) and returns chronological
 * order — a single default call would silently truncate long sessions.
 *
 * @module provider/opencodeV2Runtime
 */
import * as Context from "effect/Context";
import * as Data from "effect/Data";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import { HttpClient, HttpClientRequest, HttpClientResponse } from "effect/unstable/http";

import { spawnAndCollect } from "./providerSnapshot.ts";
import * as NetService from "@t3tools/shared/Net";
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import { compareSemverVersions, parseSemver } from "@t3tools/shared/semver";
import { resolveSpawnCommand } from "@t3tools/shared/shell";

export const MINIMUM_OPENCODE_V2_VERSION = "2.0.13";
const DEFAULT_HOSTNAME = "127.0.0.1";
const SERVER_STARTUP_TIMEOUT_MS = 30_000;
const SERVER_STARTUP_MAX_OUTPUT_CHARS = 64 * 1024;
const REQUEST_TIMEOUT_MS = 30_000;

const SERVER_READY_URL_RE = /server listening on\s+(https?:\/\/[^\s]+)/;
const SERVER_PASSWORD_RE = /^server password\s+(\S+)\s*$/m;

export interface OpenCodeV2ServerProcess {
  readonly url: string;
  readonly serverPassword?: string;
  readonly version: string;
  readonly isRunning: Effect.Effect<boolean>;
  readonly exitCode: Effect.Effect<number, never>;
}

export interface OpenCodeV2ServerConnection {
  readonly url: string;
  readonly serverPassword?: string;
  readonly version: string;
  readonly exitCode: Effect.Effect<number, never> | null;
  readonly external: boolean;
}

const OPENCODE_V2_RUNTIME_ERROR_TAG = "OpenCodeV2RuntimeError";
export class OpenCodeV2RuntimeError extends Data.TaggedError(OPENCODE_V2_RUNTIME_ERROR_TAG)<{
  readonly operation: string;
  readonly cause?: unknown;
  readonly detail: string;
}> {}

export const isOpenCodeV2RuntimeError = (u: unknown): u is OpenCodeV2RuntimeError =>
  typeof u === "object" &&
  u !== null &&
  "_tag" in u &&
  (u as { _tag: unknown })._tag === OPENCODE_V2_RUNTIME_ERROR_TAG;

export function openCodeV2RuntimeErrorDetail(cause: unknown): string {
  if (isOpenCodeV2RuntimeError(cause)) return cause.detail;
  if (cause instanceof Error && cause.message.trim().length > 0) return cause.message.trim();
  return String(cause);
}

/** @internal Parse `opencode serve` stdout for URL + generated password. */
export function parseOpenCodeV2ServerOutput(output: string): {
  readonly url: string | null;
  readonly password: string | undefined;
} {
  const urlMatch = SERVER_READY_URL_RE.exec(output);
  const passwordMatch = SERVER_PASSWORD_RE.exec(output);
  return {
    url: urlMatch?.[1] ?? null,
    password: passwordMatch?.[1],
  };
}

const ServiceRegistrationResponse = Schema.Struct({
  url: Schema.String,
  password: Schema.optional(Schema.String),
});
const decodeServiceRegistrationFile = Schema.decodeUnknownEffect(
  Schema.fromJsonString(ServiceRegistrationResponse),
);

/**
 * Parse the shared background service registration (`service.json`).
 * Exported for unit testing.
 */
export function parseOpenCodeV2ServiceRegistration(
  raw: string,
): { readonly url: string; readonly password: string | undefined } | undefined {
  const decoded = decodeServiceRegistrationFile(raw).pipe(Effect.orElseSucceed(() => undefined));
  const result = Effect.runSync(decoded);
  if (result === undefined) return undefined;
  const url = result.url.trim();
  if (!url) return undefined;
  const password = result.password?.trim();
  return { url, password: password ? password : undefined };
}

export interface OpenCodeV2ModelInfo {
  readonly id: string;
  readonly providerID: string;
  readonly modelID: string;
  readonly name: string;
  readonly variants: ReadonlyArray<string>;
}

export interface OpenCodeV2AgentInfo {
  readonly id: string;
  readonly mode: string;
}

export interface OpenCodeV2SessionInfo {
  readonly id: string;
  readonly title?: string;
}

export interface OpenCodeV2MessageInfo {
  readonly id: string;
  readonly type: string;
  readonly text: string;
}

export interface OpenCodeV2PermissionRule {
  readonly action: string;
  readonly resource: string;
  readonly effect: "allow" | "deny" | "ask";
}

export interface OpenCodeV2PromptFile {
  readonly uri: string;
  readonly name?: string;
}

export interface OpenCodeV2SkillInfo {
  readonly name: string;
  readonly description?: string;
  readonly path: string;
}

export interface OpenCodeV2CommandInfo {
  readonly name: string;
  readonly description?: string;
}

const ServerInfoResponse = Schema.Struct({
  version: Schema.String,
});
const decodeServerInfoResponse = Schema.decodeUnknownEffect(ServerInfoResponse);

const DataEnvelope = Schema.Struct({
  data: Schema.Unknown,
});
const decodeDataEnvelope = Schema.decodeUnknownEffect(DataEnvelope);

function unwrapData(payload: unknown): unknown {
  if (payload !== null && typeof payload === "object" && "data" in payload) {
    return (payload as { data: unknown }).data;
  }
  return payload;
}

export interface OpenCodeV2RuntimeShape {
  readonly startServerProcess: (input: {
    readonly binaryPath: string;
    readonly directory: string;
    readonly serverPassword?: string;
    readonly environment?: NodeJS.ProcessEnv;
    readonly port?: number;
    readonly hostname?: string;
    readonly timeoutMs?: number;
  }) => Effect.Effect<OpenCodeV2ServerProcess, OpenCodeV2RuntimeError, Scope.Scope>;
  readonly connectToServer: (input: {
    readonly binaryPath: string;
    readonly directory: string;
    readonly serverUrl?: string | null;
    readonly serverPassword?: string;
    readonly environment?: NodeJS.ProcessEnv;
    readonly port?: number;
    readonly hostname?: string;
    readonly timeoutMs?: number;
  }) => Effect.Effect<OpenCodeV2ServerConnection, OpenCodeV2RuntimeError, Scope.Scope>;
  readonly getInfo: (input: {
    readonly baseUrl: string;
    readonly serverPassword?: string;
  }) => Effect.Effect<{ readonly version: string }, OpenCodeV2RuntimeError>;
  readonly listModels: (input: {
    readonly baseUrl: string;
    readonly serverPassword?: string;
  }) => Effect.Effect<ReadonlyArray<OpenCodeV2ModelInfo>, OpenCodeV2RuntimeError>;
  readonly listAgents: (input: {
    readonly baseUrl: string;
    readonly serverPassword?: string;
  }) => Effect.Effect<ReadonlyArray<OpenCodeV2AgentInfo>, OpenCodeV2RuntimeError>;
  readonly listProviders: (input: {
    readonly baseUrl: string;
    readonly serverPassword?: string;
  }) => Effect.Effect<ReadonlyArray<string>, OpenCodeV2RuntimeError>;
  readonly getDefaultModel: (input: {
    readonly baseUrl: string;
    readonly serverPassword?: string;
    readonly directory?: string;
  }) => Effect.Effect<string | undefined, OpenCodeV2RuntimeError>;
  readonly createSession: (input: {
    readonly baseUrl: string;
    readonly serverPassword?: string;
    readonly directory: string;
    readonly title?: string;
    readonly modelSlug?: string;
    readonly variant?: string;
    readonly agent?: string;
    readonly permissions?: ReadonlyArray<OpenCodeV2PermissionRule>;
  }) => Effect.Effect<OpenCodeV2SessionInfo, OpenCodeV2RuntimeError>;
  readonly switchSessionModel: (input: {
    readonly baseUrl: string;
    readonly serverPassword?: string;
    readonly sessionId: string;
    readonly modelSlug: string;
    readonly variant?: string;
  }) => Effect.Effect<void, OpenCodeV2RuntimeError>;
  readonly switchSessionAgent: (input: {
    readonly baseUrl: string;
    readonly serverPassword?: string;
    readonly sessionId: string;
    readonly agent: string;
  }) => Effect.Effect<void, OpenCodeV2RuntimeError>;
  readonly promptSession: (input: {
    readonly baseUrl: string;
    readonly serverPassword?: string;
    readonly sessionId: string;
    readonly text: string;
    readonly files?: ReadonlyArray<OpenCodeV2PromptFile>;
  }) => Effect.Effect<{ readonly messageId: string }, OpenCodeV2RuntimeError>;
  readonly replyPermission: (input: {
    readonly baseUrl: string;
    readonly serverPassword?: string;
    readonly sessionId: string;
    readonly requestId: string;
    readonly decision: "once" | "always" | "reject";
  }) => Effect.Effect<void, OpenCodeV2RuntimeError>;
  readonly replyForm: (input: {
    readonly baseUrl: string;
    readonly serverPassword?: string;
    readonly sessionId: string;
    readonly formId: string;
    readonly answer: Record<string, string | number | boolean | ReadonlyArray<string>>;
  }) => Effect.Effect<void, OpenCodeV2RuntimeError>;
  readonly runSessionCommand: (input: {
    readonly baseUrl: string;
    readonly serverPassword?: string;
    readonly sessionId: string;
    readonly name: string;
    readonly text: string;
  }) => Effect.Effect<void, OpenCodeV2RuntimeError>;
  readonly cancelForm: (input: {
    readonly baseUrl: string;
    readonly serverPassword?: string;
    readonly sessionId: string;
    readonly formId: string;
  }) => Effect.Effect<void, OpenCodeV2RuntimeError>;
  readonly compactSession: (input: {
    readonly baseUrl: string;
    readonly serverPassword?: string;
    readonly sessionId: string;
  }) => Effect.Effect<{ readonly messageId: string }, OpenCodeV2RuntimeError>;
  readonly listSkills: (input: {
    readonly baseUrl: string;
    readonly serverPassword?: string;
    readonly directory?: string;
  }) => Effect.Effect<ReadonlyArray<OpenCodeV2SkillInfo>, OpenCodeV2RuntimeError>;
  readonly listCommands: (input: {
    readonly baseUrl: string;
    readonly serverPassword?: string;
  }) => Effect.Effect<ReadonlyArray<OpenCodeV2CommandInfo>, OpenCodeV2RuntimeError>;
  readonly interruptSession: (input: {
    readonly baseUrl: string;
    readonly serverPassword?: string;
    readonly sessionId: string;
  }) => Effect.Effect<void, OpenCodeV2RuntimeError>;
  readonly deleteSession: (input: {
    readonly baseUrl: string;
    readonly serverPassword?: string;
    readonly sessionId: string;
  }) => Effect.Effect<void, OpenCodeV2RuntimeError>;
  readonly forkSession: (input: {
    readonly baseUrl: string;
    readonly serverPassword?: string;
    readonly sessionId: string;
    readonly beforeMessageId?: string;
  }) => Effect.Effect<{ readonly id: string }, OpenCodeV2RuntimeError>;
  readonly getSessionInfo: (input: {
    readonly baseUrl: string;
    readonly serverPassword?: string;
    readonly sessionId: string;
  }) => Effect.Effect<
    { readonly revertMessageId?: string; readonly directory?: string },
    OpenCodeV2RuntimeError
  >;
  readonly updateSessionPermissions: (input: {
    readonly baseUrl: string;
    readonly serverPassword?: string;
    readonly sessionId: string;
    readonly permissions: ReadonlyArray<OpenCodeV2PermissionRule>;
  }) => Effect.Effect<void, OpenCodeV2RuntimeError>;
  readonly listMessages: (input: {
    readonly baseUrl: string;
    readonly serverPassword?: string;
    readonly sessionId: string;
  }) => Effect.Effect<ReadonlyArray<OpenCodeV2MessageInfo>, OpenCodeV2RuntimeError>;
  readonly getMessage: (input: {
    readonly baseUrl: string;
    readonly serverPassword?: string;
    readonly sessionId: string;
    readonly messageId: string;
  }) => Effect.Effect<
    {
      readonly id: string;
      readonly type: string;
      readonly text: string;
      readonly finish?: string;
      readonly errorMessage?: string;
      readonly model?: { readonly id: string; readonly providerID: string };
    },
    OpenCodeV2RuntimeError
  >;
}

const basicAuthValue = (serverPassword: string): string =>
  `Basic ${Buffer.from(`opencode:${serverPassword}`, "utf8").toString("base64")}`;

const makeOpenCodeV2Runtime = Effect.gen(function* () {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const netService = yield* NetService.NetService;
  const hostPlatform = yield* HostProcessPlatform;
  const httpClient = yield* HttpClient.HttpClient;
  const fileSystem = yield* FileSystem.FileSystem;
  const pathService = yield* Path.Path;
  const resolveCommand = (command: string, args: ReadonlyArray<string>, env?: NodeJS.ProcessEnv) =>
    resolveSpawnCommand(command, args, env ? { env } : {});

  const fail = (operation: string, detail: string, cause?: unknown): OpenCodeV2RuntimeError =>
    new OpenCodeV2RuntimeError({ operation, detail, cause });

  const withTimeout = <A, E>(
    self: Effect.Effect<A, E>,
    operation: string,
  ): Effect.Effect<A, E | OpenCodeV2RuntimeError> =>
    self.pipe(
      Effect.timeoutOption(REQUEST_TIMEOUT_MS),
      Effect.flatMap((option) =>
        Option.isNone(option)
          ? Effect.fail(fail(operation, `Timed out during ${operation}.`))
          : Effect.succeed(option.value),
      ),
    );

  const executeJson = (
    operation: string,
    request: HttpClientRequest.HttpClientRequest,
  ): Effect.Effect<unknown, OpenCodeV2RuntimeError> =>
    Effect.gen(function* () {
      const response = yield* httpClient
        .execute(request)
        .pipe(
          Effect.mapError((cause) => fail(operation, openCodeV2RuntimeErrorDetail(cause), cause)),
        );
      yield* response.pipe(
        HttpClientResponse.filterStatusOk,
        Effect.mapError((cause) =>
          response.status === 401
            ? fail(
                operation,
                "OpenCode v2 server rejected the credentials (HTTP 401). The background service rotates its password on restart — clear Server URL and Server password to let T3 Code discover or spawn the server automatically.",
                cause,
              )
            : fail(operation, `HTTP ${response.status} ${operation}`, cause),
        ),
      );
      return yield* response.json.pipe(
        Effect.mapError((cause) => fail(operation, `Invalid JSON for ${operation}`, cause)),
        Effect.scoped,
      );
    });

  const withAuth = (
    request: HttpClientRequest.HttpClientRequest,
    serverPassword: string | undefined,
  ): HttpClientRequest.HttpClientRequest =>
    serverPassword === undefined
      ? request
      : HttpClientRequest.setHeader(request, "Authorization", basicAuthValue(serverPassword));

  const getInfo: OpenCodeV2RuntimeShape["getInfo"] = (input) =>
    Effect.gen(function* () {
      const request = withAuth(
        HttpClientRequest.get(`${input.baseUrl}/api/info`).pipe(
          HttpClientRequest.setHeader("accept", "application/json"),
        ),
        input.serverPassword,
      );
      const payload = yield* executeJson("server.info", request).pipe((self) =>
        withTimeout(self, "server.info"),
      );
      const info = yield* decodeServerInfoResponse(payload).pipe(
        Effect.mapError((cause) =>
          fail(
            "server.info",
            `OpenCode v2 server returned an invalid info response. Requires v${MINIMUM_OPENCODE_V2_VERSION} or newer.`,
            cause,
          ),
        ),
      );
      if (parseSemver(info.version) === null) {
        return yield* fail("server.info", "OpenCode v2 server returned an invalid version.");
      }
      if (compareSemverVersions(info.version, MINIMUM_OPENCODE_V2_VERSION) < 0) {
        return yield* fail(
          "server.info",
          `OpenCode v2 v${info.version} is too old. Upgrade to v${MINIMUM_OPENCODE_V2_VERSION} or newer.`,
        );
      }
      return { version: info.version };
    }).pipe(Effect.withSpan("opencodeV2.server.info"));

  const startServerProcess: OpenCodeV2RuntimeShape["startServerProcess"] = (input) =>
    Effect.gen(function* () {
      const runtimeScope = yield* Scope.Scope;
      const hostname = input.hostname ?? DEFAULT_HOSTNAME;
      const port =
        input.port ??
        (yield* netService
          .findAvailablePort(0)
          .pipe(
            Effect.mapError((cause) =>
              fail(
                "startServerProcess",
                `Failed to find available port: ${openCodeV2RuntimeErrorDetail(cause)}`,
                cause,
              ),
            ),
          ));
      const timeoutMs = input.timeoutMs ?? SERVER_STARTUP_TIMEOUT_MS;
      const spawnCommand = yield* resolveCommand(
        input.binaryPath,
        ["serve", `--hostname=${hostname}`, `--port=${port}`],
        input.environment,
      );
      const child = yield* spawner
        .spawn(
          ChildProcess.make(spawnCommand.command, spawnCommand.args, {
            detached: hostPlatform !== "win32",
            shell: spawnCommand.shell,
            env: { ...input.environment },
            extendEnv: input.environment === undefined,
          }),
        )
        .pipe(
          Effect.provideService(Scope.Scope, runtimeScope),
          Effect.mapError((cause) =>
            fail(
              "startServerProcess",
              `Failed to spawn OpenCode v2 server: ${openCodeV2RuntimeErrorDetail(cause)}`,
              cause,
            ),
          ),
        );

      const killProcessGroup = (signal: NodeJS.Signals) =>
        hostPlatform === "win32"
          ? child.kill({ killSignal: signal, forceKillAfter: "1 second" }).pipe(Effect.asVoid)
          : Effect.sync(() => {
              try {
                process.kill(-Number(child.pid), signal);
              } catch {
                // Already exited.
              }
            });
      const terminateChild = killProcessGroup("SIGTERM").pipe(
        Effect.andThen(Effect.sleep("1 second")),
        Effect.andThen(killProcessGroup("SIGKILL")),
        Effect.ignore,
      );
      yield* Scope.addFinalizer(runtimeScope, terminateChild);

      const stdoutRef = yield* Ref.make<string | null>("");
      const stderrRef = yield* Ref.make<string | null>("");
      const readyDeferred = yield* Deferred.make<
        { readonly url: string; readonly password: string | undefined },
        OpenCodeV2RuntimeError
      >();

      const setReadyFromChunk = (chunk: string) =>
        Ref.modify(stdoutRef, (stdout) => {
          if (stdout === null) return [null, null] as const;
          const next = `${stdout}${chunk}`.slice(-SERVER_STARTUP_MAX_OUTPUT_CHARS);
          const parsed = parseOpenCodeV2ServerOutput(next);
          return [
            parsed.url ? { url: parsed.url, password: parsed.password } : null,
            next,
          ] as const;
        }).pipe(
          Effect.flatMap((parsed) =>
            parsed ? Deferred.succeed(readyDeferred, parsed).pipe(Effect.ignore) : Effect.void,
          ),
        );

      const stdoutFiber = yield* child.stdout.pipe(
        Stream.decodeText(),
        Stream.runForEach(setReadyFromChunk),
        Effect.ignore,
        Effect.forkIn(runtimeScope),
      );
      const stderrFiber = yield* child.stderr.pipe(
        Stream.decodeText(),
        Stream.runForEach((chunk) =>
          Ref.update(stderrRef, (stderr) =>
            stderr === null ? null : `${stderr}${chunk}`.slice(-SERVER_STARTUP_MAX_OUTPUT_CHARS),
          ),
        ),
        Effect.ignore,
        Effect.forkIn(runtimeScope),
      );
      const exitFiber = yield* child.exitCode.pipe(
        Effect.flatMap((code) =>
          Effect.gen(function* () {
            const stdout = (yield* Ref.get(stdoutRef)) ?? "";
            const stderr = (yield* Ref.get(stderrRef)) ?? "";
            yield* Deferred.fail(
              readyDeferred,
              fail(
                "startServerProcess",
                [
                  `OpenCode v2 server exited before startup (code: ${String(Number(code))}).`,
                  stdout.trim() ? `stdout:\n${stdout.trim()}` : null,
                  stderr.trim() ? `stderr:\n${stderr.trim()}` : null,
                ]
                  .filter((part): part is string => part !== null)
                  .join("\n\n"),
              ),
            ).pipe(Effect.ignore);
          }),
        ),
        Effect.ignore,
        Effect.forkIn(runtimeScope),
      );

      const readyExit = yield* Effect.exit(
        Deferred.await(readyDeferred).pipe(Effect.timeoutOption(timeoutMs)),
      );
      if (Exit.isFailure(readyExit) || Option.isNone(readyExit.value)) {
        yield* Fiber.interruptAll([stdoutFiber, stderrFiber, exitFiber]).pipe(Effect.ignore);
      }
      if (Exit.isFailure(readyExit)) {
        return yield* fail(
          "startServerProcess",
          "Failed while waiting for OpenCode v2 server startup.",
          readyExit.cause,
        );
      }
      const readyOption = readyExit.value;
      if (Option.isNone(readyOption)) {
        return yield* fail(
          "startServerProcess",
          `Timed out waiting for OpenCode v2 server start after ${timeoutMs}ms.`,
        );
      }

      yield* Ref.set(stdoutRef, null);
      yield* Ref.set(stderrRef, null);

      const { url, password } = readyOption.value;
      const serverPassword = input.serverPassword ?? password;
      const infoInput =
        serverPassword === undefined ? { baseUrl: url } : { baseUrl: url, serverPassword };
      const { version } = yield* getInfo(infoInput);

      const serverProcess: OpenCodeV2ServerProcess = {
        url,
        ...(serverPassword === undefined ? {} : { serverPassword }),
        version,
        isRunning: child.isRunning.pipe(Effect.orElseSucceed(() => false)),
        exitCode: child.exitCode.pipe(
          Effect.map(Number),
          Effect.orElseSucceed(() => 0),
        ),
      };
      return serverProcess;
    });

  const connectToServer: OpenCodeV2RuntimeShape["connectToServer"] = (input) => {
    const serverUrl = input.serverUrl?.trim();
    if (serverUrl) {
      const infoInput =
        input.serverPassword === undefined
          ? { baseUrl: serverUrl }
          : { baseUrl: serverUrl, serverPassword: input.serverPassword };
      return getInfo(infoInput).pipe(
        Effect.map(({ version }) => ({
          url: serverUrl,
          ...(input.serverPassword === undefined ? {} : { serverPassword: input.serverPassword }),
          version,
          exitCode: null,
          external: true,
        })),
      );
    }
    // No explicit URL: prefer the user's shared background service (warm
    // caches, user integrations) and fall back to spawning our own server.
    return Effect.gen(function* () {
      const discovered = yield* discoverSharedService();
      if (discovered !== undefined) {
        const verified = yield* getInfo({
          baseUrl: discovered.url,
          ...(discovered.password === undefined ? {} : { serverPassword: discovered.password }),
        }).pipe(Effect.exit);
        if (verified._tag === "Success") {
          return {
            url: discovered.url,
            ...(discovered.password === undefined ? {} : { serverPassword: discovered.password }),
            version: verified.value.version,
            exitCode: null,
            external: true,
          };
        }
      }
      return yield* startOwnedServer(input);
    });
  };

  const startOwnedServer = (
    input: Parameters<OpenCodeV2RuntimeShape["connectToServer"]>[0],
  ): Effect.Effect<OpenCodeV2ServerConnection, OpenCodeV2RuntimeError, Scope.Scope> =>
    startServerProcess({
      binaryPath: input.binaryPath,
      directory: input.directory,
      ...(input.serverPassword === undefined ? {} : { serverPassword: input.serverPassword }),
      ...(input.environment === undefined ? {} : { environment: input.environment }),
      ...(input.port === undefined ? {} : { port: input.port }),
      ...(input.hostname === undefined ? {} : { hostname: input.hostname }),
      ...(input.timeoutMs === undefined ? {} : { timeoutMs: input.timeoutMs }),
    }).pipe(
      Effect.map((server) => ({
        url: server.url,
        ...(server.serverPassword === undefined ? {} : { serverPassword: server.serverPassword }),
        version: server.version,
        exitCode: server.exitCode,
        external: false,
      })),
    );

  const discoverSharedService = (): Effect.Effect<
    { readonly url: string; readonly password: string | undefined } | undefined,
    never
  > =>
    Effect.gen(function* () {
      const home = process.env.HOME ?? process.env.USERPROFILE;
      if (!home) return undefined;
      const candidate = pathService.join(home, ".local", "state", "opencode", "service.json");
      const raw = yield* fileSystem
        .readFileString(candidate)
        .pipe(Effect.orElseSucceed(() => undefined));
      if (raw === undefined) return undefined;
      return parseOpenCodeV2ServiceRegistration(raw);
    }).pipe(Effect.orElseSucceed(() => undefined));

  const getJsonList = (
    operation: string,
    baseUrl: string,
    path: string,
    serverPassword: string | undefined,
  ): Effect.Effect<ReadonlyArray<Record<string, unknown>>, OpenCodeV2RuntimeError> =>
    Effect.gen(function* () {
      const request = withAuth(
        HttpClientRequest.get(`${baseUrl}${path}`).pipe(
          HttpClientRequest.setHeader("accept", "application/json"),
        ),
        serverPassword,
      );
      const payload = yield* executeJson(operation, request).pipe((self) =>
        withTimeout(self, operation),
      );
      const envelope = yield* decodeDataEnvelope(payload).pipe(
        Effect.orElseSucceed(() => undefined),
      );
      const items = envelope === undefined ? unwrapData(payload) : envelope.data;
      return Array.isArray(items) ? (items as ReadonlyArray<Record<string, unknown>>) : [];
    });

  const postJson = (
    operation: string,
    baseUrl: string,
    path: string,
    serverPassword: string | undefined,
    body: Record<string, unknown>,
  ): Effect.Effect<unknown, OpenCodeV2RuntimeError> =>
    Effect.gen(function* () {
      const built = yield* HttpClientRequest.post(`${baseUrl}${path}`).pipe(
        HttpClientRequest.setHeader("accept", "application/json"),
        HttpClientRequest.setHeader("content-type", "application/json"),
        HttpClientRequest.bodyJson(body),
        Effect.mapError((cause) => fail(operation, `Invalid request body for ${operation}`, cause)),
      );
      const payload = yield* executeJson(operation, withAuth(built, serverPassword)).pipe((self) =>
        withTimeout(self, operation),
      );
      return unwrapData(payload);
    });

  const patchJson = (
    operation: string,
    baseUrl: string,
    path: string,
    serverPassword: string | undefined,
    body: Record<string, unknown>,
  ): Effect.Effect<unknown, OpenCodeV2RuntimeError> =>
    Effect.gen(function* () {
      const built = yield* HttpClientRequest.patch(`${baseUrl}${path}`).pipe(
        HttpClientRequest.setHeader("accept", "application/json"),
        HttpClientRequest.setHeader("content-type", "application/json"),
        HttpClientRequest.bodyJson(body),
        Effect.mapError((cause) => fail(operation, `Invalid request body for ${operation}`, cause)),
      );
      const payload = yield* executeJson(operation, withAuth(built, serverPassword)).pipe((self) =>
        withTimeout(self, operation),
      );
      return unwrapData(payload);
    });

  const listModels: OpenCodeV2RuntimeShape["listModels"] = (input) =>
    getJsonList("model.list", input.baseUrl, "/api/model", input.serverPassword).pipe(
      Effect.map((items) => {
        const out: Array<OpenCodeV2ModelInfo> = [];
        for (const item of items) {
          const id = typeof item.id === "string" ? item.id : undefined;
          if (!id) continue;
          const providerID =
            typeof item.providerID === "string" ? item.providerID : (id.split("/")[0] ?? "");
          const modelID = typeof item.modelID === "string" ? item.modelID : id;
          const name = typeof item.name === "string" && item.name.trim() ? item.name : id;
          const variants = Array.isArray(item.variants)
            ? item.variants.flatMap((entry) =>
                typeof entry === "string"
                  ? [entry]
                  : typeof entry === "object" &&
                      entry !== null &&
                      typeof (entry as Record<string, unknown>).id === "string"
                    ? [(entry as Record<string, string>).id as string]
                    : [],
              )
            : [];
          out.push({ id, providerID, modelID, name, variants });
        }
        return out;
      }),
      Effect.withSpan("opencodeV2.model.list"),
    );

  const listAgents: OpenCodeV2RuntimeShape["listAgents"] = (input) =>
    getJsonList("agent.list", input.baseUrl, "/api/agent", input.serverPassword).pipe(
      Effect.map((items) =>
        items.flatMap((item): ReadonlyArray<OpenCodeV2AgentInfo> => {
          const id =
            typeof item.id === "string"
              ? item.id
              : typeof item.name === "string"
                ? item.name
                : undefined;
          if (!id) return [];
          return [{ id, mode: typeof item.mode === "string" ? item.mode : "primary" }];
        }),
      ),
      Effect.orElseSucceed((): ReadonlyArray<OpenCodeV2AgentInfo> => []),
      Effect.withSpan("opencodeV2.agent.list"),
    );

  const listProviders: OpenCodeV2RuntimeShape["listProviders"] = (input) =>
    getJsonList("provider.list", input.baseUrl, "/api/provider", input.serverPassword).pipe(
      Effect.map((items) =>
        items.flatMap((item) => (typeof item.id === "string" ? [item.id] : [])),
      ),
      Effect.orElseSucceed((): ReadonlyArray<string> => []),
      Effect.withSpan("opencodeV2.provider.list"),
    );

  const getDefaultModel: OpenCodeV2RuntimeShape["getDefaultModel"] = (input) =>
    Effect.gen(function* () {
      const request = withAuth(
        HttpClientRequest.get(
          input.directory === undefined
            ? `${input.baseUrl}/api/model/default`
            : `${input.baseUrl}/api/model/default?location%5Bdirectory%5D=${encodeURIComponent(input.directory)}`,
        ).pipe(HttpClientRequest.setHeader("accept", "application/json")),
        input.serverPassword,
      );
      const payload = yield* executeJson("model.default", request).pipe((self) =>
        withTimeout(self, "model.default"),
      );
      const data = unwrapData(payload);
      if (typeof data !== "object" || data === null) return undefined;
      const id = (data as Record<string, unknown>).id;
      return typeof id === "string" && id.trim().length > 0 ? id : undefined;
    }).pipe(
      Effect.orElseSucceed(() => undefined),
      Effect.withSpan("opencodeV2.model.default"),
    );

  const resolveModelRef = (
    baseUrl: string,
    serverPassword: string | undefined,
    slug: string,
    variant?: string,
  ): Effect.Effect<
    { readonly id: string; readonly providerID: string; readonly variant?: string },
    OpenCodeV2RuntimeError
  > =>
    listModels({ baseUrl, ...(serverPassword === undefined ? {} : { serverPassword }) }).pipe(
      Effect.orElseSucceed((): ReadonlyArray<OpenCodeV2ModelInfo> => []),
      Effect.map((known) => {
        const match = known.find((candidate) => candidate.id === slug);
        const base =
          match !== undefined
            ? { id: match.id, providerID: match.providerID }
            : (() => {
                const separator = slug.indexOf("/");
                return separator > 0
                  ? { id: slug, providerID: slug.slice(0, separator) }
                  : { id: slug, providerID: slug };
              })();
        const trimmedVariant = variant?.trim();
        return trimmedVariant ? { ...base, variant: trimmedVariant } : base;
      }),
    );

  const createSession: OpenCodeV2RuntimeShape["createSession"] = (input) =>
    Effect.gen(function* () {
      // `POST /prompt` accepts no model — the model rides on create (or the
      // switch endpoint). Resolve the T3 slug to a `Model.Ref`: exact match
      // against `/api/model` first (v2 provider ids like `openrouter` differ
      // from the slug's first segment), first-segment split as fallback.
      const slug = input.modelSlug?.trim();
      const modelRef =
        slug === undefined || slug.length === 0
          ? undefined
          : yield* resolveModelRef(input.baseUrl, input.serverPassword, slug, input.variant);
      const payload = yield* postJson(
        "session.create",
        input.baseUrl,
        "/api/session",
        input.serverPassword,
        {
          ...(input.title ? { title: input.title } : {}),
          ...(modelRef === undefined ? {} : { model: modelRef }),
          ...(input.agent ? { agent: input.agent } : {}),
          ...(input.permissions === undefined ? {} : { permissions: input.permissions }),
          location: { directory: input.directory },
        },
      );
      const data = payload;
      if (
        typeof data !== "object" ||
        data === null ||
        !("id" in data) ||
        typeof (data as { id: unknown }).id !== "string"
      ) {
        return yield* fail("session.create", "OpenCode v2 session.create returned no session id.");
      }
      const record = data as { id: string; title?: unknown };
      return {
        id: record.id,
        ...(typeof record.title === "string" ? { title: record.title } : {}),
      };
    }).pipe(Effect.withSpan("opencodeV2.session.create"));

  const switchSessionModel: OpenCodeV2RuntimeShape["switchSessionModel"] = (input) =>
    Effect.gen(function* () {
      const slug = input.modelSlug.trim();
      if (slug.length === 0) return;
      const modelRef = yield* resolveModelRef(
        input.baseUrl,
        input.serverPassword,
        slug,
        input.variant,
      );
      yield* postJson(
        "session.switchModel",
        input.baseUrl,
        `/api/session/${encodeURIComponent(input.sessionId)}/model`,
        input.serverPassword,
        { model: modelRef },
      );
    }).pipe(Effect.withSpan("opencodeV2.session.switchModel"));

  const switchSessionAgent: OpenCodeV2RuntimeShape["switchSessionAgent"] = (input) =>
    postJson(
      "session.switchAgent",
      input.baseUrl,
      `/api/session/${encodeURIComponent(input.sessionId)}/agent`,
      input.serverPassword,
      { agent: input.agent },
    ).pipe(Effect.asVoid, Effect.withSpan("opencodeV2.session.switchAgent"));

  const replyPermission: OpenCodeV2RuntimeShape["replyPermission"] = (input) =>
    postJson(
      "session.permissionReply",
      input.baseUrl,
      `/api/session/${encodeURIComponent(input.sessionId)}/permission/${encodeURIComponent(input.requestId)}/reply`,
      input.serverPassword,
      { decision: input.decision },
    ).pipe(Effect.asVoid, Effect.withSpan("opencodeV2.session.permissionReply"));

  const replyForm: OpenCodeV2RuntimeShape["replyForm"] = (input) =>
    postJson(
      "session.formReply",
      input.baseUrl,
      `/api/session/${encodeURIComponent(input.sessionId)}/form/${encodeURIComponent(input.formId)}/reply`,
      input.serverPassword,
      { answer: input.answer },
    ).pipe(Effect.asVoid, Effect.withSpan("opencodeV2.session.formReply"));

  const runSessionCommand: OpenCodeV2RuntimeShape["runSessionCommand"] = (input) =>
    postJson(
      "session.command",
      input.baseUrl,
      `/api/session/${encodeURIComponent(input.sessionId)}/command`,
      input.serverPassword,
      { name: input.name, text: input.text },
    ).pipe(Effect.asVoid, Effect.withSpan("opencodeV2.session.command"));

  const cancelForm: OpenCodeV2RuntimeShape["cancelForm"] = (input) =>
    Effect.gen(function* () {
      const request = withAuth(
        HttpClientRequest.delete(
          `${input.baseUrl}/api/session/${encodeURIComponent(input.sessionId)}/form/${encodeURIComponent(input.formId)}`,
        ),
        input.serverPassword,
      );
      yield* httpClient.execute(request).pipe(
        Effect.mapError((cause) =>
          fail("session.formCancel", openCodeV2RuntimeErrorDetail(cause), cause),
        ),
        Effect.scoped,
        Effect.orElseSucceed(() => undefined),
      );
    }).pipe(Effect.asVoid);

  const compactSession: OpenCodeV2RuntimeShape["compactSession"] = (input) =>
    postJson(
      "session.compact",
      input.baseUrl,
      `/api/session/${encodeURIComponent(input.sessionId)}/compact`,
      input.serverPassword,
      {},
    ).pipe(
      Effect.map((data) => ({
        messageId:
          typeof data === "object" &&
          data !== null &&
          "id" in data &&
          typeof (data as { id: unknown }).id === "string"
            ? ((data as { id: string }).id as string)
            : "",
      })),
      Effect.withSpan("opencodeV2.session.compact"),
    );

  const listSkills: OpenCodeV2RuntimeShape["listSkills"] = (input) =>
    getJsonList(
      "skill.list",
      input.baseUrl,
      input.directory === undefined
        ? "/api/skill"
        : `/api/skill?location%5Bdirectory%5D=${encodeURIComponent(input.directory)}`,
      input.serverPassword,
    ).pipe(
      Effect.map((items) =>
        items.flatMap((item) => {
          const name = typeof item.name === "string" ? item.name.trim() : "";
          const path = typeof item.path === "string" ? item.path.trim() : "";
          if (!name || !path) return [];
          const description =
            typeof item.description === "string" && item.description.trim()
              ? item.description.trim()
              : undefined;
          return [{ name, path, ...(description === undefined ? {} : { description }) }];
        }),
      ),
      Effect.orElseSucceed((): ReadonlyArray<OpenCodeV2SkillInfo> => []),
      Effect.withSpan("opencodeV2.skill.list"),
    );

  const listCommands: OpenCodeV2RuntimeShape["listCommands"] = (input) =>
    getJsonList("command.list", input.baseUrl, "/api/command", input.serverPassword).pipe(
      Effect.map((items) =>
        items.flatMap((item) => {
          const name = typeof item.name === "string" ? item.name.trim() : "";
          if (!name) return [];
          const description =
            typeof item.description === "string" && item.description.trim()
              ? item.description.trim()
              : undefined;
          return [{ name, ...(description === undefined ? {} : { description }) }];
        }),
      ),
      Effect.orElseSucceed((): ReadonlyArray<OpenCodeV2CommandInfo> => []),
      Effect.withSpan("opencodeV2.command.list"),
    );

  const promptSession: OpenCodeV2RuntimeShape["promptSession"] = (input) =>
    postJson(
      "session.prompt",
      input.baseUrl,
      `/api/session/${encodeURIComponent(input.sessionId)}/prompt`,
      input.serverPassword,
      {
        text: input.text,
        ...(input.files === undefined || input.files.length === 0
          ? {}
          : {
              files: input.files.map((file) => ({
                uri: file.uri,
                ...(file.name === undefined ? {} : { name: file.name }),
              })),
            }),
      },
    ).pipe(
      Effect.map((data) => ({
        messageId:
          typeof data === "object" &&
          data !== null &&
          "id" in data &&
          typeof (data as { id: unknown }).id === "string"
            ? ((data as { id: string }).id as string)
            : "",
      })),
      Effect.withSpan("opencodeV2.session.prompt"),
    );

  const interruptSession: OpenCodeV2RuntimeShape["interruptSession"] = (input) =>
    postJson(
      "session.interrupt",
      input.baseUrl,
      `/api/session/${encodeURIComponent(input.sessionId)}/interrupt`,
      input.serverPassword,
      {},
    ).pipe(
      Effect.asVoid,
      Effect.orElseSucceed(() => undefined),
    );

  const deleteSession: OpenCodeV2RuntimeShape["deleteSession"] = (input) =>
    Effect.gen(function* () {
      const request = withAuth(
        HttpClientRequest.delete(
          `${input.baseUrl}/api/session/${encodeURIComponent(input.sessionId)}`,
        ),
        input.serverPassword,
      );
      yield* httpClient.execute(request).pipe(
        Effect.mapError((cause) =>
          fail("session.delete", openCodeV2RuntimeErrorDetail(cause), cause),
        ),
        Effect.scoped,
        Effect.orElseSucceed(() => undefined),
      );
    }).pipe(Effect.asVoid);

  const forkSession: OpenCodeV2RuntimeShape["forkSession"] = (input) =>
    Effect.gen(function* () {
      // Mirrors v1's fork-based rollback: copy projected history before a
      // message so T3 alone decides whether filesystem changes survive
      // (no native revert, which would rewrite workspace files).
      const body: Record<string, unknown> =
        input.beforeMessageId === undefined ? {} : { before: input.beforeMessageId };
      const payload = yield* postJson(
        "session.fork",
        input.baseUrl,
        `/api/session/${encodeURIComponent(input.sessionId)}/fork`,
        input.serverPassword,
        body,
      );
      if (
        typeof payload !== "object" ||
        payload === null ||
        !("id" in payload) ||
        typeof (payload as { id: unknown }).id !== "string"
      ) {
        return yield* fail("session.fork", "OpenCode v2 session.fork returned no session id.");
      }
      return { id: (payload as { id: string }).id };
    }).pipe(Effect.withSpan("opencodeV2.session.fork"));

  const updateSessionPermissions: OpenCodeV2RuntimeShape["updateSessionPermissions"] = (input) =>
    patchJson(
      "session.update",
      input.baseUrl,
      `/api/session/${encodeURIComponent(input.sessionId)}`,
      input.serverPassword,
      { permissions: input.permissions },
    ).pipe(Effect.asVoid, Effect.withSpan("opencodeV2.session.update"));

  const getSessionInfo: OpenCodeV2RuntimeShape["getSessionInfo"] = (input) =>
    Effect.gen(function* () {
      const request = withAuth(
        HttpClientRequest.get(
          `${input.baseUrl}/api/session/${encodeURIComponent(input.sessionId)}`,
        ).pipe(HttpClientRequest.setHeader("accept", "application/json")),
        input.serverPassword,
      );
      const payload = yield* executeJson("session.get", request).pipe((self) =>
        withTimeout(self, "session.get"),
      );
      const data = unwrapData(payload);
      if (typeof data !== "object" || data === null) {
        return yield* fail("session.get", "OpenCode v2 session.get returned no session payload.");
      }
      const revert =
        (data as Record<string, unknown>).revert !== null &&
        typeof (data as Record<string, unknown>).revert === "object"
          ? ((data as Record<string, { messageID?: unknown }>).revert as { messageID?: unknown })
          : undefined;
      const revertMessageId =
        revert !== undefined && typeof revert.messageID === "string" ? revert.messageID : undefined;
      // Verified against the 2.0.15 server: sessions carry
      // `location: { directory }` (same shape `session.create` sends).
      const record = data as Record<string, unknown>;
      const location =
        record.location !== null && typeof record.location === "object"
          ? (record.location as Record<string, unknown>)
          : undefined;
      const rawDirectory = location?.directory ?? record.directory;
      const directory =
        typeof rawDirectory === "string" && rawDirectory.trim().length > 0
          ? rawDirectory
          : undefined;
      return {
        ...(revertMessageId === undefined ? {} : { revertMessageId }),
        ...(directory === undefined ? {} : { directory }),
      };
    }).pipe(Effect.withSpan("opencodeV2.session.get"));

  const listMessages: OpenCodeV2RuntimeShape["listMessages"] = (input) =>
    Effect.gen(function* () {
      // The endpoint defaults to 50 newest-first items and caps `limit` at
      // 200, so a single call silently truncates long sessions (breaking
      // readThread order and rollback boundaries, unlike v1's full list).
      // Page newest-first (rollback only ever needs recent history), cap at
      // 1000 messages, then return chronological order.
      const raws: Array<Record<string, unknown>> = [];
      let cursor: string | undefined = undefined;
      for (let page = 0; page < 5; page += 1) {
        const path =
          cursor === undefined
            ? `/api/session/${encodeURIComponent(input.sessionId)}/message?limit=200&order=desc`
            : `/api/session/${encodeURIComponent(input.sessionId)}/message?limit=200&cursor=${encodeURIComponent(cursor)}`;
        const request = withAuth(
          HttpClientRequest.get(`${input.baseUrl}${path}`).pipe(
            HttpClientRequest.setHeader("accept", "application/json"),
          ),
          input.serverPassword,
        );
        const payload = yield* executeJson("session.messages", request).pipe((self) =>
          withTimeout(self, "session.messages"),
        );
        const envelope =
          typeof payload === "object" && payload !== null && "data" in payload
            ? (payload as {
                data: unknown;
                cursor?: { previous?: unknown; next?: unknown };
              })
            : undefined;
        const items = envelope !== undefined && Array.isArray(envelope.data) ? envelope.data : [];
        for (const item of items) {
          if (typeof item === "object" && item !== null) {
            raws.push(item as Record<string, unknown>);
          }
        }
        const next = envelope?.cursor?.next;
        if (typeof next !== "string" || next.length === 0) break;
        cursor = next;
      }
      return raws.reverse();
    }).pipe(
      Effect.map((items) =>
        items.flatMap((item) => {
          const id = typeof item.id === "string" ? item.id : undefined;
          const type = typeof item.type === "string" ? item.type : undefined;
          if (!id || !type) return [];
          const payload = item.payload;
          const payloadText =
            payload !== null &&
            typeof payload === "object" &&
            "text" in payload &&
            typeof (payload as { text: unknown }).text === "string"
              ? ((payload as { text: string }).text as string)
              : "";
          const text = typeof item.text === "string" ? item.text : payloadText;
          return [{ id, type, text }];
        }),
      ),
      Effect.orElseSucceed((): ReadonlyArray<OpenCodeV2MessageInfo> => []),
      Effect.withSpan("opencodeV2.session.messages"),
    );

  const getMessage: OpenCodeV2RuntimeShape["getMessage"] = (input) =>
    Effect.gen(function* () {
      const request = withAuth(
        HttpClientRequest.get(
          `${input.baseUrl}/api/session/${encodeURIComponent(input.sessionId)}/message/${encodeURIComponent(input.messageId)}`,
        ).pipe(HttpClientRequest.setHeader("accept", "application/json")),
        input.serverPassword,
      );
      const payload = yield* executeJson("session.message", request).pipe((self) =>
        withTimeout(self, "session.message"),
      );
      const data = unwrapData(payload);
      if (typeof data !== "object" || data === null) {
        return yield* fail("session.message", "OpenCode v2 message lookup returned no payload.");
      }
      const record = data as Record<string, unknown>;
      const id = typeof record.id === "string" ? record.id : input.messageId;
      const type = typeof record.type === "string" ? record.type : "unknown";
      const parts = Array.isArray(record.content) ? record.content : [];
      const text = parts
        .flatMap((part) =>
          typeof part === "object" &&
          part !== null &&
          typeof (part as Record<string, unknown>).text === "string"
            ? [(part as Record<string, string>).text as string]
            : [],
        )
        .join("");
      const finish = typeof record.finish === "string" ? record.finish : undefined;
      const errorRecord =
        record.error !== null && typeof record.error === "object"
          ? (record.error as Record<string, unknown>)
          : undefined;
      const errorMessage =
        errorRecord !== undefined && typeof errorRecord.message === "string"
          ? errorRecord.message
          : undefined;
      const modelRecord =
        record.model !== null && typeof record.model === "object"
          ? (record.model as Record<string, unknown>)
          : undefined;
      const model =
        modelRecord !== undefined &&
        typeof modelRecord.id === "string" &&
        typeof modelRecord.providerID === "string"
          ? { id: modelRecord.id, providerID: modelRecord.providerID }
          : undefined;
      return {
        id,
        type,
        text,
        ...(finish === undefined ? {} : { finish }),
        ...(errorMessage === undefined ? {} : { errorMessage }),
        ...(model === undefined ? {} : { model }),
      };
    }).pipe(Effect.withSpan("opencodeV2.session.message"));

  return {
    startServerProcess,
    connectToServer,
    getInfo,
    listModels,
    listAgents,
    listProviders,
    getDefaultModel,
    createSession,
    switchSessionModel,
    switchSessionAgent,
    promptSession,
    replyPermission,
    replyForm,
    runSessionCommand,
    cancelForm,
    compactSession,
    interruptSession,
    deleteSession,
    forkSession,
    getSessionInfo,
    updateSessionPermissions,
    listMessages,
    getMessage,
    listSkills,
    listCommands,
  } satisfies OpenCodeV2RuntimeShape;
});

export class OpenCodeV2Runtime extends Context.Service<OpenCodeV2Runtime, OpenCodeV2RuntimeShape>()(
  "t3/provider/opencodeV2Runtime",
) {}

export const OpenCodeV2RuntimeLive = Layer.effect(OpenCodeV2Runtime, makeOpenCodeV2Runtime).pipe(
  Layer.provide(NetService.layer),
);

/** Version probe helper: `opencode --version` without the server lifecycle. */
export const runOpenCodeV2Command = (input: {
  readonly binaryPath: string;
  readonly args: ReadonlyArray<string>;
  readonly environment?: NodeJS.ProcessEnv;
  readonly cwd?: string;
}): Effect.Effect<
  { readonly stdout: string; readonly stderr: string; readonly code: number },
  OpenCodeV2RuntimeError,
  ChildProcessSpawner.ChildProcessSpawner
> =>
  Effect.gen(function* () {
    const spawnCommand = yield* resolveSpawnCommand(
      input.binaryPath,
      input.args,
      input.environment ? { env: input.environment } : {},
    );
    const result = yield* spawnAndCollect(
      input.binaryPath,
      ChildProcess.make(spawnCommand.command, spawnCommand.args, {
        shell: spawnCommand.shell,
        ...(input.cwd ? { cwd: input.cwd } : {}),
        ...(input.environment ? { env: input.environment } : { extendEnv: true }),
      }),
    ).pipe(
      Effect.mapError((cause) =>
        failStatic(
          "runCommand",
          `Failed to execute '${input.binaryPath} ${input.args.join(" ")}'`,
          cause,
        ),
      ),
    );
    return result;
  }).pipe(Effect.scoped);

const failStatic = (operation: string, detail: string, cause?: unknown): OpenCodeV2RuntimeError =>
  new OpenCodeV2RuntimeError({ operation, detail, cause });
