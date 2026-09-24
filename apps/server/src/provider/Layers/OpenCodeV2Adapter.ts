/**
 * OpenCodeV2Adapter — minimal `ProviderAdapter` for the standalone OpenCode
 * v2 service.
 *
 * - `startSession` → `POST /api/session`
 * - `sendTurn` → `POST /api/session/{id}/prompt` (returns the user message
 *   immediately; the assistant streams over SSE)
 * - `interruptTurn` → `POST /api/session/{id}/interrupt`
 * - `stopSession` → best-effort interrupt + local teardown. The native
 *   session is deliberately KEPT server-side so a persisted resumeCursor
 *   can re-adopt it after reaper sweeps / restarts (mirrors v1, which
 *   aborts but never deletes).
 * - `streamEvents` → `GET /api/event` (SSE) mapped to `content.delta`,
 *   `turn.completed`/`turn.aborted`, `item.*` for tools, and
 *   `turn.plan.updated` synthesized from todowrite/todoread input
 * - `rollbackThread` → `POST /api/session/{id}/fork` (fork-only, like v1)
 *
 * v2 SSE event types verified against the 2.0.15 server binary:
 * `session.text/reasoning.started/delta/ended`,
 * `session.step.started/ended/failed` (+`step.streamed`, `retry.scheduled`),
 * `session.execution.started/succeeded/failed/interrupted`,
 * `session.tool.called`, streaming `input`, `progress`, `success`, `failed`,
 * `session.inbox.*`, `session.instructions.updated`,
 * `session.permission.asked/replied`, `session.form.*`. Unknown types are
 * ignored. There is no todo bus event — plan updates come from tool input.
 *
 * @module provider/Layers/OpenCodeV2Adapter
 */
import {
  EventId,
  type ApprovalRequestId,
  type OpenCodeV2Settings,
  type ProviderApprovalDecision,
  ProviderDriverKind,
  ProviderInstanceId,
  type ProviderRuntimeEvent,
  type ProviderSendTurnInput,
  type ProviderSession,
  type ProviderSessionStartInput,
  type ProviderUserInputAnswers,
  type RuntimeMode,
  RuntimeItemId,
  RuntimeRequestId,
  ThreadId,
  TurnId,
  type UserInputQuestion,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { HttpClient, HttpClientError, HttpClientRequest } from "effect/unstable/http";

import { ServerConfig } from "../../config.ts";
import {
  ProviderAdapterProcessError,
  ProviderAdapterRequestError,
  ProviderAdapterSessionClosedError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
  type ProviderAdapterError,
} from "../Errors.ts";
import type { ProviderAdapterShape, ProviderThreadSnapshot } from "../Services/ProviderAdapter.ts";
import {
  OpenCodeV2Runtime,
  openCodeV2RuntimeErrorDetail,
  type OpenCodeV2PermissionRule,
  type OpenCodeV2PromptFile,
} from "../opencodeV2Runtime.ts";
import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";
import { mapToolNameToItemType } from "@t3tools/shared/toolActivity";
import { resolveAttachmentPath } from "../../attachmentStore.ts";

const PROVIDER = ProviderDriverKind.make("opencodeV2");
const RESUME_VERSION = 1 as const;

const V2SseEnvelope = Schema.Struct({
  type: Schema.String,
  data: Schema.optional(Schema.Unknown),
});
const decodeSseLine = Schema.decodeUnknownEffect(Schema.fromJsonString(V2SseEnvelope));

function parseResume(raw: unknown): { readonly sessionId: string } | undefined {
  if (typeof raw !== "object" || raw === null || Array.isArray(raw)) return undefined;
  const record = raw as Record<string, unknown>;
  if (record.schemaVersion !== RESUME_VERSION) return undefined;
  if (typeof record.sessionId !== "string" || record.sessionId.trim().length === 0)
    return undefined;
  return { sessionId: record.sessionId.trim() };
}

/**
 * Whether an error definitively reports a missing v2 session. Only a
 * confirmed miss may silently start a fresh session; any other failure must
 * propagate, or a transient blip resets a live thread to an empty one —
 * the same silent context loss v1 guards against. Decides on structured
 * signals only: a numeric 404, an `HTTP 404` detail emitted by
 * `opencodeV2Runtime.executeJson`, or the exact `NotFoundError` name, found
 * via a bounded walk over `cause`/`body`/`error`/`data`. An explicit
 * non-404 status seals its subtree. Exported for unit testing.
 */
export function isOpenCodeV2NotFound(cause: unknown): boolean {
  const seen = new Set<unknown>();
  const queue: Array<unknown> = [cause];
  for (let steps = 0; queue.length > 0 && steps < 32; steps += 1) {
    const node = queue.shift();
    if (node === null || typeof node !== "object" || seen.has(node)) {
      continue;
    }
    seen.add(node);
    const record = node as Record<string, unknown>;

    const response = record.response;
    const statuses = [
      record.status,
      record.statusCode,
      response !== null && typeof response === "object"
        ? (response as { readonly status?: unknown }).status
        : undefined,
    ].filter((status): status is number => typeof status === "number");
    if (statuses.includes(404)) {
      return true;
    }
    if (statuses.length > 0) {
      continue;
    }

    for (const key of ["detail", "message"] as const) {
      const value = record[key];
      if (typeof value === "string" && /HTTP\s+404\b/.test(value)) {
        return true;
      }
    }

    const name = record.name;
    if (typeof name === "string" && name.toLowerCase() === "notfounderror") {
      return true;
    }

    for (const key of ["cause", "body", "error", "data"] as const) {
      if (record[key] !== undefined) {
        queue.push(record[key]);
      }
    }
  }
  return false;
}

/**
 * Whether two directory spellings name the same location. Same lexical /
 * realPath widening as v1's `isSameOpenCodeDirectory` (duplicated here to
 * keep this adapter independent of `OpenCodeAdapter`): raw string equality
 * misreads trailing slashes and symlinked cwds as a directory change and
 * would needlessly drop conversation history on every resume.
 */
export function isSameV2Directory(
  fileSystem: FileSystem.FileSystem,
  path: Path.Path,
  left: string,
  right: string,
): Effect.Effect<boolean> {
  const lexicalLeft = path.resolve(left);
  const lexicalRight = path.resolve(right);
  if (lexicalLeft === lexicalRight) {
    return Effect.succeed(true);
  }
  const canonicalize = (lexical: string) =>
    fileSystem.realPath(lexical).pipe(Effect.orElseSucceed(() => lexical));
  return Effect.zipWith(
    canonicalize(lexicalLeft),
    canonicalize(lexicalRight),
    (canonicalLeft, canonicalRight) => canonicalLeft === canonicalRight,
  );
}

interface V2SessionContext {
  session: ProviderSession;
  openCodeSessionId: string;
  serverUrl: string;
  serverPassword?: string;
  directory: string;
  runtimeMode: RuntimeMode;
  modelSlug: string | undefined;
  variant: string | undefined;
  agent: string | undefined;
  fullAccess: boolean;
  readonly pendingPermissions: Map<string, V2PendingPermission>;
  readonly pendingForms: Map<string, V2PendingForm>;
  readonly emittedTerminalRequestIds: Set<string>;
  activeTurnId: TurnId | undefined;
  inputTokens: number;
  outputTokens: number;
  reasoningTokens: number;
  cachedInputTokens: number;
  readonly stopped: Ref.Ref<boolean>;
  readonly sessionScope: Scope.Closeable;
}

interface V2PendingPermission {
  readonly id: string;
  readonly action: string;
  readonly detail: string | undefined;
}

interface V2FormField {
  readonly key: string;
  readonly questionId: string;
  readonly multi: boolean;
}

interface V2PendingForm {
  readonly id: string;
  readonly fields: ReadonlyArray<V2FormField>;
}

function normalizeV2FormFields(data: unknown): {
  readonly title: string | undefined;
  readonly questions: Array<UserInputQuestion>;
  readonly fields: Array<V2FormField>;
} {
  const questions: Array<UserInputQuestion> = [];
  const fields: Array<V2FormField> = [];
  const title = sseStringField(data, "title");
  const rawFields =
    typeof data === "object" &&
    data !== null &&
    Array.isArray((data as Record<string, unknown>).fields)
      ? ((data as Record<string, unknown>).fields as Array<unknown>)
      : [];
  rawFields.forEach((rawField, index) => {
    if (typeof rawField !== "object" || rawField === null) return;
    const field = rawField as Record<string, unknown>;
    const key = typeof field.key === "string" ? field.key : `field-${index}`;
    const fieldTitle = typeof field.title === "string" ? field.title : key;
    const description = typeof field.description === "string" ? field.description : undefined;
    const multi = field.type === "multiselect";
    const rawOptions = Array.isArray(field.options) ? field.options : [];
    const options = rawOptions.flatMap((rawOption) => {
      if (typeof rawOption !== "object" || rawOption === null) return [];
      const option = rawOption as Record<string, unknown>;
      const label =
        typeof option.label === "string"
          ? option.label
          : typeof option.value === "string"
            ? option.value
            : undefined;
      if (label === undefined) return [];
      const optionDescription = typeof option.description === "string" ? option.description : "";
      return [
        {
          label,
          description: optionDescription,
          ...(typeof option.value === "string" ? { value: option.value } : {}),
        },
      ];
    });
    const questionId = `form-${index}-${key}`;
    fields.push({ key, questionId, multi });
    questions.push({
      id: questionId,
      header: fieldTitle,
      question: description ?? fieldTitle,
      options,
      ...(options.length === 0 || field.custom === true ? { allowCustomAnswer: true } : {}),
      ...(multi ? { multiSelect: true } : {}),
    });
  });
  return { title, questions, fields };
}

function buildV2PermissionRules(runtimeMode: RuntimeMode): ReadonlyArray<OpenCodeV2PermissionRule> {
  if (runtimeMode === "full-access") {
    return [{ action: "*", resource: "*", effect: "allow" }];
  }
  if (runtimeMode === "auto-accept-edits") {
    return [
      { action: "read", resource: "*", effect: "allow" },
      { action: "edit", resource: "*", effect: "allow" },
      { action: "*", resource: "*", effect: "ask" },
    ];
  }
  return [
    { action: "read", resource: "*", effect: "allow" },
    { action: "*", resource: "*", effect: "ask" },
  ];
}

export function mapV2PermissionToRequestType(
  action: string,
): "command_execution_approval" | "file_read_approval" | "file_change_approval" {
  const normalized = action.toLowerCase();
  if (normalized === "read") return "file_read_approval";
  if (normalized === "edit" || normalized === "write") return "file_change_approval";
  return "command_execution_approval";
}

function mapV2ApprovalDecision(
  decision: "accept" | "acceptForSession" | "acceptAlways" | "decline" | "cancel",
): "once" | "always" | "reject" {
  switch (decision) {
    case "accept":
      return "once";
    case "acceptForSession":
    case "acceptAlways":
      return "always";
    default:
      return "reject";
  }
}

const OPENCODE_V2_NATIVE_IMAGE_MIMES = new Set([
  "image/png",
  "image/jpeg",
  "image/gif",
  "image/webp",
]);
const OPENCODE_V2_FILE_PART_MAX_BYTES = 20 * 1024 * 1024;

function toV2PromptFiles(input: {
  readonly attachments: ProviderSendTurnInput["attachments"];
  readonly attachmentsDir: string;
}): Array<OpenCodeV2PromptFile> {
  const files: Array<OpenCodeV2PromptFile> = [];
  for (const attachment of input.attachments ?? []) {
    if (attachment.type !== "file" && attachment.type !== "image") continue;
    if (
      "source" in attachment &&
      attachment.source !== undefined &&
      "_tag" in attachment.source &&
      attachment.source._tag === "pasted-text"
    ) {
      continue;
    }
    const mime = attachment.mimeType.trim().toLowerCase();
    const native =
      attachment.sizeBytes <= OPENCODE_V2_FILE_PART_MAX_BYTES &&
      (OPENCODE_V2_NATIVE_IMAGE_MIMES.has(mime) ||
        mime.startsWith("text/") ||
        mime === "application/pdf");
    if (!native) continue;
    const path = resolveAttachmentPath({ attachmentsDir: input.attachmentsDir, attachment });
    if (!path) continue;
    const uri = path.startsWith("file://") ? path : `file://${path.replace(/\\/g, "/")}`;
    files.push({ uri, ...(attachment.name ? { name: attachment.name } : {}) });
  }
  return files;
}

function sseDataSessionId(data: unknown): string | undefined {
  if (typeof data !== "object" || data === null) return undefined;
  const sessionID = (data as { sessionID?: unknown }).sessionID;
  if (typeof sessionID === "string") return sessionID;
  const nested = (data as { form?: unknown }).form;
  if (typeof nested === "object" && nested !== null) {
    const nestedSessionID = (nested as { sessionID?: unknown }).sessionID;
    if (typeof nestedSessionID === "string") return nestedSessionID;
  }
  return undefined;
}

function sseStringField(data: unknown, field: string): string | undefined {
  if (typeof data !== "object" || data === null) return undefined;
  const value = (data as Record<string, unknown>)[field];
  return typeof value === "string" ? value : undefined;
}

export function mapOpenCodeV2SseType(
  v2Type: string,
): "text" | "reasoning" | "step" | "execution" | "tool" | "permission" | "form" | "ignore" {
  if (v2Type.startsWith("session.text.")) return "text";
  if (v2Type.startsWith("session.reasoning.")) return "reasoning";
  if (v2Type.startsWith("session.step.")) return "step";
  if (v2Type.startsWith("session.execution.")) return "execution";
  if (v2Type.startsWith("session.tool.")) return "tool";
  if (v2Type === "permission.asked" || v2Type === "permission.replied") return "permission";
  if (v2Type.startsWith("form.")) return "form";
  if (v2Type === "session.error" || v2Type === "session.usage.updated") return "step";
  return "ignore";
}

/**
 * Only `succeeded`/`failed` settle a T3 turn. v2 emits
 * `session.execution.started` when a turn *begins* (observed against
 * v2.0.15) — completing the turn there ends it before any text streams,
 * so threads render as fragments. `session.step.failed` is likewise not
 * terminal: the server follows it with `session.retry.scheduled` and keeps
 * the same execution alive until its own `succeeded`/`failed` verdict.
 */
export function mapOpenCodeV2ExecutionOutcome(v2Type: string): "succeeded" | "failed" | undefined {
  if (v2Type === "session.execution.succeeded") return "succeeded";
  if (v2Type === "session.execution.failed") return "failed";
  return undefined;
}

/**
 * v2.0.15 carries failures as structured `{ type, message, status }`
 * objects on `session.execution.failed` / `session.step.failed` data
 * (older shapes used plain strings). Extract the human message.
 */
export function openCodeV2ErrorMessage(data: unknown): string | undefined {
  if (typeof data !== "object" || data === null) return undefined;
  const record = data as Record<string, unknown>;
  const nested = record.error;
  if (typeof nested === "string") return nested;
  if (typeof nested === "object" && nested !== null) {
    const message = (nested as Record<string, unknown>).message;
    if (typeof message === "string") return message;
  }
  const message = record.message;
  return typeof message === "string" ? message : undefined;
}

function v2ToolRecord(data: unknown): Record<string, unknown> {
  return typeof data === "object" && data !== null ? (data as Record<string, unknown>) : {};
}

function v2ToolString(value: unknown): string | undefined {
  return typeof value === "string" && value.trim().length > 0 ? value : undefined;
}

export interface NormalizedV2ToolEvent {
  readonly toolName: string;
  readonly callId: string | undefined;
  readonly title: string;
  readonly command: string | undefined;
  readonly input: unknown;
  readonly output: string | undefined;
  readonly error: string | undefined;
}

/** Flatten tool text content (`string | { text } | { content } | blocks`). */
export function openCodeV2ToolText(value: unknown, depth = 0): string | undefined {
  if (depth > 4 || value === null || value === undefined) return undefined;
  if (typeof value === "string") return value.trim().length > 0 ? value : undefined;
  if (Array.isArray(value)) {
    const chunks = value.flatMap((entry) => {
      const text = openCodeV2ToolText(entry, depth + 1);
      return text === undefined ? [] : [text];
    });
    return chunks.length > 0 ? chunks.join("\n") : undefined;
  }
  if (typeof value === "object") {
    const record = value as Record<string, unknown>;
    for (const key of ["text", "content", "output", "stdout", "result"]) {
      const text = openCodeV2ToolText(record[key], depth + 1);
      if (text !== undefined) return text;
    }
    const stderr = v2ToolString(record.stderr);
    if (stderr !== undefined) return stderr;
  }
  return undefined;
}

const V2_TOOL_OUTPUT_MAX_CHARS = 8000;

function truncateV2ToolText(value: string | undefined): string | undefined {
  if (value === undefined) return undefined;
  const trimmed = value.trim();
  if (trimmed.length === 0) return undefined;
  if (trimmed.length <= V2_TOOL_OUTPUT_MAX_CHARS) return trimmed;
  return `${trimmed.slice(0, V2_TOOL_OUTPUT_MAX_CHARS).trimEnd()}\n[truncated]`;
}

/**
 * Normalize a `session.tool.*` event into the fields the timeline needs.
 * Reads both flat (`{ name, input, output }`) and nested
 * (`{ name, state: { input, output } }`, matching the transcript
 * `Session.Message.Assistant.Tool` shape) layouts.
 */
export function normalizeOpenCodeV2ToolEvent(data: unknown): NormalizedV2ToolEvent {
  const record = v2ToolRecord(data);
  const state = v2ToolRecord(record.state);
  const metadata = v2ToolRecord(state.metadata);
  const toolName =
    v2ToolString(record.name) ?? v2ToolString(record.tool) ?? v2ToolString(state.tool) ?? "tool";
  const callId =
    v2ToolString(record.id) ??
    v2ToolString(record.callID) ??
    v2ToolString(record.callId) ??
    v2ToolString(record.toolID) ??
    v2ToolString(record.toolId) ??
    v2ToolString(record.toolCallID) ??
    v2ToolString(record.toolCallId) ??
    v2ToolString(state.id) ??
    undefined;
  const input =
    record.input ??
    state.input ??
    record.args ??
    record.arguments ??
    record.parameters ??
    undefined;
  const command = extractV2ToolCommand(toolName, input);
  const title =
    v2ToolString(record.title) ??
    v2ToolString(state.title) ??
    v2ToolString(metadata.title) ??
    command ??
    toolName;
  const output = truncateV2ToolText(
    openCodeV2ToolText(record.output) ??
      openCodeV2ToolText(record.result) ??
      openCodeV2ToolText(record.content) ??
      openCodeV2ToolText(state.output) ??
      openCodeV2ToolText(state.content) ??
      openCodeV2ToolText(state.result),
  );
  const error = openCodeV2ErrorMessage(record) ?? openCodeV2ErrorMessage(state) ?? undefined;
  return { toolName, callId, title, command, input, output, error };
}

function extractV2ToolCommand(toolName: string, input: unknown): string | undefined {
  const itemType = mapToolNameToItemType(toolName);
  if (typeof input === "string") {
    return itemType === "command_execution" ? input.trim() || undefined : undefined;
  }
  if (typeof input === "object" && input !== null && !Array.isArray(input)) {
    const record = input as Record<string, unknown>;
    const raw = record.command ?? record.cmd;
    if (typeof raw === "string" && raw.trim().length > 0) return raw.trim();
    if (Array.isArray(raw)) {
      const joined = raw
        .filter((entry): entry is string => typeof entry === "string")
        .join(" ")
        .trim();
      return joined.length > 0 ? joined : undefined;
    }
  }
  return undefined;
}

/** One-line live summary for a running tool (command, file path, or compact input). */
export function describeOpenCodeV2ToolInput(input: unknown): string | undefined {
  if (typeof input === "string") {
    const trimmed = input.trim().replace(/\s+/g, " ");
    if (trimmed.length === 0) return undefined;
    return trimmed.length <= 500 ? trimmed : `${trimmed.slice(0, 497).trimEnd()}...`;
  }
  if (typeof input === "object" && input !== null && !Array.isArray(input)) {
    const record = input as Record<string, unknown>;
    for (const key of ["filePath", "path", "file", "filename", "command", "cmd", "url", "query"]) {
      const value = v2ToolString(record[key]);
      if (value !== undefined) {
        const singleLine = value.replace(/\s+/g, " ").trim();
        return singleLine.length <= 500 ? singleLine : `${singleLine.slice(0, 497).trimEnd()}...`;
      }
    }
    try {
      const json = JSON.stringify(input);
      return json.length <= 500 ? json : `${json.slice(0, 497).trimEnd()}...`;
    } catch {
      return undefined;
    }
  }
  return undefined;
}

export interface V2RollbackBoundary {
  /** Id of the first message to drop (the fork `before` boundary). */
  readonly beforeMessageId: string;
}

export interface V2RollbackMessage {
  readonly id: string;
  readonly type: string;
}

/**
 * Find the rewind boundary for `rollbackThread`, mirroring v1's turn math
 * exactly: one turn per `assistant` message (like both adapters' readThread),
 * the target is `numTurns` back from the end, and the boundary is the last
 * `user` message at or before the target (forks retain everything before
 * it). A native revert marker truncates history first, like v1's readThread.
 */
export function findV2RollbackBoundary(
  messages: ReadonlyArray<V2RollbackMessage>,
  numTurns: number,
  revertMessageId?: string,
): V2RollbackBoundary | undefined {
  const entries =
    revertMessageId === undefined
      ? messages
      : messages.slice(
          0,
          (() => {
            const index = messages.findIndex((message) => message.id === revertMessageId);
            return index < 0 ? messages.length : index;
          })(),
        );
  const assistantIndices: Array<number> = [];
  entries.forEach((message, index) => {
    if (message.type === "assistant") assistantIndices.push(index);
  });
  if (assistantIndices.length === 0) return undefined;
  const targetIndex =
    assistantIndices[Math.max(0, assistantIndices.length - numTurns)] ??
    assistantIndices[assistantIndices.length - 1]!;
  for (let index = targetIndex; index >= 0; index -= 1) {
    if (entries[index]?.type === "user") {
      return { beforeMessageId: entries[index]!.id };
    }
  }
  return { beforeMessageId: entries[targetIndex]!.id };
}

export interface V2TodoPlanStep {
  readonly step: string;
  readonly status: "pending" | "inProgress" | "completed";
}

/**
 * Tool row lifecycle for `session.tool.*` events. Exact names only: 2.0.15
 * refines streaming input via `session.tool.input.started/ended` and
 * `session.tool.progress` for the SAME call, so suffix matching would
 * double-count one run as several rows. Returns undefined for refinements
 * and unknown subtypes (no row event).
 */
export function mapOpenCodeV2ToolLifecycle(v2Type: string): "started" | "ended" | undefined {
  if (v2Type === "session.tool.called" || v2Type === "session.tool.started") {
    return "started";
  }
  if (
    v2Type === "session.tool.ended" ||
    v2Type === "session.tool.success" ||
    v2Type === "session.tool.failed" ||
    v2Type === "session.tool.error"
  ) {
    return "ended";
  }
  return undefined;
}

/**
 * Translate a todowrite/todoread tool input into plan steps for
 * `turn.plan.updated`, matching v1's `todo.updated` rendering. Returns
 * undefined when the input carries no todo list.
 */
export function v2TodoPlanFromToolInput(input: unknown): ReadonlyArray<V2TodoPlanStep> | undefined {
  if (typeof input !== "object" || input === null || Array.isArray(input)) return undefined;
  const todos = (input as Record<string, unknown>).todos;
  if (!Array.isArray(todos)) return undefined;
  const steps: Array<V2TodoPlanStep> = [];
  for (const entry of todos) {
    if (typeof entry !== "object" || entry === null) continue;
    const record = entry as Record<string, unknown>;
    const content = typeof record.content === "string" ? record.content.trim() : "";
    if (content.length === 0) continue;
    const status = typeof record.status === "string" ? record.status : "";
    steps.push({
      step: content,
      status:
        status === "completed" ? "completed" : status === "in_progress" ? "inProgress" : "pending",
    });
  }
  return steps.length > 0 ? steps : undefined;
}

const V2_TODO_TOOL_NAMES = new Set(["todowrite", "todoread"]);

export function isV2TodoTool(toolName: string): boolean {
  return V2_TODO_TOOL_NAMES.has(toolName.toLowerCase());
}

export interface OpenCodeV2AdapterOptions {
  readonly instanceId?: ProviderInstanceId;
  readonly environment?: NodeJS.ProcessEnv;
}

const nowIso = Effect.map(DateTime.now, DateTime.formatIso);

const isSessionNotFoundError = Schema.is(ProviderAdapterSessionNotFoundError);
const isSessionClosedError = Schema.is(ProviderAdapterSessionClosedError);
const isAdapterRequestError = Schema.is(ProviderAdapterRequestError);

const toRequestError = (operation: string, cause: unknown): ProviderAdapterRequestError =>
  new ProviderAdapterRequestError({
    provider: PROVIDER,
    method: operation,
    detail: openCodeV2RuntimeErrorDetail(cause),
    cause,
  });

const wrapAdapterError = (threadId: ThreadId, cause: unknown): ProviderAdapterError =>
  isSessionNotFoundError(cause) || isSessionClosedError(cause) || isAdapterRequestError(cause)
    ? cause
    : new ProviderAdapterProcessError({
        provider: PROVIDER,
        threadId,
        detail: openCodeV2RuntimeErrorDetail(cause),
        cause,
      });

export function makeOpenCodeV2Adapter(
  settings: OpenCodeV2Settings,
  options?: OpenCodeV2AdapterOptions,
) {
  return Effect.gen(function* () {
    const boundInstanceId = options?.instanceId ?? ProviderInstanceId.make("opencodeV2");
    const serverConfig = yield* ServerConfig;
    const runtime = yield* OpenCodeV2Runtime;
    const crypto = yield* Crypto.Crypto;
    const httpClient = yield* HttpClient.HttpClient;
    const fileSystem = yield* FileSystem.FileSystem;
    const pathService = yield* Path.Path;
    const sameDirectory = (left: string, right: string) =>
      isSameV2Directory(fileSystem, pathService, left, right);
    const runtimeEvents = yield* Queue.unbounded<ProviderRuntimeEvent>();
    const sessions = new Map<ThreadId, V2SessionContext>();

    const randomEventId = crypto.randomUUIDv4.pipe(
      Effect.map(EventId.make),
      Effect.mapError((cause) => toRequestError("crypto/randomUUIDv4", cause)),
    );

    const buildBase = (input: {
      readonly threadId: ThreadId;
      readonly turnId?: TurnId;
      readonly itemId?: string;
      readonly requestId?: string;
      readonly raw?: unknown;
    }) =>
      Effect.all({ eventId: randomEventId, createdAt: nowIso }).pipe(
        Effect.map(({ eventId, createdAt }) => ({
          eventId,
          provider: PROVIDER,
          providerInstanceId: boundInstanceId,
          threadId: input.threadId,
          createdAt,
          ...(input.turnId === undefined ? {} : { turnId: input.turnId }),
          ...(input.itemId === undefined ? {} : { itemId: RuntimeItemId.make(input.itemId) }),
          ...(input.requestId === undefined
            ? {}
            : { requestId: RuntimeRequestId.make(input.requestId) }),
          ...(input.raw === undefined
            ? {}
            : { raw: { source: "opencode.sdk.event" as const, payload: input.raw } }),
        })),
      );

    const emit = (event: ProviderRuntimeEvent): Effect.Effect<void> =>
      Queue.offer(runtimeEvents, event).pipe(Effect.asVoid);

    const ensureContext = (
      threadId: ThreadId,
    ): Effect.Effect<
      V2SessionContext,
      ProviderAdapterSessionNotFoundError | ProviderAdapterSessionClosedError
    > =>
      Effect.gen(function* () {
        const context = sessions.get(threadId);
        if (context === undefined) {
          return yield* new ProviderAdapterSessionNotFoundError({ provider: PROVIDER, threadId });
        }
        if (yield* Ref.get(context.stopped)) {
          return yield* new ProviderAdapterSessionClosedError({ provider: PROVIDER, threadId });
        }
        return context;
      });

    const emitTurnCompleted = (
      context: V2SessionContext,
      turnId: TurnId,
      failed: boolean,
      errorMessage: string | undefined,
      raw: unknown,
    ): Effect.Effect<void, ProviderAdapterRequestError> =>
      Effect.gen(function* () {
        context.activeTurnId = undefined;
        const at = yield* nowIso;
        const nextSession = {
          ...context.session,
          status: failed ? "error" : "ready",
          updatedAt: at,
        } as ProviderSession;
        const mutableSession = nextSession as unknown as Record<string, unknown>;
        if (failed && errorMessage !== undefined) {
          mutableSession.lastError = errorMessage;
        } else {
          delete mutableSession.lastError;
        }
        context.session = nextSession;
        const base = yield* buildBase({ threadId: context.session.threadId, turnId, raw });
        yield* emit({
          ...base,
          type: "turn.completed",
          payload: {
            state: failed ? "failed" : "completed",
            ...(errorMessage === undefined ? {} : { errorMessage }),
            tokenUsage: {
              usageStatus: "partial",
              usageScope: "main_agent",
              inputTokens: context.inputTokens,
              outputTokens: context.outputTokens,
              reasoningTokens: Math.min(context.outputTokens, context.reasoningTokens),
              cachedInputTokens: context.cachedInputTokens,
              hasSubagents: false,
            },
          },
        });
      });

    const emitTurnAborted = (
      context: V2SessionContext,
      turnId: TurnId,
      raw?: unknown,
    ): Effect.Effect<void, ProviderAdapterRequestError> =>
      Effect.gen(function* () {
        context.activeTurnId = undefined;
        context.session = { ...context.session, status: "ready", updatedAt: yield* nowIso };
        const base = yield* buildBase({
          threadId: context.session.threadId,
          turnId,
          ...(raw === undefined ? {} : { raw }),
        });
        yield* emit({
          ...base,
          type: "turn.aborted",
          payload: {
            reason: "Interrupted.",
            tokenUsage: {
              usageStatus: "partial",
              usageScope: "main_agent",
              inputTokens: context.inputTokens,
              outputTokens: context.outputTokens,
              hasSubagents: false,
            },
          },
        });
      });

    const handleSseEvent = (
      context: V2SessionContext,
      v2Type: string,
      data: unknown,
      raw: unknown,
    ): Effect.Effect<void, ProviderAdapterRequestError> =>
      Effect.gen(function* () {
        // `/api/event` is a GLOBAL bus across every session on the server
        // (chat turns, title-generation sessions, other threads). Only handle
        // events attributable to this session — an unattributed or foreign
        // event must never move this turn (a foreign `execution.succeeded`
        // would otherwise end the turn before its own reply streams).
        const eventSessionId = sseDataSessionId(data);
        if (eventSessionId === undefined || eventSessionId !== context.openCodeSessionId) {
          return;
        }
        const kind = mapOpenCodeV2SseType(v2Type);
        if (kind === "text" || kind === "reasoning") {
          if (!v2Type.endsWith(".delta")) return;
          const delta = sseStringField(data, "delta");
          if (delta === undefined) return;
          const base = yield* buildBase({
            threadId: context.session.threadId,
            ...(context.activeTurnId === undefined ? {} : { turnId: context.activeTurnId }),
            raw,
          });
          yield* emit({
            ...base,
            type: "content.delta",
            payload: {
              streamKind: kind === "reasoning" ? "reasoning_text" : "assistant_text",
              delta,
            },
          });
          return;
        }
        if (kind === "step") {
          if (v2Type === "session.step.ended" && typeof data === "object" && data !== null) {
            const tokens = (data as { tokens?: unknown }).tokens;
            if (typeof tokens === "object" && tokens !== null) {
              const record = tokens as Record<string, unknown>;
              if (typeof record.input === "number") context.inputTokens += record.input;
              if (typeof record.output === "number") context.outputTokens += record.output;
              if (typeof record.reasoning === "number") context.reasoningTokens += record.reasoning;
              const cache = record.cache;
              if (typeof cache === "object" && cache !== null) {
                const read = (cache as Record<string, unknown>).read;
                if (typeof read === "number") context.cachedInputTokens += read;
              }
            }
          }
          return;
        }
        if (kind === "execution") {
          // The server confirms interrupts with `execution.interrupted`
          // (verified in the 2.0.15 binary) — settle locally-initiated or
          // foreign interrupts the same way instead of spinning forever.
          if (v2Type === "session.execution.interrupted") {
            const interruptedTurnId = context.activeTurnId;
            if (interruptedTurnId === undefined) return;
            yield* cancelPendingForms(context);
            yield* emitTurnAborted(context, interruptedTurnId, raw);
            return;
          }
          const outcome = mapOpenCodeV2ExecutionOutcome(v2Type);
          if (outcome === undefined) return;
          const turnId = context.activeTurnId;
          if (turnId === undefined) return;
          const failed = outcome === "failed";
          let errorMessage = openCodeV2ErrorMessage(data);
          if (failed && errorMessage === undefined) {
            // `execution.failed` events carry no reason — the provider error
            // lives on the assistant message detail. Look it up so failed
            // turns report *why* instead of vanishing silently.
            const auth =
              context.serverPassword === undefined
                ? { baseUrl: context.serverUrl }
                : { baseUrl: context.serverUrl, serverPassword: context.serverPassword };
            const messages = yield* runtime
              .listMessages({ ...auth, sessionId: context.openCodeSessionId })
              .pipe(
                Effect.orElseSucceed(
                  (): ReadonlyArray<{
                    readonly id: string;
                    readonly type: string;
                    readonly text: string;
                  }> => [],
                ),
              );
            const assistantIds = messages
              .filter((message) => message.type === "assistant")
              .map((message) => message.id);
            for (const messageId of assistantIds.toReversed()) {
              const detail = yield* runtime
                .getMessage({ ...auth, sessionId: context.openCodeSessionId, messageId })
                .pipe(Effect.orElseSucceed(() => undefined));
              if (detail?.errorMessage) {
                errorMessage = detail.errorMessage;
                break;
              }
            }
          }
          yield* emitTurnCompleted(context, turnId, failed, errorMessage, raw);
          return;
        }
        if (kind === "permission") {
          if (v2Type === "permission.asked" && typeof data === "object" && data !== null) {
            const record = data as Record<string, unknown>;
            const id = typeof record.id === "string" ? record.id : undefined;
            if (id === undefined) return;
            if (context.emittedTerminalRequestIds.has(id) || context.pendingPermissions.has(id))
              return;
            const action = typeof record.action === "string" ? record.action : "unknown";
            const resources = Array.isArray(record.resources)
              ? record.resources.filter((entry): entry is string => typeof entry === "string")
              : [];
            const detail = [
              action.replaceAll("_", " "),
              ...resources.filter((entry) => entry !== "*"),
            ].join("\n");
            const pending: V2PendingPermission = {
              id,
              action,
              detail: detail.length > 0 ? detail : undefined,
            };
            // Full access already allows everything, but requests outside the
            // session ruleset (subagents, loop guards) still surface — answer
            // them without bothering the user.
            if (context.fullAccess) {
              const replied = yield* runtime
                .replyPermission({
                  baseUrl: context.serverUrl,
                  ...(context.serverPassword === undefined
                    ? {}
                    : { serverPassword: context.serverPassword }),
                  sessionId: context.openCodeSessionId,
                  requestId: id,
                  decision: "once",
                })
                .pipe(
                  Effect.mapError((cause) => toRequestError("session.permissionReply", cause)),
                  Effect.as(true),
                  Effect.orElseSucceed(() => false),
                );
              if (replied) {
                context.emittedTerminalRequestIds.add(id);
                return;
              }
            }
            context.pendingPermissions.set(id, pending);
            const base = yield* buildBase({
              threadId: context.session.threadId,
              ...(context.activeTurnId === undefined ? {} : { turnId: context.activeTurnId }),
              requestId: id,
              raw,
            });
            yield* emit({
              ...base,
              type: "request.opened",
              payload: {
                requestType: mapV2PermissionToRequestType(action),
                ...(pending.detail === undefined ? {} : { detail: pending.detail }),
                options: [
                  { decision: "accept", label: "Allow once" },
                  { decision: "acceptForSession", label: "Allow for session" },
                  { decision: "decline", label: "Deny" },
                ],
              },
            });
          } else if (v2Type === "permission.replied" && typeof data === "object" && data !== null) {
            const record = data as Record<string, unknown>;
            const id =
              typeof record.id === "string"
                ? record.id
                : typeof record.requestID === "string"
                  ? record.requestID
                  : undefined;
            if (id === undefined || context.emittedTerminalRequestIds.has(id)) return;
            const pending = context.pendingPermissions.get(id);
            context.pendingPermissions.delete(id);
            context.emittedTerminalRequestIds.add(id);
            const reply =
              typeof record.reply === "string"
                ? record.reply
                : typeof record.decision === "string"
                  ? record.decision
                  : undefined;
            const base = yield* buildBase({
              threadId: context.session.threadId,
              ...(context.activeTurnId === undefined ? {} : { turnId: context.activeTurnId }),
              requestId: id,
              raw,
            });
            yield* emit({
              ...base,
              type: "request.resolved",
              payload: {
                requestType: pending ? mapV2PermissionToRequestType(pending.action) : "unknown",
                ...(reply === undefined
                  ? {}
                  : {
                      decision:
                        reply === "once"
                          ? "accept"
                          : reply === "always"
                            ? "acceptForSession"
                            : "decline",
                    }),
              },
            });
          }
          return;
        }
        if (kind === "form" && typeof data === "object" && data !== null) {
          const record = data as Record<string, unknown>;
          const formData =
            record.fields !== undefined
              ? data
              : typeof record.form === "object" && record.form !== null
                ? record.form
                : data;
          const formRecord = formData as Record<string, unknown>;
          const id = typeof formRecord.id === "string" ? formRecord.id : undefined;
          if (id === undefined) return;
          if (v2Type === "form.created") {
            if (context.emittedTerminalRequestIds.has(id) || context.pendingForms.has(id)) return;
            const normalized = normalizeV2FormFields(formData);
            if (normalized.questions.length === 0) return;
            context.pendingForms.set(id, { id, fields: normalized.fields });
            const base = yield* buildBase({
              threadId: context.session.threadId,
              ...(context.activeTurnId === undefined ? {} : { turnId: context.activeTurnId }),
              requestId: id,
              raw,
            });
            yield* emit({
              ...base,
              type: "user-input.requested",
              payload: { questions: normalized.questions },
            });
          } else {
            // External settlement (answered/cancelled elsewhere) — close our
            // dialog tracking so a later reply cannot reopen it.
            if (context.emittedTerminalRequestIds.has(id)) return;
            const hadPending = context.pendingForms.delete(id);
            if (!hadPending) return;
            context.emittedTerminalRequestIds.add(id);
            const base = yield* buildBase({
              threadId: context.session.threadId,
              ...(context.activeTurnId === undefined ? {} : { turnId: context.activeTurnId }),
              requestId: id,
              raw,
            });
            yield* emit({ ...base, type: "user-input.resolved", payload: { answers: {} } });
          }
          return;
        }
        if (kind === "tool") {
          const lifecycle = mapOpenCodeV2ToolLifecycle(v2Type);
          if (lifecycle === undefined) return;
          const isStart = lifecycle === "started";
          const tool = normalizeOpenCodeV2ToolEvent(data);
          const itemType = mapToolNameToItemType(tool.toolName);
          const ordinal =
            typeof data === "object" &&
            data !== null &&
            typeof (data as Record<string, unknown>).ordinal === "number"
              ? String((data as Record<string, number>).ordinal)
              : "0";
          const itemId = tool.callId ?? `${context.openCodeSessionId}:${tool.toolName}:${ordinal}`;
          const failed =
            !isStart &&
            (v2Type === "session.tool.failed" ||
              v2Type === "session.tool.error" ||
              tool.error !== undefined);
          const detail = tool.error ?? tool.output ?? describeOpenCodeV2ToolInput(tool.input);
          const payloadData: Record<string, unknown> = {
            tool: tool.toolName,
            ...(tool.callId === undefined ? {} : { toolCallId: tool.callId }),
            ...(tool.command === undefined ? {} : { command: tool.command }),
            ...(tool.input === undefined ? {} : { input: tool.input }),
            ...(tool.output === undefined ? {} : { result: tool.output }),
            ...(tool.error === undefined ? {} : { error: tool.error }),
            item: {
              ...(tool.command === undefined ? {} : { command: tool.command }),
              ...(tool.input === undefined ? {} : { input: tool.input }),
              ...(tool.output === undefined ? {} : { result: tool.output }),
            },
          };
          const base = yield* buildBase({
            threadId: context.session.threadId,
            ...(context.activeTurnId === undefined ? {} : { turnId: context.activeTurnId }),
            itemId,
            raw,
          });
          yield* emit({
            ...base,
            type: isStart ? "item.started" : "item.completed",
            payload: {
              itemType,
              status: isStart ? "inProgress" : failed ? "failed" : "completed",
              title: tool.title,
              ...(detail === undefined ? {} : { detail }),
              data: payloadData,
            },
          });
          // v2 has no `todo.updated` bus event like v1 — the same tools carry
          // the list in their input, so refresh the plan panel from it.
          if (!isStart && isV2TodoTool(tool.toolName)) {
            const plan = v2TodoPlanFromToolInput(tool.input);
            if (plan !== undefined) {
              const planBase = yield* buildBase({
                threadId: context.session.threadId,
                ...(context.activeTurnId === undefined ? {} : { turnId: context.activeTurnId }),
                raw,
              });
              yield* emit({
                ...planBase,
                type: "turn.plan.updated",
                payload: { plan: [...plan] },
              });
            }
          }
          return;
        }
      });

    const subscribeSessionEvents = (context: V2SessionContext): Effect.Effect<void> =>
      Effect.gen(function* () {
        const bufferRef = yield* Ref.make("");
        const authHeader =
          context.serverPassword === undefined
            ? {}
            : {
                Authorization: `Basic ${Buffer.from(`opencode:${context.serverPassword}`, "utf8").toString("base64")}`,
              };
        const request = HttpClientRequest.get(`${context.serverUrl}/api/event`).pipe(
          HttpClientRequest.setHeaders({ Accept: "text/event-stream", ...authHeader }),
        );
        const processChunk = (chunk: string): Effect.Effect<void> =>
          Effect.gen(function* () {
            const buffered = `${yield* Ref.get(bufferRef)}${chunk}`;
            const frames = buffered.split("\n\n");
            yield* Ref.set(bufferRef, frames.pop() ?? "");
            for (const frame of frames) {
              const dataLines = frame
                .split("\n")
                .filter((line) => line.startsWith("data:"))
                .map((line) => line.slice(5).trim())
                .filter((line) => line.length > 0);
              for (const line of dataLines) {
                const decoded = yield* decodeSseLine(line).pipe(
                  Effect.orElseSucceed(() => undefined),
                );
                if (decoded === undefined) continue;
                yield* handleSseEvent(context, decoded.type, decoded.data, decoded).pipe(
                  Effect.orElseSucceed(() => undefined),
                );
              }
            }
          });
        const pump = Effect.scoped(
          httpClient
            .execute(request)
            .pipe(
              Effect.flatMap(
                (
                  response,
                ): Effect.Effect<
                  void,
                  HttpClientError.HttpClientError | ProviderAdapterRequestError
                > =>
                  response.status === 200
                    ? response.stream.pipe(Stream.decodeText(), Stream.runForEach(processChunk))
                    : Effect.fail(toRequestError("event.subscribe", `HTTP ${response.status}`)),
              ),
            ),
        );
        yield* pump.pipe(
          Effect.orElseSucceed(() => undefined),
          Effect.forkIn(context.sessionScope),
        );
      });

    yield* Effect.addFinalizer(() =>
      Effect.gen(function* () {
        const contexts = [...sessions.values()];
        sessions.clear();
        yield* Effect.forEach(
          contexts,
          (context) =>
            Effect.gen(function* () {
              yield* Ref.set(context.stopped, true);
              yield* Scope.close(context.sessionScope, Effect.void as never).pipe(Effect.ignore);
            }),
          { concurrency: "unbounded", discard: true },
        );
      }).pipe(Effect.ensuring(Queue.shutdown(runtimeEvents))),
    );

    const startSession = (
      input: ProviderSessionStartInput,
    ): Effect.Effect<ProviderSession, ProviderAdapterError> =>
      Effect.gen(function* () {
        const directory = input.cwd ?? serverConfig.cwd;
        const resumeSessionId = parseResume(input.resumeCursor)?.sessionId;
        const selectedAgent = input.modelSelection
          ? getModelSelectionStringOptionValue(input.modelSelection, "agent")
          : undefined;
        const selectedVariant = input.modelSelection
          ? getModelSelectionStringOptionValue(input.modelSelection, "variant")
          : undefined;
        const existing = sessions.get(input.threadId);
        if (existing !== undefined) {
          yield* Ref.set(existing.stopped, true);
          yield* Scope.close(existing.sessionScope, Effect.void as never).pipe(Effect.ignore);
          sessions.delete(input.threadId);
        }
        const sessionScope = yield* Scope.make();
        const serverPassword = settings.serverPassword.trim();
        // The scope owns the spawned server child (via the Scope finalizer
        // installed in `connectToServer`). Close it on failure so a failed
        // resume/probe cannot orphan a server process.
        const startExit = yield* Effect.gen(function* () {
          const server = yield* runtime
            .connectToServer({
              binaryPath: settings.binaryPath,
              directory,
              ...(settings.serverUrl.trim().length > 0 ? { serverUrl: settings.serverUrl } : {}),
              ...(serverPassword.length > 0 ? { serverPassword } : {}),
              environment: options?.environment ?? process.env,
            })
            .pipe(
              Effect.mapError((cause) => toRequestError("connectToServer", cause)),
              Effect.provideService(Scope.Scope, sessionScope),
            );
          const created = yield* Effect.gen(function* () {
            const auth =
              server.serverPassword === undefined
                ? { baseUrl: server.url }
                : { baseUrl: server.url, serverPassword: server.serverPassword };
            if (resumeSessionId !== undefined) {
              const adopted = yield* runtime
                .getSessionInfo({ ...auth, sessionId: resumeSessionId })
                .pipe(
                  Effect.map((info) => ({ id: resumeSessionId, directory: info.directory })),
                  Effect.catchIf(
                    (cause) => isOpenCodeV2NotFound(cause),
                    () => Effect.void,
                  ),
                  Effect.mapError((cause) => toRequestError("session.get", cause)),
                );
              if (adopted !== undefined) {
                // Reuse in place only when the session still matches the
                // requested cwd. The v2 `fork` endpoint cannot retarget the
                // directory (verified against 2.0.15: it silently keeps the
                // parent's), so unlike v1 there is no fork-into-directory
                // fallback — a moved thread starts fresh in the right
                // directory rather than running in the wrong one.
                const reusable =
                  adopted.directory === undefined ||
                  (yield* sameDirectory(adopted.directory, directory))
                    ? adopted
                    : undefined;
                if (reusable !== undefined) {
                  // Resume skips `session.create`, so re-assert the ruleset —
                  // a runtime-mode change would otherwise leave the session on
                  // its original permissions.
                  yield* runtime
                    .updateSessionPermissions({
                      ...auth,
                      sessionId: reusable.id,
                      permissions: buildV2PermissionRules(input.runtimeMode),
                    })
                    .pipe(Effect.mapError((cause) => toRequestError("session.update", cause)));
                  return reusable;
                }
                yield* Effect.logWarning(
                  `OpenCode V2 session '${resumeSessionId}' was created under a different working directory; starting a fresh session in '${directory}' to avoid running in the wrong directory.`,
                ).pipe(Effect.ignore);
              } else {
                yield* Effect.logWarning(
                  `OpenCode V2 session '${resumeSessionId}' no longer exists; starting a fresh session.`,
                ).pipe(Effect.ignore);
              }
            }
            return yield* runtime
              .createSession({
                baseUrl: server.url,
                ...(server.serverPassword === undefined
                  ? {}
                  : { serverPassword: server.serverPassword }),
                directory,
                ...(input.title ? { title: input.title } : {}),
                ...(input.modelSelection?.model ? { modelSlug: input.modelSelection.model } : {}),
                ...(selectedVariant ? { variant: selectedVariant } : {}),
                ...(selectedAgent ? { agent: selectedAgent } : {}),
                permissions: buildV2PermissionRules(input.runtimeMode),
              })
              .pipe(Effect.mapError((cause) => toRequestError("session.create", cause)));
          });
          const createdAt = yield* nowIso;
          const session: ProviderSession = {
            provider: PROVIDER,
            providerInstanceId: boundInstanceId,
            status: "ready",
            runtimeMode: input.runtimeMode,
            cwd: directory,
            ...(input.modelSelection?.model ? { model: input.modelSelection.model } : {}),
            threadId: input.threadId,
            // ProviderService persists this cursor and feeds it back into
            // `startSession` after the in-memory session is lost (reaper /
            // restart), so follow-ups continue the same conversation.
            resumeCursor: {
              schemaVersion: RESUME_VERSION,
              sessionId: created.id,
            },
            createdAt,
            updatedAt: createdAt,
          };
          const context: V2SessionContext = {
            session,
            openCodeSessionId: created.id,
            serverUrl: server.url,
            ...(server.serverPassword === undefined
              ? {}
              : { serverPassword: server.serverPassword }),
            directory,
            runtimeMode: input.runtimeMode,
            modelSlug: input.modelSelection?.model,
            variant: selectedVariant,
            agent: selectedAgent,
            fullAccess: input.runtimeMode === "full-access",
            pendingPermissions: new Map(),
            pendingForms: new Map(),
            emittedTerminalRequestIds: new Set(),
            activeTurnId: undefined,
            inputTokens: 0,
            outputTokens: 0,
            reasoningTokens: 0,
            cachedInputTokens: 0,
            stopped: yield* Ref.make(false),
            sessionScope,
          };
          sessions.set(input.threadId, context);
          yield* subscribeSessionEvents(context);
          yield* emit({
            ...(yield* buildBase({ threadId: input.threadId })),
            type: "session.started",
            payload: { message: "OpenCode V2 session started" },
          });
          yield* emit({
            ...(yield* buildBase({ threadId: input.threadId })),
            type: "thread.started",
            payload: { providerThreadId: context.openCodeSessionId },
          });
          return session;
        }).pipe(Effect.exit);
        if (Exit.isFailure(startExit)) {
          sessions.delete(input.threadId);
          yield* Scope.close(sessionScope, Exit.void as never).pipe(Effect.ignore);
          return yield* Effect.failCause(startExit.cause);
        }
        return startExit.value;
      }).pipe(Effect.mapError((cause) => wrapAdapterError(input.threadId, cause)));

    const sendTurn = (
      input: ProviderSendTurnInput,
    ): Effect.Effect<
      { readonly threadId: ThreadId; readonly turnId: TurnId },
      ProviderAdapterError
    > =>
      Effect.gen(function* () {
        const context = yield* ensureContext(input.threadId);
        if (
          input.modelSelection !== undefined &&
          input.modelSelection.instanceId !== boundInstanceId
        ) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "sendTurn",
            issue: `OpenCode V2 model selection is bound to instance '${boundInstanceId}', expected '${input.modelSelection.instanceId}'.`,
          });
        }
        const text = input.input?.trim();
        if (!text) {
          return yield* new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "session.prompt",
            detail: "Empty turn input is not supported by the OpenCode V2 adapter.",
          });
        }
        const turnId = TurnId.make(yield* crypto.randomUUIDv4);
        context.activeTurnId = turnId;
        context.inputTokens = 0;
        context.outputTokens = 0;
        context.reasoningTokens = 0;
        context.cachedInputTokens = 0;
        context.session = {
          ...context.session,
          status: "running",
          activeTurnId: turnId,
          updatedAt: yield* nowIso,
        };
        const requestedModel = input.modelSelection?.model?.trim();
        const requestedVariant = input.modelSelection
          ? getModelSelectionStringOptionValue(input.modelSelection, "variant")
          : undefined;
        if (
          requestedModel &&
          (requestedModel !== context.modelSlug ||
            (requestedVariant ?? undefined) !== context.variant)
        ) {
          yield* runtime
            .switchSessionModel({
              baseUrl: context.serverUrl,
              ...(context.serverPassword === undefined
                ? {}
                : { serverPassword: context.serverPassword }),
              sessionId: context.openCodeSessionId,
              modelSlug: requestedModel,
              ...(requestedVariant ? { variant: requestedVariant } : {}),
            })
            .pipe(Effect.mapError((cause) => toRequestError("session.switchModel", cause)));
          context.modelSlug = requestedModel;
          context.variant = requestedVariant ?? undefined;
          context.session = { ...context.session, model: requestedModel };
        }
        const requestedAgent = input.modelSelection
          ? getModelSelectionStringOptionValue(input.modelSelection, "agent")
          : undefined;
        if (requestedAgent && requestedAgent !== context.agent) {
          yield* runtime
            .switchSessionAgent({
              baseUrl: context.serverUrl,
              ...(context.serverPassword === undefined
                ? {}
                : { serverPassword: context.serverPassword }),
              sessionId: context.openCodeSessionId,
              agent: requestedAgent,
            })
            .pipe(Effect.mapError((cause) => toRequestError("session.switchAgent", cause)));
          context.agent = requestedAgent;
        }
        const files = toV2PromptFiles({
          attachments: input.attachments,
          attachmentsDir: serverConfig.attachmentsDir,
        });
        // Slash commands run through the dedicated endpoint (mirrors v1's
        // native command routing); anything the server doesn't know falls
        // back to a plain prompt below.
        const commandMatch = text.match(/^\/([^\s/]+)(?:\s+([\s\S]*))?$/);
        const auth =
          context.serverPassword === undefined
            ? { baseUrl: context.serverUrl }
            : { baseUrl: context.serverUrl, serverPassword: context.serverPassword };
        if (commandMatch) {
          const ran = yield* runtime
            .runSessionCommand({
              ...auth,
              sessionId: context.openCodeSessionId,
              name: commandMatch[1]!,
              text: commandMatch[2] ?? "",
            })
            .pipe(
              Effect.mapError((cause) => toRequestError("session.command", cause)),
              Effect.as(true),
              Effect.orElseSucceed(() => false),
            );
          if (ran) {
            const base = yield* buildBase({ threadId: context.session.threadId, turnId });
            yield* emit({ ...base, type: "turn.started", payload: {} });
            return { threadId: input.threadId, turnId };
          }
        }
        yield* runtime
          .promptSession({
            ...auth,
            sessionId: context.openCodeSessionId,
            text,
            ...(files.length === 0 ? {} : { files }),
          })
          .pipe(Effect.mapError((cause) => toRequestError("session.prompt", cause)));
        const base = yield* buildBase({ threadId: context.session.threadId, turnId });
        yield* emit({ ...base, type: "turn.started", payload: {} });
        return { threadId: input.threadId, turnId };
      }).pipe(Effect.mapError((cause) => wrapAdapterError(input.threadId, cause)));

    const interruptTurn = (threadId: ThreadId): Effect.Effect<void, ProviderAdapterError> =>
      Effect.gen(function* () {
        const context = yield* ensureContext(threadId);
        yield* runtime
          .interruptSession({
            baseUrl: context.serverUrl,
            ...(context.serverPassword === undefined
              ? {}
              : { serverPassword: context.serverPassword }),
            sessionId: context.openCodeSessionId,
          })
          .pipe(Effect.mapError((cause) => toRequestError("session.interrupt", cause)));
        // Settle the turn here as well as on `execution.interrupted` so a
        // lost confirmation still stops the UI from spinning forever.
        const turnId = context.activeTurnId;
        yield* cancelPendingForms(context);
        if (turnId !== undefined) {
          yield* emitTurnAborted(context, turnId);
        }
      }).pipe(Effect.orElseSucceed(() => undefined));

    const stopSession = (threadId: ThreadId): Effect.Effect<void> =>
      Effect.gen(function* () {
        const context = sessions.get(threadId);
        if (context === undefined) return;
        sessions.delete(threadId);
        yield* Ref.set(context.stopped, true);
        yield* cancelPendingForms(context);
        // Best-effort interrupt so a running turn halts. The native session
        // itself is intentionally KEPT server-side so a later resumeCursor
        // can re-adopt it (mirrors v1, which aborts but never deletes).
        // Deleting here would turn every reaper sweep into permanent
        // amnesia: the persisted cursor would point at a destroyed session.
        yield* runtime
          .interruptSession({
            baseUrl: context.serverUrl,
            ...(context.serverPassword === undefined
              ? {}
              : { serverPassword: context.serverPassword }),
            sessionId: context.openCodeSessionId,
          })
          .pipe(Effect.ignore);
        yield* Scope.close(context.sessionScope, Effect.void as never).pipe(Effect.ignore);
      });

    const respondToRequest = (
      threadId: ThreadId,
      requestId: ApprovalRequestId,
      decision: ProviderApprovalDecision,
    ): Effect.Effect<void, ProviderAdapterError> =>
      Effect.gen(function* () {
        const context = yield* ensureContext(threadId);
        const pending = context.pendingPermissions.get(String(requestId));
        const reply = mapV2ApprovalDecision(decision);
        yield* runtime
          .replyPermission({
            baseUrl: context.serverUrl,
            ...(context.serverPassword === undefined
              ? {}
              : { serverPassword: context.serverPassword }),
            sessionId: context.openCodeSessionId,
            requestId: String(requestId),
            decision: reply,
          })
          .pipe(Effect.mapError((cause) => toRequestError("session.permissionReply", cause)));
        context.pendingPermissions.delete(String(requestId));
        if (context.emittedTerminalRequestIds.has(String(requestId))) return;
        context.emittedTerminalRequestIds.add(String(requestId));
        const base = yield* buildBase({
          threadId: context.session.threadId,
          ...(context.activeTurnId === undefined ? {} : { turnId: context.activeTurnId }),
          requestId: String(requestId),
        });
        yield* emit({
          ...base,
          type: "request.resolved",
          payload: {
            requestType: pending ? mapV2PermissionToRequestType(pending.action) : "unknown",
            decision:
              reply === "once" ? "accept" : reply === "always" ? "acceptForSession" : "decline",
          },
        });
      }).pipe(Effect.mapError((cause) => wrapAdapterError(threadId, cause)));

    const compactThread = (threadId: ThreadId): Effect.Effect<void, ProviderAdapterError> =>
      Effect.gen(function* () {
        const context = yield* ensureContext(threadId);
        const admitted = yield* runtime
          .compactSession({
            baseUrl: context.serverUrl,
            ...(context.serverPassword === undefined
              ? {}
              : { serverPassword: context.serverPassword }),
            sessionId: context.openCodeSessionId,
          })
          .pipe(Effect.mapError((cause) => toRequestError("session.compact", cause)));
        // The compaction runs async after admission; poll its message until
        // the summary lands, then report the compacted thread state the
        // ProviderService compaction waiter listens for.
        let done = false;
        for (let attempt = 0; attempt < 160 && !done; attempt += 1) {
          yield* Effect.sleep("3 seconds");
          const detail = yield* runtime
            .getMessage({
              baseUrl: context.serverUrl,
              ...(context.serverPassword === undefined
                ? {}
                : { serverPassword: context.serverPassword }),
              sessionId: context.openCodeSessionId,
              messageId: admitted.messageId,
            })
            .pipe(
              Effect.mapError((cause) => toRequestError("session.message", cause)),
              Effect.orElseSucceed(() => undefined),
            );
          if (
            detail !== undefined &&
            (detail.text.trim().length > 0 || detail.finish !== undefined)
          ) {
            done = true;
          }
        }
        const base = yield* buildBase({ threadId: context.session.threadId });
        if (!done) {
          yield* emit({
            ...base,
            type: "runtime.error",
            payload: {
              message: "OpenCode V2 compaction did not finish in time.",
              class: "transport_error",
            },
          });
          return;
        }
        yield* emit({ ...base, type: "thread.state.changed", payload: { state: "compacted" } });
      }).pipe(Effect.mapError((cause) => wrapAdapterError(threadId, cause)));

    const readThread = (
      threadId: ThreadId,
    ): Effect.Effect<ProviderThreadSnapshot, ProviderAdapterError> =>
      Effect.gen(function* () {
        const context = yield* ensureContext(threadId);
        const messages = yield* runtime
          .listMessages({
            baseUrl: context.serverUrl,
            ...(context.serverPassword === undefined
              ? {}
              : { serverPassword: context.serverPassword }),
            sessionId: context.openCodeSessionId,
          })
          .pipe(Effect.mapError((cause) => toRequestError("session.messages", cause)));
        const turns: Array<{ readonly id: TurnId; readonly items: ReadonlyArray<unknown> }> = [];
        let current: { readonly id: TurnId; readonly items: Array<unknown> } | undefined;
        for (const message of messages) {
          if (message.type === "user") {
            current = { id: TurnId.make(`turn-${message.id}`), items: [] };
            turns.push(current);
          }
          const item = { messageId: message.id, role: message.type };
          if (current === undefined) {
            current = { id: TurnId.make(`turn-${message.id}`), items: [] };
            turns.push(current);
          }
          current.items.push(item);
        }
        return { threadId, turns };
      }).pipe(Effect.mapError((cause) => wrapAdapterError(threadId, cause)));

    const cancelPendingForms = (context: V2SessionContext): Effect.Effect<void> =>
      Effect.forEach([...context.pendingForms.keys()], (formId) =>
        runtime
          .cancelForm({
            baseUrl: context.serverUrl,
            ...(context.serverPassword === undefined
              ? {}
              : { serverPassword: context.serverPassword }),
            sessionId: context.openCodeSessionId,
            formId,
          })
          .pipe(Effect.ignore),
      ).pipe(Effect.asVoid);

    const respondToUserInput = (
      threadId: ThreadId,
      requestId: ApprovalRequestId,
      answers: ProviderUserInputAnswers,
    ): Effect.Effect<void, ProviderAdapterError> =>
      Effect.gen(function* () {
        const context = yield* ensureContext(threadId);
        const pending = context.pendingForms.get(String(requestId));
        if (pending === undefined) {
          return yield* new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "respondToUserInput",
            detail: `Unknown pending question: ${String(requestId)}.`,
          });
        }
        const answer: Record<string, string | number | boolean | ReadonlyArray<string>> = {};
        for (const field of pending.fields) {
          const raw = answers[field.questionId] ?? answers[field.key];
          if (Array.isArray(raw)) {
            const values = raw.filter((entry): entry is string => typeof entry === "string");
            answer[field.key] = field.multi ? values : (values[0] ?? "");
          } else if (typeof raw === "string") {
            answer[field.key] = field.multi ? (raw ? [raw] : []) : raw;
          } else if (typeof raw === "number" || typeof raw === "boolean") {
            answer[field.key] = raw;
          } else {
            answer[field.key] = field.multi ? [] : "";
          }
        }
        yield* runtime
          .replyForm({
            baseUrl: context.serverUrl,
            ...(context.serverPassword === undefined
              ? {}
              : { serverPassword: context.serverPassword }),
            sessionId: context.openCodeSessionId,
            formId: pending.id,
            answer,
          })
          .pipe(Effect.mapError((cause) => toRequestError("session.formReply", cause)));
        context.pendingForms.delete(pending.id);
        if (context.emittedTerminalRequestIds.has(pending.id)) return;
        context.emittedTerminalRequestIds.add(pending.id);
        const base = yield* buildBase({
          threadId: context.session.threadId,
          ...(context.activeTurnId === undefined ? {} : { turnId: context.activeTurnId }),
          requestId: pending.id,
          raw: answers,
        });
        yield* emit({ ...base, type: "user-input.resolved", payload: { answers } });
      }).pipe(Effect.mapError((cause) => wrapAdapterError(threadId, cause)));

    const rollbackThread: ProviderAdapterShape<ProviderAdapterError>["rollbackThread"] = Effect.fn(
      "rollbackThread",
    )(function* (threadId: ThreadId, numTurns: number) {
      if (!Number.isInteger(numTurns) || numTurns < 1) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "rollbackThread",
          issue: "numTurns must be an integer >= 1.",
        });
      }
      const context = yield* ensureContext(threadId);
      const auth =
        context.serverPassword === undefined
          ? { baseUrl: context.serverUrl }
          : { baseUrl: context.serverUrl, serverPassword: context.serverPassword };
      const messages = yield* runtime
        .listMessages({ ...auth, sessionId: context.openCodeSessionId })
        .pipe(Effect.mapError((cause) => toRequestError("session.messages", cause)));
      const sessionInfo = yield* runtime
        .getSessionInfo({ ...auth, sessionId: context.openCodeSessionId })
        .pipe(
          Effect.mapError((cause) => toRequestError("session.get", cause)),
          Effect.orElseSucceed((): { readonly revertMessageId?: string } => ({})),
        );
      const boundary = findV2RollbackBoundary(messages, numTurns, sessionInfo.revertMessageId);
      if (boundary === undefined) {
        return yield* readThread(threadId);
      }
      // Fork only the retained conversation, like v1: no native revert,
      // so T3 alone decides whether filesystem changes survive.
      const forked = yield* runtime
        .forkSession({
          ...auth,
          sessionId: context.openCodeSessionId,
          beforeMessageId: boundary.beforeMessageId,
        })
        .pipe(Effect.mapError((cause) => toRequestError("session.fork", cause)));
      const forkedMessages = yield* runtime
        .listMessages({ ...auth, sessionId: forked.id })
        .pipe(Effect.mapError((cause) => toRequestError("session.messages", cause)));
      if (forkedMessages.some((message) => message.id === boundary.beforeMessageId)) {
        return yield* toRequestError(
          "session.fork",
          "OpenCode did not preserve the requested rewind boundary.",
        );
      }
      yield* runtime
        .updateSessionPermissions({
          ...auth,
          sessionId: forked.id,
          permissions: buildV2PermissionRules(context.runtimeMode),
        })
        .pipe(Effect.mapError((cause) => toRequestError("session.update", cause)));
      context.openCodeSessionId = forked.id;
      context.pendingPermissions.clear();
      context.pendingForms.clear();
      context.emittedTerminalRequestIds.clear();
      context.activeTurnId = undefined;
      context.inputTokens = 0;
      context.outputTokens = 0;
      context.reasoningTokens = 0;
      context.cachedInputTokens = 0;
      context.session = {
        ...context.session,
        resumeCursor: { schemaVersion: RESUME_VERSION, sessionId: forked.id },
        updatedAt: yield* nowIso,
      };
      yield* emit({
        ...(yield* buildBase({ threadId })),
        type: "thread.started",
        payload: { providerThreadId: forked.id },
      });
      return yield* readThread(threadId);
    });

    const adapter: ProviderAdapterShape<ProviderAdapterError> = {
      provider: PROVIDER,
      capabilities: { sessionModelSwitch: "in-session", supportsConversationRollback: true },
      startSession,
      sendTurn,
      interruptTurn: (threadId) => interruptTurn(threadId),
      respondToRequest: (threadId, requestId, decision) =>
        respondToRequest(threadId, requestId, decision),
      compaction: { type: "native", start: (threadId) => compactThread(threadId) },
      respondToUserInput: (threadId, requestId, answers) =>
        respondToUserInput(threadId, requestId, answers),
      stopSession: (threadId) => stopSession(threadId),
      listSessions: () => Effect.succeed([...sessions.values()].map((context) => context.session)),
      hasSession: (threadId) => Effect.succeed(sessions.has(threadId)),
      readThread,
      rollbackThread,
      stopAll: () =>
        Effect.forEach([...sessions.keys()], (threadId) => stopSession(threadId), {
          discard: true,
        }),
      streamEvents: Stream.fromQueue(runtimeEvents),
    };
    return adapter;
  });
}
