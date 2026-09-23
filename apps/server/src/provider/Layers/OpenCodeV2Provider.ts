/**
 * OpenCodeV2Provider — status probe + snapshot shaping for the standalone
 * OpenCode v2 driver.
 *
 * Independent from v1 (`OpenCodeProvider.ts`): v2 inventory comes from
 * `/api/model`, `/api/agent`, `/api/provider` with `{data}` envelopes.
 *
 * @module provider/Layers/OpenCodeV2Provider
 */
import {
  type ModelCapabilities,
  type OpenCodeV2Settings,
  type ServerProviderModel,
  type ServerProviderSkill,
  type ServerProviderSlashCommand,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Data from "effect/Data";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import { ChildProcessSpawner } from "effect/unstable/process";

import { createModelCapabilities } from "@t3tools/shared/model";
import { compareSemverVersions } from "@t3tools/shared/semver";
import {
  buildServerProvider,
  COMPACT_SLASH_COMMAND,
  parseGenericCliVersion,
  providerModelsFromSettings,
  type ServerProviderDraft,
} from "../providerSnapshot.ts";
import {
  MINIMUM_OPENCODE_V2_VERSION,
  OpenCodeV2Runtime,
  openCodeV2RuntimeErrorDetail,
  runOpenCodeV2Command,
  type OpenCodeV2AgentInfo,
  type OpenCodeV2CommandInfo,
  type OpenCodeV2ModelInfo,
  type OpenCodeV2SkillInfo,
} from "../opencodeV2Runtime.ts";

const OPENCODE_V2_PRESENTATION = {
  displayName: "OpenCode V2",
  showInteractionModeToggle: false,
} as const;
const VERSION_PROBE_TIMEOUT = "4 seconds";

class OpenCodeV2ProbeError extends Data.TaggedError("OpenCodeV2ProbeError")<{
  readonly cause?: unknown;
  readonly detail: string;
}> {}

const DEFAULT_MODEL_CAPABILITIES: ModelCapabilities = createModelCapabilities({
  optionDescriptors: [],
});

function titleCaseSlug(value: string): string {
  const segments: Array<string> = [];
  for (const segment of value.split(/[-_/]+/)) {
    if (segment.length > 0) {
      segments.push(segment.charAt(0).toUpperCase() + segment.slice(1));
    }
  }
  return segments.join(" ");
}

// Agents the server lists but never offers as a chat agent (mirrors v1's
// KNOWN_HIDDEN_AGENTS in opencodeRuntime.ts, which the v2 `/api/agent`
// response does not flag).
const KNOWN_HIDDEN_V2_AGENTS = new Set(["compaction", "summary", "title"]);

function inferDefaultV2Variant(
  providerID: string,
  variants: ReadonlyArray<string>,
): string | undefined {
  if (variants.length === 1) {
    return variants[0];
  }
  if (providerID === "anthropic" || providerID.startsWith("google")) {
    return variants.includes("high") ? "high" : undefined;
  }
  if (providerID === "openai" || providerID === "opencode") {
    return variants.includes("medium") ? "medium" : variants.includes("high") ? "high" : undefined;
  }
  return undefined;
}

export function openCodeV2CapabilitiesForModel(input: {
  readonly providerID: string;
  readonly variants: ReadonlyArray<string>;
  readonly agents: ReadonlyArray<OpenCodeV2AgentInfo>;
}): ModelCapabilities {
  // When a model advertises no variants, synthesize the standard reasoning
  // levels so the composer still offers a Reasoning selector, like v1.
  const variantValues =
    input.variants.length > 0 ? input.variants : ["low", "medium", "high", "xhigh"];
  const defaultVariant = inferDefaultV2Variant(input.providerID, variantValues);
  const variantOptions = variantValues.map((value) =>
    defaultVariant === value
      ? { id: value, label: titleCaseSlug(value), isDefault: true as const }
      : { id: value, label: titleCaseSlug(value) },
  );
  const primary = input.agents.filter(
    (agent) =>
      !KNOWN_HIDDEN_V2_AGENTS.has(agent.id) && (agent.mode === "primary" || agent.mode === "all"),
  );
  const defaultAgent = primary.find((agent) => agent.id === "build")?.id ?? primary[0]?.id;
  const agentOptions = primary.map((agent) =>
    defaultAgent === agent.id
      ? { id: agent.id, label: titleCaseSlug(agent.id), isDefault: true as const }
      : { id: agent.id, label: titleCaseSlug(agent.id) },
  );
  if (variantOptions.length === 0 && agentOptions.length === 0) {
    return DEFAULT_MODEL_CAPABILITIES;
  }
  return createModelCapabilities({
    optionDescriptors: [
      ...(variantOptions.length > 0
        ? [
            {
              id: "variant",
              label: "Reasoning",
              type: "select" as const,
              options: variantOptions,
              ...(defaultVariant ? { currentValue: defaultVariant } : {}),
            },
          ]
        : []),
      ...(agentOptions.length > 0
        ? [
            {
              id: "agent",
              label: "Agent",
              type: "select" as const,
              options: agentOptions,
              ...(defaultAgent ? { currentValue: defaultAgent } : {}),
            },
          ]
        : []),
    ],
  });
}

function toServerModels(
  models: ReadonlyArray<OpenCodeV2ModelInfo>,
  agents: ReadonlyArray<OpenCodeV2AgentInfo>,
  defaultModelId: string | undefined,
): ReadonlyArray<ServerProviderModel> {
  return models
    .map((model) => ({
      slug: model.id,
      name: model.name,
      subProvider: model.providerID,
      isCustom: false as const,
      ...(defaultModelId !== undefined && model.id === defaultModelId
        ? { isDefault: true as const }
        : {}),
      capabilities: openCodeV2CapabilitiesForModel({
        providerID: model.providerID,
        variants: model.variants,
        agents,
      }),
    }))
    .toSorted((left, right) => left.name.localeCompare(right.name));
}

function trimOptional(value: string | null | undefined): string | undefined {
  const trimmed = value?.trim();
  return trimmed && trimmed.length > 0 ? trimmed : undefined;
}

export function openCodeV2SkillsToServerProviderSkills(
  input: ReadonlyArray<OpenCodeV2SkillInfo> | undefined,
): ReadonlyArray<ServerProviderSkill> {
  const skills: ServerProviderSkill[] = [];
  for (const skill of input ?? []) {
    const name = trimOptional(skill.name);
    const path = trimOptional(skill.path);
    if (!name || !path) continue;
    const description = trimOptional(skill.description);
    skills.push({
      name,
      path,
      enabled: true,
      ...(description ? { description, shortDescription: description } : {}),
    });
  }
  return skills.toSorted((left, right) => left.name.localeCompare(right.name));
}

export function openCodeV2CommandsToServerProviderSlashCommands(
  input: ReadonlyArray<OpenCodeV2CommandInfo> | undefined,
): ReadonlyArray<ServerProviderSlashCommand> {
  const commands: ServerProviderSlashCommand[] = [COMPACT_SLASH_COMMAND];
  const names = new Set([COMPACT_SLASH_COMMAND.name]);
  for (const command of input ?? []) {
    const name = trimOptional(command.name);
    if (!name || names.has(name)) continue;
    names.add(name);
    const description = trimOptional(command.description);
    commands.push({ name, ...(description ? { description } : {}) });
  }
  return commands;
}

export const makePendingOpenCodeV2Provider = (
  settings: OpenCodeV2Settings,
): Effect.Effect<ServerProviderDraft> =>
  Effect.gen(function* () {
    const checkedAt = yield* Effect.map(DateTime.now, DateTime.formatIso);
    const models = providerModelsFromSettings(
      [],
      settings.customModels,
      DEFAULT_MODEL_CAPABILITIES,
    );
    if (!settings.enabled) {
      return buildServerProvider({
        presentation: OPENCODE_V2_PRESENTATION,
        enabled: false,
        checkedAt,
        models,
        probe: {
          installed: false,
          version: null,
          status: "warning",
          auth: { status: "unknown" },
          message: "OpenCode V2 is disabled in T3 Code settings.",
        },
      });
    }
    return buildServerProvider({
      presentation: OPENCODE_V2_PRESENTATION,
      enabled: true,
      checkedAt,
      models,
      probe: {
        installed: false,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "OpenCode V2 provider status has not been checked in this session yet.",
      },
    });
  });

export const checkOpenCodeV2ProviderStatus = Effect.fn("checkOpenCodeV2ProviderStatus")(function* (
  settings: OpenCodeV2Settings,
  cwd: string,
  environment?: NodeJS.ProcessEnv,
): Effect.fn.Return<
  ServerProviderDraft,
  never,
  OpenCodeV2Runtime | ChildProcessSpawner.ChildProcessSpawner
> {
  const runtime = yield* OpenCodeV2Runtime;
  const resolvedEnvironment = environment ?? process.env;
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  const customModels = settings.customModels;
  const isExternalServer = settings.serverUrl.trim().length > 0;

  const fallbackModels = providerModelsFromSettings([], customModels, DEFAULT_MODEL_CAPABILITIES);
  const fallback = (cause: unknown, version: string | null = null): ServerProviderDraft => {
    const detail = cause instanceof Error ? cause.message : String(cause);
    return buildServerProvider({
      presentation: OPENCODE_V2_PRESENTATION,
      enabled: settings.enabled,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: true,
        version,
        status: "error",
        auth: { status: "unknown" },
        message: detail.length > 0 ? detail : "Failed to connect to the OpenCode v2 server.",
      },
    });
  };

  if (!settings.enabled) {
    return buildServerProvider({
      presentation: OPENCODE_V2_PRESENTATION,
      enabled: false,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: false,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "OpenCode V2 is disabled in T3 Code settings.",
      },
    });
  }

  let version: string | null = null;
  if (!isExternalServer) {
    const versionResult = yield* runOpenCodeV2Command({
      binaryPath: settings.binaryPath,
      args: ["--version"],
      environment: resolvedEnvironment,
    }).pipe(
      Effect.mapError(
        (cause) => new OpenCodeV2ProbeError({ cause, detail: openCodeV2RuntimeErrorDetail(cause) }),
      ),
      Effect.timeoutOption(VERSION_PROBE_TIMEOUT),
      Effect.flatMap((option) =>
        option._tag === "None"
          ? Effect.fail(new OpenCodeV2ProbeError({ detail: "Version probe timed out." }))
          : Effect.succeed(option.value),
      ),
      Effect.exit,
    );
    if (versionResult._tag === "Failure") {
      return fallback(Cause.squash(versionResult.cause));
    }
    version = parseGenericCliVersion(versionResult.value.stdout) ?? null;
    if (!version) {
      return fallback(
        new Error("Unable to determine OpenCode v2 version from `--version` output."),
      );
    }
    if (compareSemverVersions(version, MINIMUM_OPENCODE_V2_VERSION) < 0) {
      return buildServerProvider({
        presentation: OPENCODE_V2_PRESENTATION,
        enabled: settings.enabled,
        checkedAt,
        models: fallbackModels,
        probe: {
          installed: true,
          version,
          status: "error",
          auth: { status: "unknown" },
          message: `OpenCode v2 v${version} is too old. Upgrade to v${MINIMUM_OPENCODE_V2_VERSION} or newer.`,
        },
      });
    }
  }

  const serverPassword = settings.serverPassword.trim();
  const connectInput = {
    binaryPath: settings.binaryPath,
    directory: cwd,
    ...(isExternalServer ? { serverUrl: settings.serverUrl } : {}),
    ...(serverPassword.length > 0 ? { serverPassword } : {}),
    environment: resolvedEnvironment,
  };
  // A freshly spawned v2 server reports its providers immediately but
  // populates `/api/model` lazily (provider model discovery over the
  // network takes ~20s; observed 0 → 642 models across polls). Poll briefly
  // so a cold server probes as ready instead of model-less.
  const inventoryResult = yield* Effect.scoped(
    runtime.connectToServer(connectInput).pipe(
      Effect.flatMap((server) => {
        const auth =
          server.serverPassword === undefined ? {} : { serverPassword: server.serverPassword };
        const pollModels = Effect.gen(function* () {
          for (let attempt = 0; attempt < 15; attempt += 1) {
            const models = yield* runtime.listModels({ baseUrl: server.url, ...auth });
            if (models.length > 0) return models;
            yield* Effect.sleep("2 seconds");
          }
          return yield* runtime.listModels({ baseUrl: server.url, ...auth });
        });
        return Effect.all(
          {
            models: pollModels,
            agents: runtime.listAgents({ baseUrl: server.url, ...auth }),
            defaultModel: runtime.getDefaultModel({ baseUrl: server.url, ...auth, directory: cwd }),
            skills: runtime.listSkills({ baseUrl: server.url, ...auth, directory: cwd }),
            commands: runtime.listCommands({ baseUrl: server.url, ...auth }),
            version: Effect.succeed(server.version),
          },
          { concurrency: "unbounded" },
        );
      }),
    ),
  ).pipe(
    Effect.mapError(
      (cause) => new OpenCodeV2ProbeError({ cause, detail: openCodeV2RuntimeErrorDetail(cause) }),
    ),
    Effect.exit,
  );
  if (inventoryResult._tag === "Failure") {
    return fallback(Cause.squash(inventoryResult.cause), version);
  }

  const discovered = inventoryResult.value.models;
  version = inventoryResult.value.version;
  const models = providerModelsFromSettings(
    toServerModels(discovered, inventoryResult.value.agents, inventoryResult.value.defaultModel),
    customModels,
    DEFAULT_MODEL_CAPABILITIES,
  );
  return buildServerProvider({
    presentation: OPENCODE_V2_PRESENTATION,
    enabled: true,
    checkedAt,
    models,
    skills: openCodeV2SkillsToServerProviderSkills(inventoryResult.value.skills),
    slashCommands: openCodeV2CommandsToServerProviderSlashCommands(inventoryResult.value.commands),
    probe: {
      installed: true,
      version,
      status: discovered.length > 0 ? "ready" : "warning",
      auth: {
        status: discovered.length > 0 ? "authenticated" : "unknown",
        type: "opencodeV2",
      },
      message:
        discovered.length > 0
          ? `${discovered.length} models available through OpenCode V2.`
          : "Connected to OpenCode V2, but it reported no models.",
    },
  });
});
