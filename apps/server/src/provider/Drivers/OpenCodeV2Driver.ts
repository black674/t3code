/**
 * OpenCodeV2Driver — `ProviderDriver` for the standalone OpenCode v2 service.
 *
 * Independent tool from OpenCode v1: different binary channel (`@opencode/cli`),
 * different API (`/api/*`), different SSE event model. Shares no code with
 * `OpenCodeDriver` beyond the `ProviderDriver` shape.
 *
 * @module provider/Drivers/OpenCodeV2Driver
 */
import { OpenCodeV2Settings, ProviderDriverKind } from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import { HttpClient } from "effect/unstable/http";
import { ChildProcessSpawner } from "effect/unstable/process";

import { makeOpenCodeV2TextGeneration } from "../../textGeneration/OpenCodeV2TextGeneration.ts";
import * as BackgroundPolicy from "../../background/BackgroundPolicy.ts";
import { ServerConfig } from "../../config.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import { ProviderDriverError } from "../Errors.ts";
import { makeOpenCodeV2Adapter } from "../Layers/OpenCodeV2Adapter.ts";
import { readOpenCodeGoUsageLimits } from "../Layers/openCodeUsageLimits.ts";
import {
  checkOpenCodeV2ProviderStatus,
  makePendingOpenCodeV2Provider,
  openCodeV2CommandsToServerProviderSlashCommands,
  openCodeV2SkillsToServerProviderSkills,
} from "../Layers/OpenCodeV2Provider.ts";
import { makeManagedServerProvider } from "../makeManagedServerProvider.ts";
import { OpenCodeV2Runtime } from "../opencodeV2Runtime.ts";
import {
  defaultProviderContinuationIdentity,
  type ProviderDriver,
  type ProviderInstance,
} from "../ProviderDriver.ts";
import { withInstanceIdentity } from "./instanceIdentity.ts";
import { mergeProviderInstanceEnvironment } from "../ProviderInstanceEnvironment.ts";
import {
  enrichProviderSnapshotWithVersionAdvisory,
  makeManualOnlyProviderMaintenanceCapabilities,
} from "../providerMaintenance.ts";
import {
  haveProviderSnapshotSettingsChanged,
  makeProviderSnapshotSettingsSource,
  type ProviderSnapshotSettings,
} from "../providerUpdateSettings.ts";

const decodeSettings = Schema.decodeSync(OpenCodeV2Settings);

const DRIVER_KIND = ProviderDriverKind.make("opencodeV2");

// v2 ships through several channels (curl, brew, npm, AUR) under the same
// `opencode` binary name — an npm-registry version check misfires for
// non-npm installs (phantom "Update Available" that does nothing), so
// installation and updates stay manual, like Grok.
const MAINTENANCE_CAPABILITIES = makeManualOnlyProviderMaintenanceCapabilities({
  provider: DRIVER_KIND,
  packageName: null,
});

export type OpenCodeV2DriverEnv =
  | BackgroundPolicy.BackgroundPolicy
  | ChildProcessSpawner.ChildProcessSpawner
  | Crypto.Crypto
  | FileSystem.FileSystem
  | HttpClient.HttpClient
  | OpenCodeV2Runtime
  | Path.Path
  | ServerConfig
  | ServerSettingsService;

export const OpenCodeV2Driver: ProviderDriver<OpenCodeV2Settings, OpenCodeV2DriverEnv> = {
  driverKind: DRIVER_KIND,
  metadata: {
    displayName: "OpenCode V2",
    supportsMultipleInstances: true,
  },
  configSchema: OpenCodeV2Settings,
  defaultConfig: (): OpenCodeV2Settings => decodeSettings({}),
  create: ({ instanceId, displayName, accentColor, environment, enabled, config }) =>
    Effect.gen(function* () {
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const fileSystem = yield* FileSystem.FileSystem;
      const pathService = yield* Path.Path;
      const runtime = yield* OpenCodeV2Runtime;
      const serverConfig = yield* ServerConfig;
      const httpClient = yield* HttpClient.HttpClient;
      const serverSettings = yield* ServerSettingsService;
      const processEnv = mergeProviderInstanceEnvironment(environment);
      const continuationIdentity = defaultProviderContinuationIdentity({
        driverKind: DRIVER_KIND,
        instanceId,
      });
      const stampIdentity = withInstanceIdentity({
        instanceId,
        driverKind: DRIVER_KIND,
        displayName,
        accentColor,
        continuationGroupKey: continuationIdentity.continuationKey,
      });
      const effectiveConfig = { ...config, enabled } satisfies OpenCodeV2Settings;
      const resolveMaintenance = () => Effect.succeed(MAINTENANCE_CAPABILITIES);

      const adapter = yield* makeOpenCodeV2Adapter(effectiveConfig, {
        instanceId,
        environment: processEnv,
      });
      const textGeneration = yield* makeOpenCodeV2TextGeneration(effectiveConfig);

      const checkProvider = Effect.all(
        {
          provider: checkOpenCodeV2ProviderStatus(effectiveConfig, serverConfig.cwd, processEnv),
          // Account-level Go/Zen quota: same auth.json + usage endpoint v1
          // reads, so both providers report identical usage windows.
          usageLimits: readOpenCodeGoUsageLimits({
            enabled: effectiveConfig.enabled,
            serverUrl: effectiveConfig.serverUrl,
            environment: processEnv,
          }),
        },
        { concurrency: "unbounded" },
      ).pipe(
        Effect.map(({ provider, usageLimits }) => ({ ...provider, usageLimits })),
        Effect.map(stampIdentity),
        Effect.provideService(OpenCodeV2Runtime, runtime),
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
        Effect.provideService(FileSystem.FileSystem, fileSystem),
        Effect.provideService(Path.Path, pathService),
        Effect.provideService(HttpClient.HttpClient, httpClient),
      );

      const snapshotSettings = makeProviderSnapshotSettingsSource(effectiveConfig, serverSettings);
      const snapshot = yield* makeManagedServerProvider<
        ProviderSnapshotSettings<OpenCodeV2Settings>
      >({
        resolveMaintenance,
        getSettings: snapshotSettings.getSettings,
        streamSettings: snapshotSettings.streamSettings,
        haveSettingsChanged: haveProviderSnapshotSettingsChanged,
        checkProviderOnSettingsChange: () => false,
        refreshOnInterval: false,
        initialSnapshot: (settings) =>
          makePendingOpenCodeV2Provider(settings.provider).pipe(Effect.map(stampIdentity)),
        checkProvider,
        enrichSnapshot: ({ settings, snapshot, publishSnapshot }) =>
          resolveMaintenance().pipe(
            Effect.flatMap((capabilities) =>
              enrichProviderSnapshotWithVersionAdvisory(snapshot, capabilities, {
                enableProviderUpdateChecks: settings.enableProviderUpdateChecks,
              }),
            ),
            Effect.provideService(HttpClient.HttpClient, httpClient),
            Effect.flatMap((enriched) => publishSnapshot(enriched)),
          ),
      }).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderDriverError({
              driver: DRIVER_KIND,
              instanceId,
              detail: `Failed to build OpenCode V2 snapshot: ${cause.message ?? String(cause)}`,
              cause,
            }),
        ),
      );

      return {
        instanceId,
        driverKind: DRIVER_KIND,
        continuationIdentity,
        displayName,
        accentColor,
        enabled,
        snapshot,
        snapshotForCwd: (cwd) =>
          !effectiveConfig.enabled
            ? snapshot.getSnapshot
            : Effect.scoped(
                runtime
                  .connectToServer({
                    binaryPath: effectiveConfig.binaryPath,
                    directory: cwd,
                    ...(effectiveConfig.serverUrl.trim().length > 0
                      ? { serverUrl: effectiveConfig.serverUrl }
                      : {}),
                    ...(effectiveConfig.serverPassword
                      ? { serverPassword: effectiveConfig.serverPassword }
                      : {}),
                    environment: processEnv,
                  })
                  .pipe(
                    Effect.flatMap((server) =>
                      Effect.all(
                        {
                          machineSnapshot: snapshot.getSnapshot,
                          skills: runtime.listSkills({
                            baseUrl: server.url,
                            ...(server.serverPassword === undefined
                              ? {}
                              : { serverPassword: server.serverPassword }),
                            directory: cwd,
                          }),
                          commands: runtime.listCommands({
                            baseUrl: server.url,
                            ...(server.serverPassword === undefined
                              ? {}
                              : { serverPassword: server.serverPassword }),
                          }),
                        },
                        { concurrency: "unbounded" },
                      ),
                    ),
                  ),
              ).pipe(
                Effect.map(({ machineSnapshot, skills, commands }) => ({
                  ...machineSnapshot,
                  skills: openCodeV2SkillsToServerProviderSkills(skills),
                  slashCommands: openCodeV2CommandsToServerProviderSlashCommands(commands),
                })),
                Effect.mapError(
                  (cause) =>
                    new ProviderDriverError({
                      driver: DRIVER_KIND,
                      instanceId,
                      detail: `Failed to probe OpenCode V2 commands and skills for '${cwd}'`,
                      cause,
                    }),
                ),
              ),
        adapter,
        textGeneration,
      } satisfies ProviderInstance;
    }),
};
