import * as NodeAssert from "node:assert/strict";

import { describe, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import * as Effect from "effect/Effect";

import { OpenCodeV2Settings } from "@t3tools/contracts";

import {
  makePendingOpenCodeV2Provider,
  openCodeV2CapabilitiesForModel,
} from "./OpenCodeV2Provider.ts";

const decodeSettings = Schema.decodeSync(OpenCodeV2Settings);

describe("makePendingOpenCodeV2Provider", () => {
  it("reports disabled when the instance is off", () =>
    Effect.gen(function* () {
      const snapshot = yield* makePendingOpenCodeV2Provider(
        decodeSettings({ enabled: false, binaryPath: "opencode" }),
      );
      NodeAssert.equal(snapshot.enabled, false);
      NodeAssert.equal(snapshot.status, "disabled");
      NodeAssert.equal(snapshot.displayName, "OpenCode V2");
    }).pipe(Effect.runPromise));

  it("reports pending check when the instance is on", () =>
    Effect.gen(function* () {
      const snapshot = yield* makePendingOpenCodeV2Provider(
        decodeSettings({ enabled: true, binaryPath: "opencode" }),
      );
      NodeAssert.equal(snapshot.enabled, true);
      NodeAssert.equal(snapshot.status, "warning");
      NodeAssert.equal(snapshot.installed, false);
    }).pipe(Effect.runPromise));
});

describe("openCodeV2CapabilitiesForModel", () => {
  it("exposes variant and agent selectors like v1", () => {
    const capabilities = openCodeV2CapabilitiesForModel({
      providerID: "opencode",
      variants: ["low", "medium", "high"],
      agents: [
        { id: "build", mode: "primary" },
        { id: "plan", mode: "primary" },
      ],
    });
    NodeAssert.deepStrictEqual(
      capabilities.optionDescriptors?.map((descriptor) => descriptor.id),
      ["variant", "agent"],
    );
  });

  it("synthesizes reasoning levels when the server advertises no variants", () => {
    const capabilities = openCodeV2CapabilitiesForModel({
      providerID: "openrouter",
      variants: [],
      agents: [{ id: "build", mode: "primary" }],
    });
    const variant = capabilities.optionDescriptors?.find(
      (descriptor) => descriptor.id === "variant",
    );
    NodeAssert.equal(variant?.type, "select");
  });

  it("hides housekeeping agents from the picker", () => {
    const capabilities = openCodeV2CapabilitiesForModel({
      providerID: "opencode",
      variants: [],
      agents: [
        { id: "build", mode: "primary" },
        { id: "title", mode: "all" },
        { id: "compaction", mode: "all" },
      ],
    });
    const agent = capabilities.optionDescriptors?.find((descriptor) => descriptor.id === "agent");
    NodeAssert.deepStrictEqual(
      agent?.type === "select" ? agent.options.map((option) => option.id) : [],
      ["build"],
    );
  });
});
