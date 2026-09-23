/**
 * OpenCodeV2TextGeneration — one-shot JSON/text generation via the v2 API.
 *
 * Mirrors `OpenCodeTextGeneration` semantics: the configured
 * `modelSelection` is used as-is (no fallbacks), sessions are locked down
 * with a deny-all ruleset, and failures propagate as `TextGenerationError`.
 * v2 `prompt` returns the user message immediately while the assistant
 * streams, so unlike v1's blocking prompt the result is collected by
 * polling `GET /api/session/{id}/message` until an assistant message lands.
 *
 * @module textGeneration/OpenCodeV2TextGeneration
 */
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import {
  TextGenerationError,
  type ChatAttachment,
  type ModelSelection,
  type OpenCodeV2Settings,
} from "@t3tools/contracts";
import { getModelSelectionStringOptionValue, splitProviderModelSlug } from "@t3tools/shared/model";
import { extractJsonObject } from "@t3tools/shared/schemaJson";

import { ServerConfig } from "../config.ts";
import { resolveAttachmentPath } from "../attachmentStore.ts";
import * as TextGeneration from "./TextGeneration.ts";
import {
  buildBranchNamePrompt,
  buildCommitMessagePrompt,
  buildPrContentPrompt,
  buildThreadTitlePrompt,
} from "./TextGenerationPrompts.ts";
import {
  sanitizeCommitSubject,
  sanitizePrTitle,
  sanitizeThreadTitle,
} from "./TextGenerationUtils.ts";
import { OpenCodeV2Runtime } from "../provider/opencodeV2Runtime.ts";
import { sanitizeFeatureBranchName } from "@t3tools/shared/git";

const POLL_INTERVAL_MS = 500;
const POLL_TIMEOUT_MS = 120_000;

type Operation =
  | "generateCommitMessage"
  | "generatePrContent"
  | "generateBranchName"
  | "generateThreadTitle";

const isTextGenerationError = Schema.is(TextGenerationError);

export const makeOpenCodeV2TextGeneration = Effect.fn("makeOpenCodeV2TextGeneration")(function* (
  settings: OpenCodeV2Settings,
) {
  const serverConfig = yield* ServerConfig;
  const runtime = yield* OpenCodeV2Runtime;

  const runV2Json = <S extends Schema.Top>({
    operation,
    cwd,
    prompt,
    outputSchemaJson,
    modelSelection,
    attachments,
  }: {
    operation: Operation;
    cwd: string;
    prompt: string;
    outputSchemaJson: S;
    modelSelection: ModelSelection;
    attachments?: ReadonlyArray<ChatAttachment> | undefined;
  }): Effect.Effect<S["Type"], TextGenerationError, S["DecodingServices"]> =>
    Effect.gen(function* () {
      const serverPassword = settings.serverPassword.trim();
      const connectInput = {
        binaryPath: settings.binaryPath,
        directory: cwd,
        ...(settings.serverUrl.trim().length > 0 ? { serverUrl: settings.serverUrl } : {}),
        ...(serverPassword.length > 0 ? { serverPassword } : {}),
      };
      const parsedModel = splitProviderModelSlug(modelSelection.model);
      if (!parsedModel) {
        return yield* new TextGenerationError({
          operation,
          detail: "OpenCode V2 model selection must use the 'provider/model' format.",
        });
      }
      const selectedAgent = getModelSelectionStringOptionValue(modelSelection, "agent");
      const selectedVariant = getModelSelectionStringOptionValue(modelSelection, "variant");
      const fileParts = (attachments ?? [])
        .filter((attachment) => attachment.type === "image")
        .flatMap((attachment) => {
          const path = resolveAttachmentPath({
            attachmentsDir: serverConfig.attachmentsDir,
            attachment,
          });
          if (!path) return [];
          const uri = path.startsWith("file://") ? path : `file://${path.replace(/\\/g, "/")}`;
          return [{ uri, name: attachment.name }];
        });
      const raw = yield* Effect.scoped(
        Effect.gen(function* () {
          const server = yield* runtime
            .connectToServer(connectInput)
            .pipe(
              Effect.mapError(
                (cause) => new TextGenerationError({ operation, detail: cause.detail, cause }),
              ),
            );
          const auth =
            server.serverPassword === undefined ? {} : { serverPassword: server.serverPassword };
          const session = yield* runtime
            .createSession({
              baseUrl: server.url,
              ...auth,
              directory: cwd,
              title: `T3 Code ${operation}`,
              modelSlug: modelSelection.model,
              ...(selectedVariant ? { variant: selectedVariant } : {}),
              ...(selectedAgent ? { agent: selectedAgent } : {}),
              permissions: [{ action: "*", resource: "*", effect: "deny" }],
            })
            .pipe(
              Effect.mapError(
                (cause) => new TextGenerationError({ operation, detail: cause.detail, cause }),
              ),
            );
          try {
            yield* runtime
              .promptSession({
                baseUrl: server.url,
                ...auth,
                sessionId: session.id,
                text: prompt,
                ...(fileParts.length === 0 ? {} : { files: fileParts }),
              })
              .pipe(
                Effect.mapError(
                  (cause) => new TextGenerationError({ operation, detail: cause.detail, cause }),
                ),
              );
            const deadline = (yield* Clock.currentTimeMillis) + POLL_TIMEOUT_MS;
            let lastText = "";
            let lastError: string | undefined;
            while ((yield* Clock.currentTimeMillis) < deadline) {
              yield* Effect.sleep(`${POLL_INTERVAL_MS} millis`);
              const messages = yield* runtime
                .listMessages({ baseUrl: server.url, ...auth, sessionId: session.id })
                .pipe(
                  Effect.orElseSucceed(
                    (): ReadonlyArray<{
                      readonly type: string;
                      readonly text: string;
                      readonly id: string;
                    }> => [],
                  ),
                );
              const assistantIds = messages
                .filter((message) => message.type === "assistant")
                .map((message) => message.id);
              for (const messageId of assistantIds) {
                const detail = yield* runtime
                  .getMessage({ baseUrl: server.url, ...auth, sessionId: session.id, messageId })
                  .pipe(Effect.orElseSucceed(() => undefined));
                if (detail === undefined) continue;
                if (detail.errorMessage) {
                  lastError = detail.errorMessage;
                  break;
                }
                if (detail.text.trim().length > 0) {
                  lastText = detail.text;
                  break;
                }
              }
              if (lastText || lastError) break;
            }
            if (lastError) {
              return yield* new TextGenerationError({
                operation,
                detail: `OpenCode V2 error: ${lastError}`,
              });
            }
            if (lastText.trim().length === 0) {
              return yield* new TextGenerationError({
                operation,
                detail: "OpenCode V2 returned empty output.",
              });
            }
            return lastText;
          } finally {
            yield* runtime
              .deleteSession({ baseUrl: server.url, ...auth, sessionId: session.id })
              .pipe(Effect.ignore);
          }
        }),
      ).pipe(
        Effect.mapError((cause) =>
          isTextGenerationError(cause)
            ? cause
            : new TextGenerationError({
                operation,
                detail: "OpenCode V2 text generation failed.",
                cause,
              }),
        ),
      );
      const decodeOutput = Schema.decodeEffect(Schema.fromJsonString(outputSchemaJson));
      void serverConfig;
      return yield* decodeOutput(extractJsonObject(raw)).pipe(
        Effect.catchTags({
          SchemaError: (cause) =>
            Effect.fail(
              new TextGenerationError({
                operation,
                detail: "OpenCode V2 returned invalid structured output.",
                cause,
              }),
            ),
        }),
      );
    }).pipe(
      Effect.mapError((cause) =>
        isTextGenerationError(cause)
          ? cause
          : new TextGenerationError({
              operation,
              detail: "OpenCode V2 text generation failed.",
              cause,
            }),
      ),
    );

  const generateCommitMessage: TextGeneration.TextGeneration["Service"]["generateCommitMessage"] =
    Effect.fn("OpenCodeV2TextGeneration.generateCommitMessage")(function* (
      input: Parameters<TextGeneration.TextGeneration["Service"]["generateCommitMessage"]>[0],
    ) {
      const { prompt, outputSchema } = buildCommitMessagePrompt({
        branch: input.branch,
        stagedSummary: input.stagedSummary,
        stagedPatch: input.stagedPatch,
        includeBranch: input.includeBranch === true,
        policy: input.policy,
      });
      const generated = yield* runV2Json({
        operation: "generateCommitMessage",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        modelSelection: input.modelSelection,
      });
      return {
        subject: sanitizeCommitSubject(generated.subject),
        body: generated.body.trim(),
        ...("branch" in generated && typeof generated.branch === "string"
          ? { branch: sanitizeFeatureBranchName(generated.branch) }
          : {}),
      };
    });

  const generatePrContent: TextGeneration.TextGeneration["Service"]["generatePrContent"] =
    Effect.fn("OpenCodeV2TextGeneration.generatePrContent")(function* (
      input: Parameters<TextGeneration.TextGeneration["Service"]["generatePrContent"]>[0],
    ) {
      const { prompt, outputSchema } = buildPrContentPrompt({
        baseBranch: input.baseBranch,
        headBranch: input.headBranch,
        commitSummary: input.commitSummary,
        diffSummary: input.diffSummary,
        diffPatch: input.diffPatch,
        policy: input.policy,
        changeRequestTemplate: input.changeRequestTemplate,
      });
      const generated = yield* runV2Json({
        operation: "generatePrContent",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        modelSelection: input.modelSelection,
      });
      return { title: sanitizePrTitle(generated.title), body: generated.body };
    });

  const generateBranchName: TextGeneration.TextGeneration["Service"]["generateBranchName"] =
    Effect.fn("OpenCodeV2TextGeneration.generateBranchName")(function* (
      input: Parameters<TextGeneration.TextGeneration["Service"]["generateBranchName"]>[0],
    ) {
      const { prompt, outputSchema } = buildBranchNamePrompt({
        message: input.message,
        attachments: input.attachments,
      });
      const generated = yield* runV2Json({
        operation: "generateBranchName",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        modelSelection: input.modelSelection,
        attachments: input.attachments,
      });
      return { branch: sanitizeFeatureBranchName(generated.branch) };
    });

  const generateThreadTitle: TextGeneration.TextGeneration["Service"]["generateThreadTitle"] =
    Effect.fn("OpenCodeV2TextGeneration.generateThreadTitle")(function* (
      input: Parameters<TextGeneration.TextGeneration["Service"]["generateThreadTitle"]>[0],
    ) {
      // Same local-first policy as v1: initial titles never bill.
      if (input.previousTitle === undefined) {
        return { title: sanitizeThreadTitle(input.message) };
      }
      const { prompt, outputSchema } = buildThreadTitlePrompt({
        message: input.message,
        previousTitle: input.previousTitle,
        linkedContext: input.linkedContext,
        attachments: input.attachments,
      });
      const generated = yield* runV2Json({
        operation: "generateThreadTitle",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        modelSelection: input.modelSelection,
        attachments: input.attachments,
      });
      return {
        title: sanitizeThreadTitle(generated.title),
        ...(generated.needsRefinement ? { needsRefinement: true } : {}),
      };
    });

  return TextGeneration.TextGeneration.of({
    generateCommitMessage,
    generatePrContent,
    generateBranchName,
    generateThreadTitle,
  });
});
