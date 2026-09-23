import * as NodeAssert from "node:assert/strict";

import { describe, it } from "vite-plus/test";

import { mapToolNameToItemType } from "@t3tools/shared/toolActivity";

import {
  parseOpenCodeV2ServerOutput,
  parseOpenCodeV2ServiceRegistration,
} from "./opencodeV2Runtime.ts";
import {
  describeOpenCodeV2ToolInput,
  findV2RollbackBoundary,
  isV2TodoTool,
  mapOpenCodeV2ExecutionOutcome,
  mapOpenCodeV2SseType,
  mapOpenCodeV2ToolLifecycle,
  mapV2PermissionToRequestType,
  normalizeOpenCodeV2ToolEvent,
  openCodeV2ErrorMessage,
  openCodeV2ToolText,
  v2TodoPlanFromToolInput,
} from "./Layers/OpenCodeV2Adapter.ts";

describe("parseOpenCodeV2ServerOutput", () => {
  it("parses the v2 serve URL and generated password", () => {
    const result = parseOpenCodeV2ServerOutput(
      [
        "server listening on http://127.0.0.1:48999",
        "server password aXaICplpoA0kedyaA1DBt-Gghk1j1mHQWf1tm-NeJ28",
        "",
      ].join("\n"),
    );
    NodeAssert.equal(result.url, "http://127.0.0.1:48999");
    NodeAssert.equal(result.password, "aXaICplpoA0kedyaA1DBt-Gghk1j1mHQWf1tm-NeJ28");
  });

  it("returns null URL when the server has not printed its address yet", () => {
    const result = parseOpenCodeV2ServerOutput("starting up\n");
    NodeAssert.equal(result.url, null);
    NodeAssert.equal(result.password, undefined);
  });
});

describe("parseOpenCodeV2ServiceRegistration", () => {
  it("parses url and password from service.json", () => {
    const result = parseOpenCodeV2ServiceRegistration(
      JSON.stringify({
        id: "abc",
        version: "2.0.14",
        url: "http://127.0.0.1:49374",
        pid: 1,
        password: "secret",
      }),
    );
    NodeAssert.deepStrictEqual(result, { url: "http://127.0.0.1:49374", password: "secret" });
  });

  it("returns undefined for garbage", () => {
    NodeAssert.equal(parseOpenCodeV2ServiceRegistration("not json"), undefined);
    NodeAssert.equal(parseOpenCodeV2ServiceRegistration(JSON.stringify({ url: "  " })), undefined);
  });
});

describe("mapOpenCodeV2SseType", () => {
  it("maps text and reasoning deltas", () => {
    NodeAssert.equal(mapOpenCodeV2SseType("session.text.delta"), "text");
    NodeAssert.equal(mapOpenCodeV2SseType("session.reasoning.delta"), "reasoning");
  });

  it("maps step, execution, and tool lifecycles", () => {
    NodeAssert.equal(mapOpenCodeV2SseType("session.step.ended"), "step");
    NodeAssert.equal(mapOpenCodeV2SseType("session.execution.succeeded"), "execution");
    NodeAssert.equal(mapOpenCodeV2SseType("session.tool.started"), "tool");
  });

  it("ignores unknown event types", () => {
    NodeAssert.equal(mapOpenCodeV2SseType("server.connected"), "ignore");
    NodeAssert.equal(mapOpenCodeV2SseType("session.future.unknown"), "ignore");
  });

  it("maps permission lifecycle events", () => {
    NodeAssert.equal(mapOpenCodeV2SseType("permission.asked"), "permission");
    NodeAssert.equal(mapOpenCodeV2SseType("permission.replied"), "permission");
  });
});

describe("mapV2PermissionToRequestType", () => {
  it("maps shell to command approval and edit to file approval", () => {
    NodeAssert.equal(mapV2PermissionToRequestType("shell"), "command_execution_approval");
    NodeAssert.equal(mapV2PermissionToRequestType("edit"), "file_change_approval");
    NodeAssert.equal(mapV2PermissionToRequestType("read"), "file_read_approval");
    NodeAssert.equal(mapV2PermissionToRequestType("something-new"), "command_execution_approval");
  });
});

describe("mapOpenCodeV2ExecutionOutcome", () => {
  it("only treats succeeded/failed as terminal", () => {
    NodeAssert.equal(mapOpenCodeV2ExecutionOutcome("session.execution.succeeded"), "succeeded");
    NodeAssert.equal(mapOpenCodeV2ExecutionOutcome("session.execution.failed"), "failed");
  });

  it("ignores started so turns survive until text streams", () => {
    NodeAssert.equal(mapOpenCodeV2ExecutionOutcome("session.execution.started"), undefined);
    NodeAssert.equal(mapOpenCodeV2ExecutionOutcome("session.step.failed"), undefined);
    NodeAssert.equal(mapOpenCodeV2ExecutionOutcome("session.retry.scheduled"), undefined);
  });
});

describe("openCodeV2ErrorMessage", () => {
  it("reads structured v2.0.15 error objects", () => {
    NodeAssert.equal(
      openCodeV2ErrorMessage({
        sessionID: "ses_1",
        error: { type: "provider.auth", message: "Out of credits", status: 402 },
      }),
      "Out of credits",
    );
  });

  it("reads legacy string errors and messages", () => {
    NodeAssert.equal(openCodeV2ErrorMessage({ error: "boom" }), "boom");
    NodeAssert.equal(openCodeV2ErrorMessage({ message: "kaput" }), "kaput");
    NodeAssert.equal(openCodeV2ErrorMessage({}), undefined);
    NodeAssert.equal(openCodeV2ErrorMessage(null), undefined);
  });
});

describe("mapToolNameToItemType", () => {
  it("mirrors v1 rows so the timeline renders detail", () => {
    NodeAssert.equal(mapToolNameToItemType("bash"), "command_execution");
    NodeAssert.equal(mapToolNameToItemType("edit"), "file_change");
    NodeAssert.equal(mapToolNameToItemType("read"), "dynamic_tool_call");
    NodeAssert.equal(mapToolNameToItemType("mcp__playwright__click"), "mcp_tool_call");
    NodeAssert.equal(mapToolNameToItemType("something-new"), "dynamic_tool_call");
  });
});

describe("normalizeOpenCodeV2ToolEvent", () => {
  it("reads flat tool events with input and output", () => {
    const normalized = normalizeOpenCodeV2ToolEvent({
      sessionID: "ses_1",
      id: "call_1",
      name: "bash",
      input: { command: "git status" },
      output: "On branch main\nnothing to commit",
    });
    NodeAssert.equal(normalized.toolName, "bash");
    NodeAssert.equal(normalized.callId, "call_1");
    NodeAssert.equal(normalized.command, "git status");
    NodeAssert.equal(normalized.title, "git status");
    NodeAssert.equal(normalized.output, "On branch main\nnothing to commit");
    NodeAssert.equal(normalized.error, undefined);
  });

  it("reads nested transcript-style state layouts", () => {
    const normalized = normalizeOpenCodeV2ToolEvent({
      sessionID: "ses_1",
      name: "read",
      state: {
        status: "completed",
        input: { filePath: "/repo/README.md" },
        content: [{ text: "Hello" }],
      },
    });
    NodeAssert.equal(normalized.toolName, "read");
    NodeAssert.equal(normalized.callId, undefined);
    NodeAssert.equal(normalized.output, "Hello");
  });

  it("extracts structured errors", () => {
    const normalized = normalizeOpenCodeV2ToolEvent({
      name: "bash",
      state: {
        status: "error",
        input: { command: "exit 1" },
        error: { type: "tool.failed", message: "exit code 1" },
      },
    });
    NodeAssert.equal(normalized.error, "exit code 1");
  });
});

describe("openCodeV2ToolText", () => {
  it("flattens text content blocks", () => {
    NodeAssert.equal(openCodeV2ToolText("hi"), "hi");
    NodeAssert.equal(openCodeV2ToolText([{ text: "a" }, { text: "b" }]), "a\nb");
    NodeAssert.equal(openCodeV2ToolText({ content: [{ text: "x" }] }), "x");
    NodeAssert.equal(openCodeV2ToolText({}), undefined);
    NodeAssert.equal(openCodeV2ToolText(42), undefined);
  });
});

describe("describeOpenCodeV2ToolInput", () => {
  it("summarizes commands, paths, and objects", () => {
    NodeAssert.equal(describeOpenCodeV2ToolInput("git status"), "git status");
    NodeAssert.equal(
      describeOpenCodeV2ToolInput({ filePath: "/repo/README.md" }),
      "/repo/README.md",
    );
    NodeAssert.equal(describeOpenCodeV2ToolInput({}), "{}");
    NodeAssert.equal(describeOpenCodeV2ToolInput(undefined), undefined);
  });
});

describe("mapOpenCodeV2ToolLifecycle", () => {
  it("opens rows on called and closes them on success or failure", () => {
    NodeAssert.equal(mapOpenCodeV2ToolLifecycle("session.tool.called"), "started");
    NodeAssert.equal(mapOpenCodeV2ToolLifecycle("session.tool.started"), "started");
    NodeAssert.equal(mapOpenCodeV2ToolLifecycle("session.tool.success"), "ended");
    NodeAssert.equal(mapOpenCodeV2ToolLifecycle("session.tool.failed"), "ended");
    NodeAssert.equal(mapOpenCodeV2ToolLifecycle("session.tool.ended"), "ended");
    NodeAssert.equal(mapOpenCodeV2ToolLifecycle("session.tool.error"), "ended");
  });

  it("ignores streaming input refinements so one run is one row", () => {
    NodeAssert.equal(mapOpenCodeV2ToolLifecycle("session.tool.input.started"), undefined);
    NodeAssert.equal(mapOpenCodeV2ToolLifecycle("session.tool.input.ended"), undefined);
    NodeAssert.equal(mapOpenCodeV2ToolLifecycle("session.tool.progress"), undefined);
    NodeAssert.equal(mapOpenCodeV2ToolLifecycle("session.text.delta"), undefined);
  });
});

describe("findV2RollbackBoundary", () => {
  const messages = [
    { id: "m1", type: "user" },
    { id: "m2", type: "assistant" },
    { id: "m3", type: "user" },
    { id: "m4", type: "assistant" },
    { id: "m5", type: "user" },
    { id: "m6", type: "assistant" },
  ];

  it("rewinds one turn to its opening user message", () => {
    NodeAssert.deepStrictEqual(findV2RollbackBoundary(messages, 1), { beforeMessageId: "m5" });
  });

  it("clamps rewinding past the first turn", () => {
    NodeAssert.deepStrictEqual(findV2RollbackBoundary(messages, 9), { beforeMessageId: "m1" });
  });

  it("counts turns by assistant messages like v1, dropping a pending prompt too", () => {
    NodeAssert.deepStrictEqual(
      findV2RollbackBoundary(
        [
          { id: "m1", type: "user" },
          { id: "m2", type: "assistant" },
          { id: "m3", type: "user" },
        ],
        1,
      ),
      { beforeMessageId: "m1" },
    );
  });

  it("truncates history at a native revert marker like v1", () => {
    NodeAssert.deepStrictEqual(findV2RollbackBoundary(messages, 1, "m4"), {
      beforeMessageId: "m1",
    });
  });

  it("returns undefined without assistant messages", () => {
    NodeAssert.equal(
      findV2RollbackBoundary(
        [
          { id: "m1", type: "user" },
          { id: "m2", type: "idle" },
        ],
        1,
      ),
      undefined,
    );
  });
});

describe("v2TodoPlanFromToolInput", () => {
  it("maps todo states to plan steps", () => {
    NodeAssert.deepStrictEqual(
      v2TodoPlanFromToolInput({
        todos: [
          { content: "Done", status: "completed" },
          { content: "Doing", status: "in_progress" },
          { content: "Later", status: "pending" },
          { content: "  ", status: "pending" },
        ],
      }),
      [
        { step: "Done", status: "completed" },
        { step: "Doing", status: "inProgress" },
        { step: "Later", status: "pending" },
      ],
    );
  });

  it("ignores non-todo inputs", () => {
    NodeAssert.equal(v2TodoPlanFromToolInput({ command: "ls" }), undefined);
    NodeAssert.equal(v2TodoPlanFromToolInput({ todos: [] }), undefined);
    NodeAssert.equal(v2TodoPlanFromToolInput(undefined), undefined);
  });

  it("recognizes the todo tools", () => {
    NodeAssert.equal(isV2TodoTool("todowrite"), true);
    NodeAssert.equal(isV2TodoTool("TodoRead"), true);
    NodeAssert.equal(isV2TodoTool("bash"), false);
  });
});
