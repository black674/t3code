import { describe, expect, it } from "vite-plus/test";

import { deriveToolActivityPresentation, mapToolNameToItemType } from "./toolActivity.ts";

describe("toolActivity", () => {
  it("normalizes command tools to a stable ran-command label", () => {
    expect(
      deriveToolActivityPresentation({
        itemType: "command_execution",
        title: "Terminal",
        detail: "Terminal",
        data: {
          command: "bun run lint",
        },
        fallbackSummary: "Terminal",
      }),
    ).toEqual({
      summary: "Ran command",
      detail: "bun run lint",
    });
  });

  it("uses structured file paths for read-file tools when available", () => {
    expect(
      deriveToolActivityPresentation({
        itemType: "dynamic_tool_call",
        title: "Read File",
        detail: "Read File",
        data: {
          kind: "read",
          locations: [{ path: "/tmp/app.ts" }],
        },
        fallbackSummary: "Read File",
      }),
    ).toEqual({
      summary: "Read file",
      detail: "/tmp/app.ts",
    });
  });

  it("drops duplicated generic read-file detail when no path is available", () => {
    expect(
      deriveToolActivityPresentation({
        itemType: "dynamic_tool_call",
        title: "Read File",
        detail: "Read File",
        data: {
          kind: "read",
          rawInput: {},
        },
        fallbackSummary: "Read File",
      }),
    ).toEqual({
      summary: "Read file",
    });
  });
});

describe("mapToolNameToItemType", () => {
  it("maps shell and command tools to command execution", () => {
    expect(mapToolNameToItemType("bash")).toBe("command_execution");
    expect(mapToolNameToItemType("shell")).toBe("command_execution");
    expect(mapToolNameToItemType("run_command")).toBe("command_execution");
  });

  it("maps edit and write tools to file change", () => {
    expect(mapToolNameToItemType("edit")).toBe("file_change");
    expect(mapToolNameToItemType("multiedit")).toBe("file_change");
  });

  it("maps remaining tools to their dedicated rows", () => {
    expect(mapToolNameToItemType("webfetch")).toBe("web_search");
    expect(mapToolNameToItemType("mcp__playwright__click")).toBe("mcp_tool_call");
    expect(mapToolNameToItemType("task")).toBe("collab_agent_tool_call");
  });

  it("falls back to dynamic tool calls", () => {
    expect(mapToolNameToItemType("read")).toBe("dynamic_tool_call");
    expect(mapToolNameToItemType("todowrite")).toBe("dynamic_tool_call");
    expect(mapToolNameToItemType("something-new")).toBe("dynamic_tool_call");
  });
});
