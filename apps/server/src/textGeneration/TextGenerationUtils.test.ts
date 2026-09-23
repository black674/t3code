import { TextGenerationError } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import {
  fallbackThreadTitleFromMessage,
  isBillingError,
  sanitizeThreadTitle,
} from "./TextGenerationUtils.ts";

const billingError = (detail: string) =>
  new TextGenerationError({ operation: "generateThreadTitle", detail });

describe("isBillingError", () => {
  it("matches provider out-of-credits messages", () => {
    expect(
      isBillingError(
        billingError(
          "You're out of credits - this request needs $0.0085. Add credits to keep going: https://www.orcarouter.ai/console/billing",
        ),
      ),
    ).toBe(true);
  });

  it("matches quota, payment, and free-tier lockouts", () => {
    expect(isBillingError(billingError("Payment Required"))).toBe(true);
    expect(isBillingError(billingError("quota exceeded for this billing account"))).toBe(true);
    expect(
      isBillingError(
        billingError("Free models are not available to this account yet (status 429)"),
      ),
    ).toBe(true);
    expect(
      isBillingError(billingError("OpenCode's free tier can only be used from within OpenCode")),
    ).toBe(true);
  });

  it("ignores model, network, and schema failures", () => {
    expect(isBillingError(billingError("Model not found: openai/gpt-5"))).toBe(false);
    expect(isBillingError(billingError("Timed out during session.prompt."))).toBe(false);
    expect(isBillingError(new Error("boom"))).toBe(false);
    expect(isBillingError(undefined)).toBe(false);
  });

  it("collects nested error objects and causes", () => {
    expect(isBillingError({ detail: "request failed", error: { message: "out of credits" } })).toBe(
      true,
    );
    expect(
      isBillingError(
        new TextGenerationError({
          operation: "generateThreadTitle",
          detail: "request failed",
          cause: { status: 429, message: "rate limited" },
        }),
      ),
    ).toBe(true);
  });
});

describe("fallbackThreadTitleFromMessage", () => {
  it("uses the message text so titles never bill", () => {
    expect(fallbackThreadTitleFromMessage("hello")).toBe("hello");
    expect(fallbackThreadTitleFromMessage("which model are you")).toBe("which model are you");
  });

  it("matches sanitizeThreadTitle truncation rules", () => {
    expect(fallbackThreadTitleFromMessage("")).toBe(sanitizeThreadTitle(""));
    expect(fallbackThreadTitleFromMessage("a".repeat(500))).toBe(
      sanitizeThreadTitle("a".repeat(500)),
    );
  });
});
