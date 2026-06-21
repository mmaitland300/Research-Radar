import { expect, test as base } from "@playwright/test";
import type { Page } from "@playwright/test";

type Fixtures = {
  consoleErrors: string[];
};

const test = base.extend<Fixtures>({
  consoleErrors: [
    async ({ page }, run) => {
      const errors: string[] = [];
      page.on("console", (message) => {
        if (message.type() === "error") {
          errors.push(message.text());
        }
      });
      page.on("pageerror", (error) => {
        errors.push(error.message);
      });

      await run(errors);

      expect(errors, "page should not emit browser console errors").toEqual([]);
    },
    { auto: true },
  ],
});

async function expectPrimaryNavigation(page: Page) {
  await expect(page.getByRole("navigation", { name: "Primary" })).toBeVisible();
}

test("search renders fixture paper results", async ({ page }) => {
  await page.goto("/search?q=audio&limit=5");

  await expectPrimaryNavigation(page);
  await expect(
    page.getByRole("heading", { name: /Search the curated corpus with lexical retrieval first/i })
  ).toBeVisible();
  await expect(page.getByText("Total matches", { exact: true })).toBeVisible();
  await expect(
    page.getByRole("link", { name: /Contrastive Audio Embeddings for Music Retrieval/i })
  ).toBeVisible();
  await expect(page.getByText("fixture lexical score", { exact: false })).toBeVisible();
});

test("emerging recommendations render family tabs and fixture results", async ({ page }) => {
  await page.goto("/recommended?family=emerging");

  await expectPrimaryNavigation(page);
  const familyTabs = page.getByRole("navigation", { name: "Recommendation family" });
  await expect(familyTabs).toBeVisible();
  await expect(familyTabs.getByRole("link", { name: "Emerging" })).toHaveAttribute(
    "aria-current",
    "page"
  );
  await expect(page.getByRole("heading", { name: "Emerging results" })).toBeVisible();
  await expect(
    page.getByRole("link", { name: /Contrastive Audio Embeddings for Music Retrieval/i })
  ).toBeVisible();
});

test("bridge recommendations render guardrail copy and fixture results", async ({ page }) => {
  await page.goto("/recommended?family=bridge");

  await expectPrimaryNavigation(page);
  const familyTabs = page.getByRole("navigation", { name: "Recommendation family" });
  await expect(familyTabs.getByRole("link", { name: "Bridge" })).toHaveAttribute(
    "aria-current",
    "page"
  );
  await expect(page.getByText("Bridge evidence is experimental and run-specific")).toBeVisible();
  await expect(page.getByRole("heading", { name: "Bridge preview results" })).toBeVisible();
  await expect(
    page.getByRole("link", { name: /Bridge Signals Between Timbre Models and Generative Audio/i })
  ).toBeVisible();
  await expect(
    page.getByText("Bridge preview shows measured cross-cluster signal", { exact: false })
  ).toBeVisible();
});

test("evaluation renders proxy boundary content", async ({ page }) => {
  await page.goto("/evaluation");

  await expectPrimaryNavigation(page);
  await expect(
    page.getByRole("heading", { name: "Evaluation v0: ranked feed vs simple baselines" })
  ).toBeVisible();
  await expect(page.getByText("Distributional checks only")).toBeVisible();
  await expect(
    page.getByText("Nothing here measures whether researchers would find the papers useful", {
      exact: false,
    })
  ).toBeVisible();
  await expect(page.getByRole("heading", { name: "Interpretation notes" })).toBeVisible();
});

test("trends renders fixture topic momentum content", async ({ page }) => {
  await page.goto("/trends");

  await expectPrimaryNavigation(page);
  await expect(
    page.getByRole("heading", { name: "Topic momentum in the current dataset only." })
  ).toBeVisible();
  await expect(page.getByText("Curated corpus trends")).toBeVisible();
  await expect(page.getByRole("heading", { name: "Topic momentum (curated corpus)" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Audio Embeddings" })).toBeVisible();
});
