const { defineConfig, devices } = require("@playwright/test");

const apiUrl = "http://127.0.0.1:8000";
const webUrl = "http://localhost:3000";

const fixtureEnv = {
  ...process.env,
  API_BASE_URL: apiUrl,
  NEXT_PUBLIC_API_BASE_URL: apiUrl,
  NEXT_PUBLIC_EMBEDDING_VERSION: "fixture-title-abstract-v0",
  NEXT_PUBLIC_RANKING_VERSION: "fixture-demo-v0-no-db",
  RESEARCH_RADAR_DATA_MODE: "fixture",
  PYTHONUNBUFFERED: "1",
};

module.exports = defineConfig({
  testDir: "./apps/web/tests",
  timeout: 30_000,
  expect: {
    timeout: 10_000,
  },
  fullyParallel: false,
  forbidOnly: Boolean(process.env.CI),
  globalTeardown: require.resolve("./scripts/playwright-restore-next-env.cjs"),
  retries: 0,
  workers: 1,
  outputDir: "artifacts/playwright/test-results",
  reporter: [
    ["list"],
    ["html", { outputFolder: "artifacts/playwright/html-report", open: "never" }],
  ],
  use: {
    baseURL: webUrl,
    screenshot: "only-on-failure",
    trace: "off",
    video: "off",
  },
  webServer: [
    {
      command:
        "python -m uvicorn app.main:app --app-dir apps/api --host 127.0.0.1 --port 8000",
      env: fixtureEnv,
      reuseExistingServer: false,
      timeout: 120_000,
      url: `${apiUrl}/health`,
    },
    {
      command: "npm run dev:web",
      env: fixtureEnv,
      reuseExistingServer: false,
      timeout: 120_000,
      url: webUrl,
    },
  ],
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
  ],
});
