const fs = require("node:fs");
const path = require("node:path");
const { spawnSync } = require("node:child_process");

const nextEnvPath = path.resolve(__dirname, "../apps/web/next-env.d.ts");
const repoRoot = path.resolve(__dirname, "..");

const content = `/// <reference types="next" />
/// <reference types="next/image-types/global" />
import "./.next/types/routes.d.ts";

// NOTE: This file should not be edited
// see https://nextjs.org/docs/app/api-reference/config/typescript for more information.
`;

module.exports = async function restoreNextEnv() {
  fs.writeFileSync(nextEnvPath, content, "utf8");
  spawnSync("git", ["update-index", "--refresh", "--", "apps/web/next-env.d.ts"], {
    cwd: repoRoot,
    stdio: "ignore",
  });
};
