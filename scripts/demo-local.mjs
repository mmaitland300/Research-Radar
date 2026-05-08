#!/usr/bin/env node

import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import path from "node:path";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const apiUrl = "http://127.0.0.1:8000";
const webUrl = "http://localhost:3000";
const pythonCmd = process.env.PYTHON || "python";
const npmCmd = process.platform === "win32" ? "npm.cmd" : "npm";

const env = {
  ...process.env,
  API_BASE_URL: apiUrl,
  NEXT_PUBLIC_API_BASE_URL: apiUrl,
  NEXT_PUBLIC_EMBEDDING_VERSION: "fixture-title-abstract-v0",
  NEXT_PUBLIC_RANKING_VERSION: "fixture-demo-v0-no-db",
  PYTHONUNBUFFERED: "1",
  RESEARCH_RADAR_DATA_MODE: "fixture",
};

const processes = [
  {
    name: "api",
    command: pythonCmd,
    args: ["-m", "uvicorn", "app.main:app", "--app-dir", "apps/api", "--host", "127.0.0.1", "--port", "8000"],
  },
  {
    name: "web",
    command: npmCmd,
    args: ["run", "dev:web"],
  },
];

let shuttingDown = false;
const children = [];

function prefixLines(name, chunk) {
  const text = chunk.toString();
  for (const line of text.split(/\r?\n/)) {
    if (line.trim().length > 0) {
      console.log(`[${name}] ${line}`);
    }
  }
}

function shutdown(code = 0) {
  if (shuttingDown) {
    return;
  }
  shuttingDown = true;
  for (const child of children) {
    if (process.platform === "win32" && child.pid) {
      spawn("taskkill", ["/pid", String(child.pid), "/T", "/F"], {
        stdio: "ignore",
      });
    } else if (!child.killed) {
      child.kill();
    }
  }
  setTimeout(() => process.exit(code), 250);
}

console.log("Starting Research Radar fixture demo.");
console.log(`API: ${apiUrl} (${env.RESEARCH_RADAR_DATA_MODE} mode, no DB/API keys)`);
console.log(`Web: ${webUrl}`);
console.log("Press Ctrl+C to stop both processes.");

for (const proc of processes) {
  const child = spawn(proc.command, proc.args, {
    cwd: root,
    env,
    stdio: ["ignore", "pipe", "pipe"],
  });
  children.push(child);
  child.stdout.on("data", (chunk) => prefixLines(proc.name, chunk));
  child.stderr.on("data", (chunk) => prefixLines(proc.name, chunk));
  child.on("exit", (code, signal) => {
    if (!shuttingDown) {
      console.error(`[${proc.name}] exited with ${signal ?? code}`);
      shutdown(code ?? 1);
    }
  });
}

process.on("SIGINT", () => shutdown(0));
process.on("SIGTERM", () => shutdown(0));
