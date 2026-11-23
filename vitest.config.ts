import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    fileParallelism: false,      // one shared Postgres database
    testTimeout: 60_000,
    hookTimeout: 60_000,
  },
});
