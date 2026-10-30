import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    include: ['test/**/*.test.ts'],
    // The stack test bundles both functions with esbuild during synthesis.
    testTimeout: 60_000,
  },
});
