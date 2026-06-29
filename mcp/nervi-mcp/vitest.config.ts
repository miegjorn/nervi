import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    // Unit tests only — excludes integration tests that require a live NATS server.
    include: ['test/**/*.test.ts'],
    exclude: ['test/**/*.integration.test.ts'],
    environment: 'node',
  },
});
