import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    include: ['test/**/*.integration.test.ts'],
    environment: 'node',
    // Integration tests talk to a real NATS server — allow generous timeouts.
    testTimeout: 30_000,
    hookTimeout: 30_000,
  },
});
