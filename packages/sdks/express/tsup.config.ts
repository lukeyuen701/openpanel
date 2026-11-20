import { defineConfig } from 'tsup';

export default defineConfig({
  entry: ['index.ts', 'cdn.ts'],
  format: ['cjs', 'esm', 'iife'],
  // tsup injects `baseUrl` into its dts compile; TS 6 rejects it unless told to.
  dts: { compilerOptions: { ignoreDeprecations: '6.0' } },
  splitting: false,
  sourcemap: false,
  clean: true,
  minify: true,
});
