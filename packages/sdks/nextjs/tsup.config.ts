import { defineConfig } from 'tsup';

export default defineConfig({
  format: ['cjs', 'esm'],
  entry: ['index.tsx', 'server.ts'],
  external: ['react', 'next'],
  // tsup injects `baseUrl` into its dts compile; TS 6 rejects it unless told to.
  dts: { compilerOptions: { ignoreDeprecations: '6.0' } },
  splitting: false,
  sourcemap: false,
  clean: true,
  minify: true,
});
