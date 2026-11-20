import { defineConfig } from 'tsup';

export default defineConfig({
  entry: ['index.ts'],
  format: ['cjs', 'esm'],
  // Inline @openpanel/core's constants types into the bundled .d.ts so
  // consumers don't see an `import type { … }` of a workspace-internal,
  // unpublished package.
  // tsup injects `baseUrl` into its dts compile; TS 6 rejects it unless told to.
  dts: {
    resolve: [/^@openpanel\//],
    compilerOptions: { ignoreDeprecations: '6.0' },
  },
  splitting: false,
  sourcemap: false,
  clean: true,
  minify: true,
});
