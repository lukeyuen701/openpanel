import { defineConfig } from 'tsup';

export default defineConfig({
  entry: ['index.ts', 'vercel.ts', 'openai.ts', 'anthropic.ts'],
  format: ['cjs', 'esm'],
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
