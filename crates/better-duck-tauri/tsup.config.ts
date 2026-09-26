import { defineConfig } from 'tsup'

// Bundles the guest-js bindings into dist-js (ESM + CJS + d.ts), matching the
// layout the official Tauri plugins use.
export default defineConfig({
  entry: ['guest-js/index.ts'],
  outDir: 'dist-js',
  format: ['esm', 'cjs'],
  dts: true,
  clean: true,
  external: ['@tauri-apps/api'],
})
