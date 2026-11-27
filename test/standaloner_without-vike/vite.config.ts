import { defineConfig } from 'vite'
import { config } from 'telefunc'
import { telefunc } from 'telefunc/vite'
import standaloner from 'standaloner/vite'

config.disableNamingConvention = true
// `test-dev.test.ts` serves Telefunc at a URL of the app's own, as an app with no server code of its own sets it.
if (process.env.TELEFUNC_URL) config.telefuncUrl = process.env.TELEFUNC_URL

export default defineConfig({
  plugins: [telefunc(), standaloner()],
  build: { ssr: true, outDir: 'dist/server', rollupOptions: { input: { index: 'server.ts' } } },
})
