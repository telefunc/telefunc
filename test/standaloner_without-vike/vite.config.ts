import { defineConfig } from 'vite'
import { config } from 'telefunc'
import { telefunc } from 'telefunc/vite'
import standaloner from 'standaloner/vite'

config.telefuncUrl = '/api/tf'

export default defineConfig({
  plugins: [telefunc(), standaloner()],
  build: { ssr: true, outDir: 'dist/server', rollupOptions: { input: { index: 'server.ts' } } },
})
