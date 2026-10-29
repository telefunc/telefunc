import { defineConfig } from 'vite'
import { telefunc } from 'telefunc/vite'
import standaloner from 'standaloner/vite'

export default defineConfig({
  plugins: [telefunc(), standaloner()],
  build: { ssr: true, outDir: 'dist/server', rollupOptions: { input: { index: 'server.ts' } } },
})
