import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import vike from 'vike/plugin'
import { cloudflare } from '@cloudflare/vite-plugin'
import { telefunc } from 'telefunc/vite'

export default defineConfig({
  // Pre-bundles telefunc in the Worker, as Vite does for an app that installs it from npm.
  environments: { ssr: { optimizeDeps: { include: ['telefunc/cloudflare'] } } },
  plugins: [
    telefunc(),
    react(),
    cloudflare({
      // TO-DO/eventuall: remove this line depending on the outcome of https://github.com/cloudflare/workers-sdk/issues/10120
      viteEnvironment: { name: 'ssr' },
    }),
    vike(),
  ],
})
