import telefunc from 'telefunc/vite'
import react from '@vitejs/plugin-react'
import vike from 'vike/plugin'
import type { UserConfig } from 'vite'
import tailwindcss from '@tailwindcss/vite'

export default {
  server: {
    hmr: {
      port: 24679,
    },
  },
  plugins: [react(), vike(), telefunc(), tailwindcss()],
  // @ts-expect-error
  vitePluginServerEntry: {
    disableAutoImport: true,
  },
  build: {
    outDir: `${__dirname}/../../test/playground/dist/nested`,
  },
} satisfies UserConfig
