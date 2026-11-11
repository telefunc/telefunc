import { defineConfig } from 'vite'
import { cloudflare } from '@cloudflare/vite-plugin'
import { telefunc } from 'telefunc/vite'

export default defineConfig({
  plugins: [telefunc(), cloudflare()],
})
