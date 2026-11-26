import { defineConfig } from 'vite'
import { cloudflare } from '@cloudflare/vite-plugin'
import { config } from 'telefunc'
import { telefunc } from 'telefunc/vite'

config.telefuncUrl = '/api/tf'

export default defineConfig({
  plugins: [telefunc(), cloudflare()],
})
