import tailwindcss from '@tailwindcss/vite'
import react from '@vitejs/plugin-react'
import telefunc from 'telefunc/vite'
import vike from 'vike/plugin'
import type { UserConfig } from 'vite'

export default {
  plugins: [react(), vike(), telefunc(), tailwindcss()],
} satisfies UserConfig
