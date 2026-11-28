import telefunc from 'telefunc/vite'
import react from '@vitejs/plugin-react'
import vike from 'vike/plugin'
import type { UserConfig } from 'vite'

export default {
  plugins: [
    react(),
    vike(),
    telefunc(),
    {
      name: 'playground:chunk-modules',
      apply: 'build',
      applyToEnvironment: (environment) => environment.name === 'client',
      generateBundle(_options, bundle) {
        const modulesByChunk: Record<string, string[]> = {}
        for (const output of Object.values(bundle)) {
          if (output.type === 'chunk') modulesByChunk[`/${output.fileName}`] = Object.keys(output.modules)
        }
        this.emitFile({ type: 'asset', fileName: 'chunk-modules.json', source: JSON.stringify(modulesByChunk) })
      },
    },
  ],
  build: {
    outDir: `${__dirname}/../../test/playground/dist/nested`,
  },
} satisfies UserConfig
