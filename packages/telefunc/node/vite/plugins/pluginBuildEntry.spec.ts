import { expect, it, vi } from 'vitest'
import { config } from '../../server/serverConfig.js'
import { pluginBuildEntry } from './pluginBuildEntry.js'

const mocks = vi.hoisted(() => ({ getServerProductionEntry: null as null | (() => string) }))

vi.mock('@brillout/vite-plugin-server-entry/plugin', () => ({
  serverProductionEntryPlugin: ({ getServerProductionEntry }: { getServerProductionEntry: () => string }) => {
    mocks.getServerProductionEntry = getServerProductionEntry
    return []
  },
}))

it('the production entry carries the config set at build time', () => {
  config.telefuncUrl = '/api/tf'
  config.channel.pingInterval = 999
  pluginBuildEntry()
  const entryCode = mocks.getServerProductionEntry!()
  const manifest = JSON.parse(entryCode.match(/loadManifest: \(\) => \((.*)\)\n\}\);/s)![1]!)
  expect(manifest.config).toEqual({ telefuncUrl: '/api/tf', channel: { pingInterval: 999 } })
})
