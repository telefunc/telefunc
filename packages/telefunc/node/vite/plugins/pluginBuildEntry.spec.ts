import { expect, it, vi } from 'vitest'
import { config } from '../../server/serverConfig.js'
import { setTelefuncLoaders } from '../../server/runTelefunc/loadTelefuncFilesUsingVite/loadBuildEntry.js'
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
  const manifest = getManifest()
  expect(manifest.config).toMatchObject({ telefuncUrl: '/api/tf', channel: { pingInterval: 999 } })
  expect(manifest.config).not.toHaveProperty('extensions')
})

it("the production entry keeps the channel settings of the app's server code", () => {
  config.channel.pingInterval = 999
  const manifest = getManifest()
  config.channel = { transports: ['sse', 'ws'], reconnectTimeout: 12345 }
  setTelefuncLoaders({ loadTelefuncFiles: async () => ({}), loadManifest: () => manifest })
  expect(config.channel.pingInterval).toBe(999)
  expect(config.channel.transports).toEqual(['sse', 'ws'])
  expect(config.channel.reconnectTimeout).toBe(12345)
})

it("the production entry keeps the app's own root and telefunc files", () => {
  config.root = '/build/app'
  config.telefuncFiles = ['/build/app/pages/a.telefunc.ts']
  const manifest = getManifest()
  config.root = '/srv/app'
  config.telefuncFiles = ['/srv/app/pages/a.telefunc.js']
  setTelefuncLoaders({ loadTelefuncFiles: async () => ({}), loadManifest: () => manifest })
  expect(config.root).toBe('/srv/app')
  expect(config.telefuncFiles).toEqual(['/srv/app/pages/a.telefunc.js'])
})

function getManifest() {
  pluginBuildEntry()
  const entryCode = mocks.getServerProductionEntry!()
  return JSON.parse(entryCode.match(/loadManifest: \(\) => \((.*)\)\n\}\);/s)![1]!)
}
