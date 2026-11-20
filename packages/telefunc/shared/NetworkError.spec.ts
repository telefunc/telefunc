import { expect, test, vi } from 'vitest'
import { NetworkError } from './NetworkError.js'
import { ConnectionError } from '../client/ConnectionError.js'
import { ChannelCloseUnknownError, ChannelPageLeftError } from '../wire-protocol/channel/channelErrors.js'

// A second copy of Telefunc's modules, as Vite's dev server loads one beside the one Node loads: an app's telefunction
// imports `NetworkError` from one copy while the channel it awaits ends with an error of the other. Loaded while the file
// is collected.
vi.resetModules()
const otherCopy = {
  ...(await import('../wire-protocol/channel/channelErrors.js')),
  ...(await import('./NetworkError.js')),
}

test('a NetworkError from another copy of Telefunc, as in a Vite dev server, is a NetworkError, both ways', () => {
  expect(otherCopy.NetworkError).not.toBe(NetworkError)
  expect(new otherCopy.ChannelPageLeftError()).toBeInstanceOf(NetworkError)
  expect(new otherCopy.ChannelCloseUnknownError()).toBeInstanceOf(NetworkError)
  expect(new otherCopy.NetworkError('Channel lost', true)).toBeInstanceOf(NetworkError)
  expect(new ChannelPageLeftError()).toBeInstanceOf(otherCopy.NetworkError)
})

test('anything without its mark is no NetworkError, and a subclass takes only its own kind', () => {
  expect(new Error('Channel lost')).not.toBeInstanceOf(NetworkError)
  expect({ isChannel: true }).not.toBeInstanceOf(NetworkError)
  expect(null).not.toBeInstanceOf(NetworkError)
  expect(new ConnectionError()).toBeInstanceOf(NetworkError)
  expect(new ChannelPageLeftError()).toBeInstanceOf(ChannelPageLeftError)
  expect(new ChannelPageLeftError()).not.toBeInstanceOf(ChannelCloseUnknownError)
  expect(new ConnectionError()).not.toBeInstanceOf(ChannelPageLeftError)
})
