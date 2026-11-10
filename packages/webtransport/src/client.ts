// Adds WebTransport to the transports a page's channels climb, where the browser has it.
import { registerChannelTransport } from 'telefunc/__internal'
import { WtTransport } from './client/WtTransport.js'

if (typeof WebTransport !== 'undefined') registerChannelTransport('webtransport', (options) => new WtTransport(options))
