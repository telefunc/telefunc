export { textPayload, binaryPayload, isLossy, binaryData, publishedData }

import { assertUsage } from '../../utils/assert.js'

// A broadcast publish as its transport carries it, from the publish to each subscriber's server and page: it says
// whether a page that is behind may skip it.

// A binary payload's marks: no UTF-8 text and few binary formats start with either byte, so nearly every plain one is
// the caller's own bytes.
/** Leads a lossy binary payload. */
const LOSSY = 0xfe
/** Leads a plain binary payload whose data starts with a mark. */
const ESCAPE = 0xfd

/** A lossy one leads with a line feed, which no serialized message starts with and JSON's parse skips. */
function textPayload(serialized: string, lossy = false): string {
  assertLossy(lossy, 'publish')
  return lossy ? '\n' + serialized : serialized
}

/** `data` itself, unless it is lossy or starts with a mark: then `data` behind one. */
function binaryPayload(data: Uint8Array, lossy = false): Uint8Array {
  assertLossy(lossy, 'publishBinary')
  if (!lossy && !isMarked(data)) return data
  const payload = new Uint8Array(1 + data.byteLength)
  payload[0] = lossy ? LOSSY : ESCAPE
  payload.set(data, 1)
  return payload
}

function isLossy(payload: string | Uint8Array): boolean {
  return typeof payload === 'string' ? payload.charCodeAt(0) === 10 : payload[0] === LOSSY
}

/** A JavaScript caller may pass anything, such as `'false'` read from a form. */
function assertLossy(lossy: boolean, method: 'publish' | 'publishBinary'): void {
  if (typeof lossy !== 'boolean') assertUsage(false, `\`${method}()\`'s \`lossy\` should be a boolean`)
}

/** What was published, without the binary payload's mark. */
function binaryData(payload: Uint8Array): Uint8Array {
  return isMarked(payload) ? payload.subarray(1) : payload
}

function isMarked(bytes: Uint8Array): boolean {
  return bytes[0] === LOSSY || bytes[0] === ESCAPE
}

/** What was published, of either kind: without the binary payload's mark or the lossy text's line feed. */
function publishedData(payload: string | Uint8Array): string | Uint8Array {
  if (typeof payload !== 'string') return binaryData(payload)
  return isLossy(payload) ? payload.slice(1) : payload
}
