export { textPayload, binaryPayload, isLossy, binaryData }

// A broadcast publish as its transport carries it, from the publish to each subscriber's server and page: it says
// whether a page that is behind may skip it.

/** A lossy one leads with a line feed, which no serialized message starts with and JSON's parse skips. */
function textPayload(serialized: string, lossy = false): string {
  return lossy ? '\n' + serialized : serialized
}

/** One leading byte, 1 where it is lossy, then `data`. */
function binaryPayload(data: Uint8Array, lossy = false): Uint8Array<ArrayBuffer> {
  const payload = new Uint8Array(1 + data.byteLength)
  if (lossy) payload[0] = 1
  payload.set(data, 1)
  return payload
}

function isLossy(payload: string | Uint8Array): boolean {
  return typeof payload === 'string' ? payload.charCodeAt(0) === 10 : payload[0] === 1
}

/** What was published, without the binary payload's leading byte. */
function binaryData(payload: Uint8Array): Uint8Array {
  return payload.subarray(1)
}
