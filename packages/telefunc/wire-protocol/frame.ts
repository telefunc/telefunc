export { encodeU32, decodeU32, concat }

const textEncoder = new TextEncoder()
const textDecoder = new TextDecoder()

function encodeU32(n: number): Uint8Array {
  const buf = new Uint8Array(4)
  new DataView(buf.buffer).setUint32(0, n, false)
  return buf
}

function decodeU32(buf: Uint8Array, offset = 0): number {
  return new DataView(buf.buffer, buf.byteOffset + offset, 4).getUint32(0, false)
}

function concat(a: Uint8Array, b: Uint8Array): Uint8Array {
  const result = new Uint8Array(a.length + b.length)
  result.set(a, 0)
  result.set(b, a.length)
  return result
}

export { textEncoder, textDecoder }
