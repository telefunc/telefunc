export { uint8ArrayToBase64url, base64urlToUint8Array }

// `Uint8Array.toBase64` / `fromBase64` are 5–10× faster than the btoa/atob path.
// Chrome 133+, Firefox 133+, Safari 18.2+, Node 22+. Older runtimes use the fallback.
const NATIVE_TO_BASE64 = typeof Uint8Array.prototype.toBase64 === 'function'
const NATIVE_FROM_BASE64 = typeof Uint8Array.fromBase64 === 'function'

// Stays under every engine's `apply` argument cap (Safari ~65k).
const ENCODE_CHUNK = 0x8000

function uint8ArrayToBase64url(bytes: Uint8Array<ArrayBuffer>): string {
  if (NATIVE_TO_BASE64) return bytes.toBase64({ alphabet: 'base64url', omitPadding: true })
  let binary = ''
  for (let i = 0; i < bytes.length; i += ENCODE_CHUNK) {
    binary += String.fromCharCode.apply(null, bytes.subarray(i, i + ENCODE_CHUNK) as unknown as number[])
  }
  return btoa(binary).replaceAll('+', '-').replaceAll('/', '_').replace(/=+$/, '')
}

function base64urlToUint8Array(s: string): Uint8Array<ArrayBuffer> {
  if (NATIVE_FROM_BASE64) return Uint8Array.fromBase64(s, { alphabet: 'base64url' })
  const padCount = (4 - (s.length & 3)) & 3
  const base64 = s.replaceAll('-', '+').replaceAll('_', '/') + '==='.slice(0, padCount)
  const binary = atob(base64)
  const len = binary.length
  const bytes = new Uint8Array(len)
  for (let i = 0; i < len; i++) bytes[i] = binary.charCodeAt(i)
  return bytes
}
