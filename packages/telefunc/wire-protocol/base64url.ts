export { uint8ArrayToBase64url, base64urlToUint8Array }

/** Encode a Uint8Array to a base64url string (no padding). */
function uint8ArrayToBase64url(bytes: Uint8Array): string {
  // Build a binary string from the byte array, then use btoa() to get base64.
  // btoa() is available in browsers and Node.js 16+.
  let binary = ''
  for (let i = 0; i < bytes.length; i++) {
    binary += String.fromCharCode(bytes[i]!)
  }
  return btoa(binary).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '')
}

/** Decode a base64url string (no padding) to a Uint8Array. */
function base64urlToUint8Array(s: string): Uint8Array {
  // Restore standard base64: replace url-safe chars and add padding.
  const base64 = s.replace(/-/g, '+').replace(/_/g, '/') + '===='.slice(s.length % 4 || 4)
  const binary = atob(base64)
  const bytes = new Uint8Array(binary.length)
  for (let i = 0; i < binary.length; i++) {
    bytes[i] = binary.charCodeAt(i)
  }
  return bytes
}
