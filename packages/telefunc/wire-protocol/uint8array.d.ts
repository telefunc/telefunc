// Mirrors `lib.esnext.uint8array.d.ts`. Drop this file once TypeScript ships the lib.
interface Uint8Array<TArrayBuffer extends ArrayBufferLike> {
  toBase64(options?: { alphabet?: 'base64' | 'base64url'; omitPadding?: boolean }): string
}
interface Uint8ArrayConstructor {
  fromBase64(string: string, options?: { alphabet?: 'base64' | 'base64url' }): Uint8Array<ArrayBuffer>
}
