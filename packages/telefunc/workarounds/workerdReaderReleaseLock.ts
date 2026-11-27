// Workaround: on the body of an incoming HTTP request, workerd's `reader.releaseLock()` throws, and keeps the lock, while
// a read is pending; Node, Deno and Bun fail the pending read, and so does workerd on a stream JS made. Without the
// cancel, a request body let go of on workerd ends its read only at its next chunk.
// TODO: remove once workerd fixes native incoming-body releaseLock() with a pending read (documented as ledger #29 in https://github.com/cloudflare/workerd/pull/7471; spec https://github.com/whatwg/streams/pull/1168).
export { releaseOrCancel }

/** Releases `reader`'s lock, failing its read under way; where the runtime refuses (workerd), cancels its stream. */
function releaseOrCancel(reader: ReadableStreamDefaultReader<Uint8Array<ArrayBuffer>>): void {
  try {
    reader.releaseLock()
  } catch {
    reader.cancel().catch(() => {})
  }
}
