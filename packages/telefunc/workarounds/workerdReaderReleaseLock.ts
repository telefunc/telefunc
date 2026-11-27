// Workaround: on the body of an incoming HTTP request, workerd's `reader.releaseLock()` throws, and keeps the lock, while
// a read is pending, as the streams standard did before https://github.com/whatwg/streams/pull/1168; Node, Deno and Bun fail the pending read, and so does
// workerd on a stream JS made. Without the cancel, a request body let go of on workerd ends its read only at its next
// chunk. Remove once workerd releases a native request body's lock with a read pending.
export { releaseOrCancel }

/** Releases `reader`'s lock, failing its read under way; where the runtime refuses (workerd), cancels its stream. */
function releaseOrCancel(reader: ReadableStreamDefaultReader<Uint8Array<ArrayBuffer>>): void {
  try {
    reader.releaseLock()
  } catch {
    reader.cancel().catch(() => {})
  }
}
