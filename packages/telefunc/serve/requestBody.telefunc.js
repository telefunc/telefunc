// The telefunctions of serve/node.requestBody.spec.ts and serve/node.uploadBody.spec.ts, which Telefunc loads from
// `config.telefuncFiles` with Node's own `import()`, as an app's: they tell the spec through `process` events.

export async function hello() {
  return 'hello'
}

/** Its own outgoing request fails: the app's error, whatever its code. */
export async function callsAnotherServer() {
  throw Object.assign(new Error('the other server reset the connection'), { code: 'ECONNRESET' })
}

export async function readsBlob(blob) {
  process.emit('requestBody.telefunc:read')
  return (await blob.text()).length
}

/** Reads the second `Blob` even when the first one's read fails. */
export async function readsBoth(first, second) {
  process.emit('requestBody.telefunc:read')
  await first.text().catch(() => {})
  return (await second.text()).length
}
