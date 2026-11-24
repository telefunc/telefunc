// The telefunctions of the serve/ specs of request bodies (node.requestBody, node.uploadBody, node.keepAlive and
// node.heldUpload), which Telefunc loads from `config.telefuncFiles` with Node's own `import()`, as an app's: they tell
// the spec through `process` events.

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

/** Holds its first `Blob`'s reader without reading, and reads the second: tells the spec how that read ended. */
export async function holdsFirstReadsSecond(first, second) {
  first.stream().getReader()
  process.emit('requestBody.telefunc:read')
  try {
    const text = await second.text()
    process.emit('requestBody.telefunc:readEnded', `read ${text.length} bytes`)
  } catch (err) {
    process.emit('requestBody.telefunc:readEnded', `failed: ${err.message}`)
  }
}

/** Answers without reading its `Blob`. */
export async function ignoresBlob() {
  return 'ignored'
}

/** Starts reading its `Blob` and answers without waiting for it: tells the spec how that read ended. */
export async function startsReadingBlob(blob) {
  blob.text().then(
    (text) => process.emit('requestBody.telefunc:readEnded', `read ${text.length} bytes`),
    (err) => process.emit('requestBody.telefunc:readEnded', `failed: ${err.message}`),
  )
  process.emit('requestBody.telefunc:read')
  return 'started'
}

/** Answers without reading its `Blob`, which it hands the spec to read once the call is done. */
export async function keepsBlob(blob) {
  process.emit('requestBody.telefunc:kept', blob)
  return 'kept'
}

/** Answers with a Channel the spec makes (and ends), leaving its `Blob` unread: the channel holds the request. */
export async function returnsChannelIgnoringBlob() {
  const made = {}
  process.emit('requestBody.telefunc:channel', made)
  return made.channel
}

/** Answers with a Channel the spec makes, and hands the spec its `Blob` to read once the call is done. */
export async function returnsChannelKeepingBlob(blob) {
  const made = {}
  process.emit('requestBody.telefunc:channel', made)
  process.emit('requestBody.telefunc:kept', blob)
  return made.channel
}

/** Answers with a Channel the spec makes, and reads its `Blob` as it arrives: tells the spec how that read ended. */
export async function returnsChannelReadingBlob(blob) {
  const made = {}
  process.emit('requestBody.telefunc:channel', made)
  void (async () => {
    let read = 0
    try {
      for await (const chunk of blob.stream()) read += chunk.length
      process.emit('requestBody.telefunc:readEnded', `read ${read} bytes`)
    } catch (err) {
      process.emit('requestBody.telefunc:readEnded', `failed after ${read} bytes: ${err.message}`)
    }
  })()
  return made.channel
}
