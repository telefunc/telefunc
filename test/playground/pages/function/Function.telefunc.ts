export { onGetGreeter, onGetAdder, onGetEchoWithState, onMap, onReduce, onUpload }

/** Returns a plain function — transparently proxied over an ack channel on the client. */
async function onGetGreeter() {
  return async (name: string) => `Hello, ${name}! (from server)`
}

/** Returns a stateful function — closure state lives on the server. */
async function onGetAdder(initial: number) {
  let total = initial
  return async (n: number) => {
    total += n
    return total
  }
}

/** Returns a function that echoes with server-side call count. */
async function onGetEchoWithState() {
  let count = 0
  return async (msg: string) => {
    count++
    return { echo: msg, callCount: count }
  }
}

/** Accepts a client-defined mapper and applies it server-side. */
async function onMap(items: number[], mapper: (n: number) => Promise<number>) {
  const results: number[] = []
  for (const item of items) {
    results.push(await mapper(item))
  }
  return results
}

/** Accepts a client-defined reducer and reduces an array server-side. */
async function onReduce(items: number[], reducer: (acc: number, n: number) => Promise<number>, initial: number) {
  let acc = initial
  for (const item of items) {
    acc = await reducer(acc, item)
  }
  return acc
}

/** File upload with progress callback — server streams file and calls onProgress with percentage. */
async function onUpload(file: File, onProgress: (percent: number) => Promise<void>) {
  const total = file.size
  let loaded = 0
  const reader = file.stream().getReader()
  while (true) {
    const { done, value } = await reader.read()
    if (done) break
    loaded += value.byteLength
    await onProgress(Math.round((loaded / total) * 100))
    // Simulate slow IO (disk write, S3 upload, etc.)
    await new Promise((r) => setTimeout(r, 10))
  }
  return { name: file.name, size: total }
}
