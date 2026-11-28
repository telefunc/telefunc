let retained

export async function onUpload(first, second) {
  const reader = first.stream().getReader()
  await reader.read()
  await reader.cancel()
  retained = second
  return true
}

export async function onReadRetained() {
  const bytes = new Uint8Array(await retained.arrayBuffer())
  return bytes.length === 786432 && bytes.every((byte) => byte === 22)
}
