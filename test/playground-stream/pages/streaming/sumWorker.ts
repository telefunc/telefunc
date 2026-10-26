const sums: Record<string, number> = {}

onmessage = ({ data }: MessageEvent<{ name: string; chunk: Uint8Array } | 'sums'>) => {
  if (data === 'sums') return postMessage(sums)
  sums[data.name] = (sums[data.name] ?? 0) + data.chunk.reduce((sum, byte) => sum + byte, 0)
}
