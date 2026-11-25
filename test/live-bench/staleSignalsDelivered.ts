import { staleSignals } from '../../packages/live/dist/staleSignals.js'

/** Until no stale signal waits for its turn: the server holds them back while it is busy. */
export async function staleSignalsDelivered(): Promise<void> {
  do await new Promise((resolve) => setTimeout(resolve, 5))
  while ((staleSignals as unknown as { waiting: unknown[] }).waiting.length > 0)
}
