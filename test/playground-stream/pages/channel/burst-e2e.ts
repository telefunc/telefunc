export { burstWhileOffline, burstOf, memoryPeakOf }

import { page, expect, autoRetry, getServerUrl } from '@brillout/test-e2e'
import { navigate, getResult, getCleanupState, runServerAction } from '../../e2e-utils'
import type { ChannelMemoryPeak } from '../../channel-memory'

type Burst = { channelId: string; received: number[]; onClose: string | null }

/** Opens the burst channel of the button `#wire-burst-open-<button>`, and has the server send its burst, unawaited,
 *  while the page is offline; the page then comes back. Returns the channel's id. */
async function burstWhileOffline(button: string): Promise<string> {
  await navigate(`${getServerUrl()}/channel`)
  await page.click(`#wire-burst-open-${button}`)
  let channelId = ''
  await autoRetry(async () => {
    channelId = (await burstOf()).channelId
    expect((await getCleanupState())[`wireBurst_${channelId}_onOpen`]).toBe('true')
  })
  await page.context().setOffline(true)
  await runServerAction(`wireBurst_${channelId}`)
  await page.context().setOffline(false)
  return channelId
}

/** The burst channel, as the page shows it. */
async function burstOf(): Promise<Burst> {
  const { burst } = await getResult<{ burst: Burst | null }>('#wire-state')
  if (burst === null) throw new Error('No burst channel yet')
  return burst
}

/** The most the server's channel memory held since the burst of the channel `channelId`, as its ledger counts it. */
async function memoryPeakOf(channelId: string): Promise<ChannelMemoryPeak> {
  await runServerAction(`wireBurst_${channelId}_peak`)
  const peak: unknown = JSON.parse((await getCleanupState())[`wireBurst_${channelId}_peak`] ?? 'null')
  if (!isPeak(peak)) throw new Error(`No memory peak for ${channelId}`)
  return peak
}

function isPeak(value: unknown): value is ChannelMemoryPeak {
  return (
    typeof value === 'object' &&
    value !== null &&
    typeof Reflect.get(value, 'held') === 'number' &&
    typeof Reflect.get(value, 'heldWithWaiting') === 'number' &&
    typeof Reflect.get(value, 'maxMemory') === 'number'
  )
}
