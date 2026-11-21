import { describe, expect, it } from 'vitest'
import { assertPublishOptions } from './arguments.js'

describe('assertPublishOptions', () => {
  it('takes `lossy: false` without a track, as leaving it out', () => {
    expect(() => assertPublishOptions({ lossy: false })).not.toThrow()
    expect(() => assertPublishOptions({ lossy: false, retain: true })).not.toThrow()
  })

  it('refuses `lossy: true` without a track, and takes it on one', () => {
    // @ts-expect-error `lossy: true` needs a `track`, which a JavaScript caller is told at the call.
    expect(() => assertPublishOptions({ lossy: true })).toThrow('publish() options.lossy: true needs options.track')
    expect(() => assertPublishOptions({ track: 'cursor', lossy: true })).not.toThrow()
  })
})
