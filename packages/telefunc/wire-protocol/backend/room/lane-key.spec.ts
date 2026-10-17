import { expect, it } from 'vitest'
import type { LaneId } from './contract.js'
import { decodeLaneKey, encodeLaneKey } from './lane-key.js'

it('decodes every lane key back to its lane, separators included', () => {
  const lanes: LaneId[] = [
    { kind: 'control' },
    { kind: 'semantic' },
    { kind: 'inbox', member: 'a:b%c' },
    { kind: 'binary', member: 'm', track: '' },
    { kind: 'binary', member: 'm:1', track: 'screen:hi/%' },
  ]
  for (const lane of lanes) expect(decodeLaneKey(encodeLaneKey(lane))).toEqual(lane)
})
