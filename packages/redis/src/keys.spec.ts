import { expect, test } from 'vitest'
import { broadcastChannel, broadcastSequenceKey } from './keys.js'

test('a Broadcast key is one encoded hash tag, whatever it contains', () => {
  expect(broadcastSequenceKey('tf:', '}edge')).toBe('tf:seq:{%7Dedge}')
  expect(broadcastChannel('tf:', { key: 'a}b', kind: 'binary' })).toBe('tf:b:{a%7Db}')
  expect(broadcastSequenceKey('tf:', '')).toBe('tf:seq:{_}:empty')
})
