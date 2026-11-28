import { expect, it } from 'vitest'
import { SortedIds, compareCodePoints } from './SortedIds.js'

/** Every id of `ids` from `start` on. */
function from(ids: SortedIds, start: string): string[] {
  return [...ids.from(start)]
}

it('reads ids in code point order, a supplementary character after every BMP one', () => {
  const ids = new SortedIds(['b', '\u{1F600}', 'a', '\u{FFFF}', 'ab', '\u{E000}', '\u{10000}', '\u{D7FF}'])
  expect(from(ids, '')).toEqual(['a', 'ab', 'b', '\u{D7FF}', '\u{E000}', '\u{FFFF}', '\u{10000}', '\u{1F600}'])
  expect(from(ids, 'aa')).toEqual(['ab', 'b', '\u{D7FF}', '\u{E000}', '\u{FFFF}', '\u{10000}', '\u{1F600}'])
})

it('merges the adds and deletes recorded since the last read, at the start, the end and between held ids', () => {
  const ids = new SortedIds(['b', 'd', 'f'])
  ids.add('a')
  ids.add('c')
  ids.add('g')
  ids.delete('d')
  expect(from(ids, '')).toEqual(['a', 'b', 'c', 'f', 'g'])
  ids.delete('a')
  ids.delete('g')
  ids.add('e')
  expect(from(ids, 'c')).toEqual(['c', 'e', 'f'])
})

it('reads as a sorted copy of what it holds after any changes, more of them than one join takes', () => {
  let state = 1
  // A fixed sequence, so a failure repeats.
  const next = () => (state = (state * 48271) % 2147483647)
  const held = new Set(Array.from({ length: 2000 }, () => `id-${next() % 50000}`))
  const ids = new SortedIds(held)
  for (const changes of [3, 100, 20000]) {
    for (let i = 0; i < changes; i++) {
      const id = `id-${next() % 50000}`
      if (held.has(id)) {
        held.delete(id)
        ids.delete(id)
      } else {
        held.add(id)
        ids.add(id)
      }
    }
    expect(from(ids, '')).toEqual([...held].sort(compareCodePoints))
  }
})
