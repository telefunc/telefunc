// What the memory store reads beside a room's cells, as each store does: for each cell under `of` it returns, the cell
// its reference names, in the same read, on every page of a prefix read; nothing for a cell with no reference or not
// under `of`, and `null` for a reference whose cell is absent.
import { expect, it } from 'vitest'
import { Room } from '../index.js'
import { CELL_PAGE_SIZE, type CellsBeside, type CellsRead } from './contract.js'
import { getRoomPlane } from './plane.js'

const BESIDE: CellsBeside = { of: 'tm:', reference: 'tml:', target: 'tl:' }
const encoder = new TextEncoder()
const decoder = new TextDecoder()

/** A cell per filler member, each referencing lease `L1`: with the three below, a prefix read takes two pages. */
const FILLERS = Array.from({ length: CELL_PAGE_SIZE }, (_, i) => `tm:f${String(i).padStart(4, '0')}`)

/** `tm:a` references a lease that is there, `tm:b` one that isn't, `tm:c` none; `tx:a` isn't under `of`. */
const CELLS: Record<string, string> = {
  'tm:a': 'A',
  'tml:a': 'L1',
  'tl:L1': 'lease one',
  'tm:b': 'B',
  'tml:b': 'L2',
  'tm:c': 'C',
  'tx:a': 'X',
  ...Object.fromEntries(
    FILLERS.flatMap((key) => [
      [key, 'F'],
      [`tml:${key.slice(3)}`, 'L1'],
    ]),
  ),
}

function besideText(read: CellsRead): Record<string, string | null> | 'none' {
  if (read.kind !== 'read' || read.beside === undefined) return 'none'
  return Object.fromEntries([...read.beside].map(([key, bytes]) => [key, bytes && decoder.decode(bytes)]))
}

it('reads the cell each reference names beside its cell, by key and on every page of a prefix read', async () => {
  const id = `beside-${crypto.randomUUID()}`
  await Room.create(id)
  const { direct } = getRoomPlane()
  const inc = (await direct.reads.readHead(id))?.currentInc
  if (typeof inc !== 'string') throw new Error(`room ${id} has no incarnation`)
  const mutations = Object.entries(CELLS).map(([key, text]) => ({ key, bytes: encoder.encode(text) }))
  expect((await direct.compareExchangeCells(id, inc, { expect: [], mutations })).kind).toBe('committed')

  const keys = ['tm:a', 'tm:b', 'tm:c', 'tx:a', 'tm:absent']
  const byKey = await direct.reads.readCells(id, inc, { keys, beside: BESIDE })
  const pages: (Record<string, string | null> | 'none')[] = []
  for await (const page of direct.reads.readCellPages(id, inc, 'tm:', BESIDE)) pages.push(besideText(page))
  const plain = await direct.reads.readCells(id, inc, { keys })
  await Room.close(id)

  expect(besideText(byKey)).toEqual({ 'tm:a': 'lease one', 'tm:b': null })
  expect({ pages: pages.length > 1, without: pages.filter((page) => page === 'none').length }).toEqual({
    pages: true,
    without: 0,
  })
  expect(Object.assign({}, ...pages)).toEqual({
    'tm:a': 'lease one',
    'tm:b': null,
    ...Object.fromEntries(FILLERS.map((key) => [key, 'lease one'])),
  })
  expect(besideText(plain)).toBe('none')
})
