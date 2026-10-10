export { readRoomMeta, writeRoomMeta }
export type { StoredRoomMeta, RoomMetaWrite }

import { getRoomBackend } from '../../backend/install.js'
import type { CellMutation, CellsCx } from '../../backend/room/contract.js'
import { decodeCellCounter } from '../../backend/room/semantics.js'
import { roomClosedError } from '../errors.js'
import { mergeAttributes } from '../model.js'
import type { RoomMeta } from '../types.js'
import { META_CELL_PREFIX, META_KEY_CELL_PREFIX, META_SEQ_CELL_KEY, metaKeyCellKey, metaKeyOfCellKey } from './cells.js'
import { CX_CONFLICT, retryCompareExchange } from './cx.js'
import { decodeRoomRecord, encodeRoomRecord, publishCtrl } from './lanes.js'

/** `seq` counts the writes that made `meta`, so it orders `update` events. */
type StoredRoomMeta = { meta: RoomMeta; seq: number }
type RoomMetaWrite = { replace: RoomMeta } | { merge: RoomMeta }

/** Null once the incarnation is gone. Until its first write, the meta is the one the room was created with. */
async function readRoomMeta(roomId: string, inc: string, initial: RoomMeta): Promise<StoredRoomMeta | null> {
  const read = await getRoomBackend().readCells(roomId, inc, { prefix: META_CELL_PREFIX })
  if ('staleInc' in read) return null
  const seq = read.cells.get(META_SEQ_CELL_KEY)
  if (seq === undefined) return { meta: initial, seq: 0 }
  const entries = [...read.cells].flatMap(([key, raw]) =>
    key === META_SEQ_CELL_KEY ? [] : [[metaKeyOfCellKey(key), decodeRoomRecord(raw)] as const],
  )
  return { meta: Object.fromEntries(entries), seq: decodeCellCounter(seq).value }
}

/** Once the room's meta was first written, a write is one exchange that expects nothing, so concurrent writes never
 *  conflict; the first stores the whole meta, and only the writes racing it conflict, once. */
async function writeRoomMeta(roomId: string, inc: string, initial: RoomMeta, write: RoomMetaWrite): Promise<void> {
  const backend = getRoomBackend()
  await retryCompareExchange(async () => {
    const read = await backend.readCells(roomId, inc, { keys: [META_SEQ_CELL_KEY] })
    if ('staleInc' in read) throw roomClosedError(roomId)
    let cx: CellsCx
    if (!read.cells.has(META_SEQ_CELL_KEY)) {
      const meta = 'replace' in write ? write.replace : mergeAttributes(initial, write.merge)
      cx = {
        expect: [{ key: META_SEQ_CELL_KEY, version: null }],
        mutations: metaMutations(meta),
        counts: [{ key: META_SEQ_CELL_KEY, delta: 1, create: true }],
      }
    } else {
      const counts = [{ key: META_SEQ_CELL_KEY, delta: 1 }]
      cx =
        'replace' in write
          ? { expect: [], clear: [META_KEY_CELL_PREFIX], mutations: metaMutations(write.replace), counts }
          : { expect: [], mutations: metaMutations(write.merge, true), counts }
    }
    const result = await backend.compareExchangeCells(roomId, inc, cx)
    if (result === 'stale-inc') throw roomClosedError(roomId)
    return result === 'conflict' ? CX_CONFLICT : undefined
  })
  // Read after the write, so the newest of the concurrent writes publishes the newest meta.
  const stored = await readRoomMeta(roomId, inc, initial)
  if (stored === null) throw roomClosedError(roomId)
  await publishCtrl(roomId, inc, { __r: 'update', ...stored })
}

/** With `undefinedRemoves`, an `undefined` value deletes its key, as in `mergeAttributes`. */
function metaMutations(meta: RoomMeta, undefinedRemoves = false): CellMutation[] {
  return Object.entries(meta).map(([key, value]) => ({
    key: metaKeyCellKey(key),
    bytes: undefinedRemoves && value === undefined ? null : encodeRoomRecord(value),
  }))
}
