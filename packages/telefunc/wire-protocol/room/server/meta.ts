export { readRoomMeta, writeRoomMeta }
export type { StoredRoomMeta, RoomMetaWrite }

import { getRoomBackend } from '../../backend/install.js'
import type { CellMutation, CellsCx } from '../../backend/room/contract.js'
import { decodeCellCounter } from '../../backend/room/semantics.js'
import { roomClosedError } from '../errors.js'
import { mergeAttributes } from '../model.js'
import type { RoomConfigRecord } from '../protocol.js'
import type { RoomMeta } from '../types.js'
import { META_CELL_PREFIX, META_KEY_CELL_PREFIX, META_SEQ_CELL_KEY, metaKeyCellKey, metaKeyOfCellKey } from './cells.js'
import { CX_CONFLICT, retryCompareExchange } from './cx.js'
import { decodeRoomRecord, encodeRoomRecord, publishCtrl } from './lanes.js'

/** `seq` counts the writes that made `meta`, so it orders `update` events. */
type StoredRoomMeta = { meta: RoomMeta; seq: number }
type RoomMetaWrite = { replace: RoomMeta } | { merge: RoomMeta }
/** A key's place: the `meta:seq` the write that added it read, which every later write reads higher, and its index in
 *  that write. Keys that concurrent writes added tie, and go by key. */
type MetaKeyCell = { value: unknown; place: [seq: number, index: number] }

/** Null once the incarnation is gone. Until its first write, the meta is the one the room was created with. */
async function readRoomMeta(roomId: string, { inc, meta }: RoomConfigRecord): Promise<StoredRoomMeta | null> {
  const read = await getRoomBackend().readCells(roomId, inc, { prefix: META_CELL_PREFIX })
  if ('staleInc' in read) return null
  const seq = read.cells.get(META_SEQ_CELL_KEY)
  if (seq === undefined) return { meta, seq: 0 }
  const cells = [...read.cells].flatMap(([key, raw]) =>
    key === META_SEQ_CELL_KEY ? [] : [{ key: metaKeyOfCellKey(key), ...decodeRoomRecord<MetaKeyCell>(raw) }],
  )
  cells.sort((a, b) => a.place[0] - b.place[0] || a.place[1] - b.place[1] || (a.key < b.key ? -1 : 1))
  return { meta: Object.fromEntries(cells.map(({ key, value }) => [key, value])), seq: decodeCellCounter(seq).value }
}

/** Once the room's meta was first written, a write is one exchange that expects nothing, so concurrent writes never
 *  conflict; the first stores the whole meta, and only the writes racing it conflict, once. A merge reads its keys
 *  with `meta:seq`, so a key already there keeps its place. */
async function writeRoomMeta(roomId: string, config: RoomConfigRecord, write: RoomMetaWrite): Promise<void> {
  const { inc } = config
  const backend = getRoomBackend()
  const merged = 'merge' in write ? Object.keys(write.merge).map(metaKeyCellKey) : []
  await retryCompareExchange(async () => {
    const read = await backend.readCells(roomId, inc, { keys: [META_SEQ_CELL_KEY, ...merged] })
    if ('staleInc' in read) throw roomClosedError(roomId)
    const seq = read.cells.get(META_SEQ_CELL_KEY)
    let cx: CellsCx
    if (seq === undefined) {
      const meta = 'replace' in write ? write.replace : mergeAttributes(config.meta, write.merge)
      cx = {
        expect: [{ key: META_SEQ_CELL_KEY, version: null }],
        mutations: metaMutations(meta, 0),
        counts: [{ key: META_SEQ_CELL_KEY, delta: 1, create: true }],
      }
    } else {
      const counts = [{ key: META_SEQ_CELL_KEY, delta: 1 }]
      const count = decodeCellCounter(seq).value
      cx =
        'replace' in write
          ? { expect: [], clear: [META_KEY_CELL_PREFIX], mutations: metaMutations(write.replace, count), counts }
          : { expect: [], mutations: metaMutations(write.merge, count, read.cells), counts }
    }
    const result = await backend.compareExchangeCells(roomId, inc, cx)
    if (result === 'stale-inc') throw roomClosedError(roomId)
    return result === 'conflict' ? CX_CONFLICT : undefined
  })
  // Read after the write, so the newest of the concurrent writes publishes the newest meta.
  const stored = await readRoomMeta(roomId, config)
  if (stored === null) throw roomClosedError(roomId)
  await publishCtrl(roomId, inc, { __r: 'update', ...stored })
}

/** A new key's place is `seq` and its index here. With the `merging` cells read, a key found there keeps its place,
 *  and an `undefined` value deletes its key, as in `mergeAttributes`. */
function metaMutations(meta: RoomMeta, seq: number, merging?: ReadonlyMap<string, Uint8Array>): CellMutation[] {
  return Object.entries(meta).map(([key, value], index) => {
    const cellKey = metaKeyCellKey(key)
    if (merging !== undefined && value === undefined) return { key: cellKey, bytes: null }
    const found = merging?.get(cellKey)
    const place: MetaKeyCell['place'] = found === undefined ? [seq, index] : decodeRoomRecord<MetaKeyCell>(found).place
    return { key: cellKey, bytes: encodeRoomRecord({ value, place } satisfies MetaKeyCell) }
  })
}
