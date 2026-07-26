import type { HeadCx, LaneId } from '../../spi.js'
import type { HeadNextWire } from '../../../server/adapter/cloudflare/room/do.js'

export type SessionRoomCommand =
  | { kind: 'read-head'; roomId: string }
  | { kind: 'compare-exchange-head'; roomId: string; cx: HeadCx; next: HeadNextWire }
  | { kind: 'read-cells'; roomId: string; inc: string; selection: { keys: string[] } | { prefix: string } }
  | {
      kind: 'compare-exchange-cells'
      roomId: string
      inc: string
      revision: string
      mutations: Array<{ key: string; set?: { bytesB64: string; ttlMs?: number } }>
    }
  | {
      kind: 'commit-lane'
      roomId: string
      inc: string
      lane: LaneId
      payloadB64: string
      options?: { retain?: boolean; closingLease?: string }
    }
  | { kind: 'read-retained'; roomId: string; inc: string; lane: LaneId }
  | { kind: 'list-retained'; roomId: string; inc: string }
  | { kind: 'delete-retained'; roomId: string; inc: string; lane?: LaneId; options?: { ifSeq?: number } }
  | { kind: 'list-generations'; roomId: string }
  | { kind: 'drop-generation'; roomId: string; inc: string }
  | { kind: 'directory-put'; roomId: string; incTag: string }
  | { kind: 'directory-delete'; roomId: string; incTag: string }
  | { kind: 'directory-list'; prefix: string; cursor?: string }
  | { kind: 'run-order-maintenance'; roomId: string }
  | { kind: 'reconstruct-order-authority'; roomId: string }
  | {
      kind: 'seed-order-watermark'
      roomId: string
      inc: string
      lane: LaneId
      seq: number
      timestamp: number
    }

export type SessionRoomReply = { ok: true; value: unknown } | { ok: false; error: string }
