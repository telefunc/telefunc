export { RoomWrites, runAfterHook }

import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
import { createDeferred } from '../../../utils/createDeferred.js'
import { unrefTimer } from '../../../utils/unrefTimer.js'
import type { CommitAccepted } from '../../backend/room/contract.js'
import { inProcessBinaryLaneKey } from '../../backend/room/lane-key.js'
import type { ChannelPublishAck } from '../../channel.js'
import { reportServerChannelError } from '../../server/channel.js'
import type { BinaryFrame } from '../binary.js'
import { ROOM_DM_ACK_TIMEOUT_MS } from './constants.js'
import {
  DM_FAILURE,
  participantGoneError,
  participantLeftError,
  roomClosedError,
  roomFailureError,
  toRoomFailure,
  ROOM_BUG_MESSAGE,
} from '../errors.js'
import { ownMetaArgument, senderOf } from '../model.js'
import type { AcceptedMeta, DmReply, RoomCtrlEnvelope, RoomDmEnvelope, SenderStamp } from '../protocol.js'
import type { RoomDataEnvelope } from './protocol.js'
import type { RoomModel, RoomState } from '../state.js'
import type { ParticipantMeta, RoomAckReceipt, RoomSendReceipt, Sender } from '../types.js'
import { memberCellKey } from './cells.js'
import { reportRoomError } from './errors.js'
import { SEMANTIC_LANE, commitControlOrThrow, commitRoomLaneOrThrow, commitToInbox, encodeRoomRecord } from './lanes.js'
import type { RoomIncarnation } from './lifecycle.js'
import { reapAndReadMembersById, storeDmReply, takeDmReply } from './membership.js'
import { dropMemberMetaGeneration, writeMemberMeta, type MemberMetaWrite } from './meta.js'
import type { RoomReplica } from './replica.js'
import type { RoomGuardHooks } from './statics.js'
assertIsNotBrowser()

/** The handle the writes go out through. */
type WritesHost = {
  readonly id: string
  readonly _incarnation: RoomIncarnation
  readonly _model: Pick<RoomModel, 'closed' | 'memberMetaSeq'>
  readonly _state: Pick<RoomState, 'getRemote'>
  readonly _replica: RoomReplica
}
/** What the writes read of the handle's own: its guards and its local participants. */
type WritesOf = {
  guards(): Partial<RoomGuardHooks> | null
  /** A local participant, whose meta a view that holds no roster yet doesn't hold. */
  local(id: string): { meta: ParticipantMeta; identity: string | null; _metaSeq: number } | undefined
}

/** A member's writes through one handle: each passes its guard, carries its sender's stamp, commits its cells with its
 *  event in one exchange, then runs its after-hook. */
class RoomWrites {
  /** The last publish in call order, by key: a member's id for its text, `meta:<id>` for its meta writes, and
   *  `inProcessBinaryLaneKey` for a binary lane. */
  private readonly _publishTurns = new Map<string, Promise<void>>()

  constructor(
    private readonly _host: WritesHost,
    private readonly _of: WritesOf,
  ) {}

  /** Full replace (`setMeta`). */
  async setMemberMeta(id: string, meta: ParticipantMeta): Promise<AcceptedMeta> {
    return await this._writeMemberMeta(id, { replace: ownMetaArgument(meta, 'setMeta() meta') })
  }

  /** Per-key merge (`setAttributes`); an `undefined` value deletes the key. */
  async mergeMemberMeta(id: string, attrs: ParticipantMeta): Promise<AcceptedMeta> {
    return await this._writeMemberMeta(id, { merge: ownMetaArgument(attrs, 'setAttributes() attributes') })
  }

  /** The member's writes, from its single owner here, each start once the one before it was sent, so they publish in
   *  seq order. */
  private async _writeMemberMeta(id: string, write: MemberMetaWrite): Promise<AcceptedMeta> {
    const host = this._host
    return await this._inCallOrder(`meta:${id}`, async (sent) => {
      const { update, hidden, replaced } = await writeMemberMeta(host._incarnation, id, write)
      const accepted: Extract<RoomCtrlEnvelope, { __r: 'p-meta' }> = { __r: 'p-meta', id, ...update }
      if (hidden) accepted.hidden = true
      host._replica.applyControl(accepted)
      const publishing = commitControlOrThrow(host.id, host._incarnation.inc, accepted)
      sent()
      await publishing
      if (replaced !== null) await dropMemberMetaGeneration(host._incarnation, id, replaced)
      return update
    })
  }

  async publishText(from: string, data: unknown, retain = false): Promise<ChannelPublishAck> {
    const host = this._host
    const { sender, commit } = await this._inCallOrder(from, async (sent) => {
      const sender = await this._admitPublish(from, data)
      const envelope: RoomDataEnvelope = { __r: 'data', from, data, ...this._stampOf(from, sender) }
      const record = encodeRoomRecord(envelope)
      const opts = { retain, requiredCellKeys: [memberCellKey(from)] }
      const committing = commitRoomLaneOrThrow(host.id, host._incarnation.inc, SEMANTIC_LANE, record, opts)
      sent()
      return { sender, commit: await committing }
    })
    return this._finishPublish(sender, data, commit)
  }

  /** `frame` is `framed` decoded; receivers trust its sender id, which every caller checked is the publisher's. */
  async publishBinaryFrame(frame: BinaryFrame, framed: Uint8Array): Promise<ChannelPublishAck> {
    const host = this._host
    const { from } = frame
    const lane = { kind: 'binary', member: from, track: frame.track } as const
    const { sender, commit } = await this._inCallOrder(inProcessBinaryLaneKey(from, lane.track), async (sent) => {
      const sender = await this._admitPublish(from, frame.payload)
      const planned = host._replica.plan.lanePlanned(from, lane.track)
      if (planned !== null) await planned
      host._replica.demand.published(from, lane.track)
      const opts = { retain: frame.retain, requiredCellKeys: [memberCellKey(from)] }
      const committing = commitRoomLaneOrThrow(host.id, host._incarnation.inc, lane, framed, opts)
      sent()
      return { sender, commit: await committing }
    })
    return await this._finishPublish(sender, frame.payload, commit)
  }

  /** A member's publishes on one lane leave in call order: each starts once the one before it was sent to the backend
   *  (its guard ran and its commit went out) or failed, never waiting on that commit's answer, which the backend
   *  keeps in sending order. */
  private async _inCallOrder<T>(key: string, publish: (sent: () => void) => Promise<T>): Promise<T> {
    const previous = this._publishTurns.get(key)
    const turn = createDeferred()
    this._publishTurns.set(key, turn.promise)
    try {
      await previous
      return await publish(turn.resolve)
    } finally {
      turn.resolve()
      if (this._publishTurns.get(key) === turn.promise) this._publishTurns.delete(key)
    }
  }

  private async _admitPublish(from: string, payload: unknown): Promise<Sender> {
    if (this._host._model.closed) throw roomClosedError(this._host.id)
    const sender = this._memberSender(from)
    const onBeforePublish = this._of.guards()?.onBeforePublish
    if (onBeforePublish) await onBeforePublish(sender, payload)
    return sender
  }

  private async _finishPublish(sender: Sender, payload: unknown, commit: CommitAccepted): Promise<ChannelPublishAck> {
    const receipt: Omit<ChannelPublishAck, 'key'> = { seq: commit.seq, timestamp: commit.timestamp }
    if (commit.receivers !== undefined) receipt.receivers = commit.receivers
    const onAfterPublish = this._of.guards()?.onAfterPublish
    if (onAfterPublish) await runAfterHook(() => onAfterPublish(sender, payload, receipt))
    return { key: this._host.id, ...receipt }
  }

  /** `sender` with the meta seq it reflects, as `_memberSender(from)` just read it. */
  private _stampOf(from: string, sender: Sender): SenderStamp {
    const seq = this._host._model.memberMetaSeq(from) ?? this._of.local(from)!._metaSeq
    const stamp: SenderStamp = { fromMeta: sender.meta, fromMetaSeq: seq }
    if (sender.identity !== null) stamp.fromIdentity = sender.identity
    return stamp
  }

  /** A member that left while its call waited has no sender to show a guard. */
  private _memberSender(from: string): Sender {
    const known = this._host._state.getRemote(from) ?? this._of.local(from)
    if (!known) throw participantLeftError()
    return senderOf(from, known.meta, known.identity)
  }

  async sendDm(from: string, to: string, data: unknown, ack: boolean): Promise<RoomSendReceipt | RoomAckReceipt> {
    return ack ? await this._sendDmAck(from, to, data) : await this._publishDm(from, to, data)
  }
  private async _sendDmAck(from: string, to: string, data: unknown): Promise<RoomAckReceipt> {
    const acks = this._host._replica.acks
    const ackId = crypto.randomUUID()
    const reply = new Promise<DmReply>((settle) => acks.add(ackId, { from, to, settle }))
    // Bounds the wait nothing else settles: a lost reply frame, or a recipient that never handled the DM.
    const timer = unrefTimer(setTimeout(() => void this._settleTimedOut(from, ackId), ROOM_DM_ACK_TIMEOUT_MS))
    let receipt: RoomSendReceipt
    try {
      receipt = await this._publishDm(from, to, data, ackId)
    } catch (err) {
      acks.delete(ackId)
      clearTimeout(timer)
      throw err
    }
    const settled = await reply
    clearTimeout(timer)
    if (!settled.ok) throw roomFailureError(settled)
    return { ...receipt, response: settled.result }
  }

  /** The recipient stores its reply before committing its frame, so a reply whose frame was lost is still read. */
  private async _settleTimedOut(from: string, ackId: string): Promise<void> {
    let reply: DmReply
    try {
      reply = (await takeDmReply(this._host.id, this._host._incarnation.inc, from, ackId)) ?? DM_FAILURE.timeout
    } catch (err) {
      reply = toRoomFailure(err, reportRoomError, ROOM_BUG_MESSAGE)
    }
    this._host._replica.acks.settle(ackId, reply)
  }

  private async _publishDm(from: string, to: string, data: unknown, ackId?: string): Promise<RoomSendReceipt> {
    const host = this._host
    if (host._model.closed) throw roomClosedError(host.id)
    const target = await this._resolveMember(to)
    if (!target) throw participantGoneError(to)
    const sender = this._memberSender(from)
    const onBeforeSend = this._of.guards()?.onBeforeSend
    if (onBeforeSend) await onBeforeSend(sender, target, data)
    const envelope: RoomDmEnvelope = { __r: 'dm', to, from, ...this._stampOf(from, sender), data }
    if (ackId) envelope.ackId = ackId
    const receipt = await commitToInbox(host.id, host._incarnation.inc, envelope, [from])
    if ('gone' in receipt) throw participantGoneError(receipt.gone)
    const info: RoomSendReceipt = { seq: receipt.seq, timestamp: receipt.timestamp }
    const onAfterSend = this._of.guards()?.onAfterSend
    if (onAfterSend) await runAfterHook(() => onAfterSend(sender, target, data, info))
    return info
  }

  /** A sender that left had its wait failed by its leave; only a closed room is an error. */
  async publishDmAck(to: string, ackId: string, reply: DmReply): Promise<unknown> {
    await storeDmReply(this._host.id, this._host._incarnation.inc, to, ackId, reply)
    return await commitToInbox(this._host.id, this._host._incarnation.inc, { __r: 'dm-ack', to, ackId, ...reply })
  }

  private async _resolveMember(id: string): Promise<Sender | null> {
    const remote = this._host._state.getRemote(id)
    if (remote) return senderOf(id, remote.meta, remote.identity)
    const [member] = await reapAndReadMembersById(this._host._incarnation, [id])
    return member === undefined ? null : senderOf(id, member.meta, member.identity ?? null)
  }
}

async function runAfterHook(hook: () => unknown): Promise<void> {
  try {
    await hook()
  } catch (error) {
    reportServerChannelError(error)
  }
}
