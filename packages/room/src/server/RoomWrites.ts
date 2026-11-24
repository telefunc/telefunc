export { RoomWrites, runAfterHook }
export type { RoomWritesOptions, RoomWritesOwner }

import { assertIsNotBrowser } from '../utils/assertIsNotBrowser.js'
import type { CommitAccepted, LaneId } from '../backend/contract.js'
import { getRoomPlane } from '../backend/plane.js'
import type { RoomStore } from '../backend/RoomStore.js'
import { encodeLaneKey } from '../backend/laneKey.js'
import {
  type ChannelPublishAck,
  handleCallbackBug,
  createDeferred,
  isChannelOverflowError,
  unrefTimer,
} from 'telefunc/__internal'
import { publicTrack, type BinaryFrame } from '../shared/binary.js'
import { ROOM_DM_ACK_TIMEOUT_MS } from './constants.js'
import {
  DM_FAILURE,
  messageLostError,
  participantGoneError,
  participantLeftError,
  roomClosedError,
  roomFailureError,
  toRoomFailure,
  ROOM_BUG_MESSAGE,
} from '../shared/errors.js'
import { senderSnapshot } from '../shared/arguments.js'
import type {
  MemberMetaUpdate,
  WholeMemberUpdate,
  RoomResult,
  RoomDmEnvelope,
  SenderStamp,
  MemberSnapshot,
} from '../shared/protocol.js'
import type { RoomDataEnvelope, RoomDmAckEnvelope, RoomIncarnation } from './protocol.js'
import type { RoomModel } from '../shared/RoomModel.js'
import type { ParticipantMeta, RoomSendReceipt, Sender } from '../types.js'
import { hostedMembers } from './store/cells.js'
import { reportRoomError } from './reportRoomError.js'
import {
  SEMANTIC_LANE,
  acceptedOrThrow,
  commitFitsBeforeWriting,
  commitLane,
  encodeRoomRecord,
  inboxOf,
  type CommitFitsBeforeWriting,
  type CommitsOf,
  type SizedCommit,
} from './lanes/lanes.js'
import { reapAndReadMembersById } from './store/roster.js'
import { dropDmReply, readDmReply, storeDmReply } from './store/dmReplies.js'
import { setMemberConnection, setMemberMeta, type MemberMetaWrite } from './store/meta.js'
import type { RoomReplica } from './RoomReplica.js'
import type { PendingDmAcks } from './PendingDmAcks.js'
import type { RoomGuardHooks } from './roomStatics.js'
assertIsNotBrowser()

/** The handle the writes go out through: its guards pass them. */
interface RoomWritesOwner {
  readonly guards: Partial<RoomGuardHooks> | null
}

type RoomWritesOptions = {
  owner: RoomWritesOwner
  incarnation: RoomIncarnation
  model: Pick<RoomModel, 'closed' | 'acceptedMeta' | 'entry'>
  /** The view the writes commit through: it applies a member's committed meta or connection event at once, and admits
   *  a binary publish's track. */
  replica: Pick<RoomReplica, 'applyControl' | 'admitTrack'>
  /** The view's in-flight ack DMs, which a leave of either end or the room's close settles too. */
  acks: PendingDmAcks
  /** The handle's local participants, whose stamps a view that holds no roster yet doesn't hold. */
  localParticipants: ReadonlyMap<
    string,
    { meta: ParticipantMeta; identity: string | null; _acceptedMeta: WholeMemberUpdate }
  >
}

/** A member's writes through one handle: each passes its guard, carries its sender's stamp, commits its cells with its
 *  event in one exchange, then runs its after-hook. */
class RoomWrites {
  /** The last publish in call order, by key: a member's id for its text, `meta:<id>` for its meta writes, and
   *  its lane key for a binary lane. */
  private readonly publishTurns = new Map<string, Promise<void>>()

  private readonly owner: RoomWritesOwner
  private readonly incarnation: RoomIncarnation
  private readonly model: RoomWritesOptions['model']
  private readonly replica: RoomWritesOptions['replica']
  private readonly acks: PendingDmAcks
  private readonly localParticipants: RoomWritesOptions['localParticipants']

  constructor({ owner, incarnation, model, replica, acks, localParticipants }: RoomWritesOptions) {
    this.owner = owner
    this.incarnation = incarnation
    this.model = model
    this.replica = replica
    this.acks = acks
    this.localParticipants = localParticipants
  }

  /** Full replace (`setMeta`), of a meta owned already. */
  async setMemberMeta(id: string, meta: ParticipantMeta): Promise<MemberMetaUpdate> {
    return await this.writeMetaInOrder(id, { replace: meta })
  }

  /** Per-key merge (`updateMeta`), of keys owned already; an `undefined` value deletes the key. */
  async mergeMemberMeta(id: string, attrs: ParticipantMeta): Promise<MemberMetaUpdate> {
    return await this.writeMetaInOrder(id, { merge: attrs })
  }

  /** The connection of the page holding the member, which its holder here saw change. */
  async setMemberConnected(id: string, connected: boolean): Promise<void> {
    await this.inCallOrder(`meta:${id}`, async () => {
      // Its holder saw the page's connection change: work that tells no answer.
      const written = await setMemberConnection(getRoomPlane().direct, this.incarnation, id, connected)
      if (written !== null) this.replica.applyControl(written.event)
    })
  }

  /** The member's writes, from its single owner here, each start once the one before it committed, so they publish
   *  in seq order. */
  private async writeMetaInOrder(id: string, write: MemberMetaWrite): Promise<MemberMetaUpdate> {
    return await this.inCallOrder(`meta:${id}`, async () => {
      const { update, event } = await getRoomPlane().call((store) => setMemberMeta(store, this.incarnation, id, write))
      this.replica.applyControl(event)
      return update
    })
  }

  async publishText(from: string, data: unknown, retain = false): Promise<ChannelPublishAck> {
    // One call: the guard, then the commit; the after-hook runs once it answered.
    const { sender, commit } = await this.inCallOrder(from, (sent) =>
      getRoomPlane().call(async (store) => {
        const { sender, stamp } = await this.admitPublish(from, data, null)
        const envelope: RoomDataEnvelope = { __r: 'data', from, data, ...stamp }
        const record = encodeRoomRecord(envelope)
        const target = { roomId: this.incarnation.id, inc: this.incarnation.inc, lane: SEMANTIC_LANE }
        const opts = { retain, owner: from, required: hostedMembers(from) }
        const committing = commitLane(store, target, record, opts)
        sent()
        return { sender, commit: acceptedOrThrow(this.incarnation.id, await committing) }
      }),
    )
    return await this.finishPublish(sender, data, null, commit)
  }

  /** `frame` is `framed` decoded, and `message` what its guards see: its bytes, or a text frame's message. Receivers
   *  trust its sender id, which every caller checked is the publisher's. */
  async publishBinaryFrame(frame: BinaryFrame, framed: Uint8Array, message: unknown): Promise<ChannelPublishAck> {
    const { from } = frame
    const track = publicTrack(frame.track)
    const lane = { kind: 'binary', member: from, track: frame.track } as const
    // One call: the guard, the track's admission, then the commit; the after-hook runs once it answered.
    const { sender, commit } = await this.inCallOrder(encodeLaneKey(lane), (sent) =>
      getRoomPlane().call(async (store) => {
        const { sender } = await this.admitPublish(from, message, track)
        const admitted = this.replica.admitTrack(from, lane.track)
        if (admitted !== null) await admitted
        const target = { roomId: this.incarnation.id, inc: this.incarnation.inc, lane }
        const opts = { retain: frame.retain, owner: from, required: hostedMembers(from) }
        const committing = commitLane(store, target, framed, opts)
        sent()
        return { sender, commit: acceptedOrThrow(this.incarnation.id, await committing) }
      }),
    )
    return await this.finishPublish(sender, message, track, commit)
  }

  /** A member's publishes on one lane leave in call order: each starts once the one before it was sent to the backend
   *  (its guard ran and its commit went out) or failed, never waiting on that commit's answer, which the backend
   *  keeps in sending order. */
  private async inCallOrder<T>(key: string, publish: (sent: () => void) => Promise<T>): Promise<T> {
    const previous = this.publishTurns.get(key)
    const turn = createDeferred()
    this.publishTurns.set(key, turn.promise)
    try {
      await previous
      return await publish(turn.resolve)
    } finally {
      turn.resolve()
      if (this.publishTurns.get(key) === turn.promise) this.publishTurns.delete(key)
    }
  }

  /** The sender as its guard sees it, with its stamp. */
  private async admitPublish(
    from: string,
    payload: unknown,
    track: string | null,
  ): Promise<{ sender: Sender; stamp: SenderStamp }> {
    if (this.model.closed) throw roomClosedError(this.incarnation.id)
    const sender = this.memberSender(from)
    const stamp = this.stampedSender(from, sender)
    const onBeforePublish = this.owner.guards?.onBeforePublish
    if (onBeforePublish) await onBeforePublish(sender, payload, { track })
    return { sender, stamp }
  }

  private async finishPublish(
    sender: Sender,
    payload: unknown,
    track: string | null,
    commit: CommitAccepted,
  ): Promise<ChannelPublishAck> {
    const receipt: Omit<ChannelPublishAck, 'key'> = { seq: commit.seq, timestamp: commit.timestamp }
    const onAfterPublish = this.owner.guards?.onAfterPublish
    if (onAfterPublish) await runAfterHook(() => onAfterPublish(sender, payload, { ...receipt, track }))
    return { key: this.incarnation.id, ...receipt }
  }

  /** `sender` with the meta seq it reflects, read before a guard waits: a meta write meanwhile takes the next seq. */
  private stampedSender(from: string, sender: Sender): SenderStamp {
    const accepted = this.model.acceptedMeta(from) ?? this.localParticipants.get(from)!._acceptedMeta
    const stamp: SenderStamp = { fromMeta: sender.meta, fromMetaSeq: accepted.seq }
    if (sender.identity !== null) stamp.fromIdentity = sender.identity
    if (accepted.disconnected) stamp.fromDisconnected = true
    return stamp
  }

  /** A member that left while its call waited has no sender to show a guard. */
  private memberSender(from: string): Sender {
    const entry = this.model.entry(from)
    if (entry) return senderSnapshot(from, entry.meta.object, entry.identity)
    const local = this.localParticipants.get(from)
    if (!local) throw participantLeftError()
    return senderSnapshot(from, local.meta, local.identity)
  }

  /** The receipt, or with `ack` the recipient's reply. */
  async sendDm(from: string, to: string, data: unknown, ack: boolean): Promise<unknown> {
    return ack ? await this.sendDmAck(from, to, data) : await this.publishDm(from, to, data)
  }

  /** A package's message, through the app's send guards as any DM. */
  async sendPrivate(from: string, to: string, field: string, data: unknown): Promise<void> {
    await this.publishDm(from, to, data, { field })
  }

  /** One call: the DM, its wait for the recipient's reply, and at the wait's timeout the reply's read. */
  private async sendDmAck(from: string, to: string, data: unknown): Promise<unknown> {
    const ackId = crypto.randomUUID()
    const acks = this.acks
    const reply = new Promise<RoomResult>((settle) => acks.add(ackId, { from, to, settle }))
    return await getRoomPlane().call(async (store) => {
      // Bounds the wait nothing else settles: a lost reply frame, or a recipient that never handled the DM.
      const timer = unrefTimer(setTimeout(() => void this.settleTimedOut(store, from, ackId), ROOM_DM_ACK_TIMEOUT_MS))
      try {
        await this.publishDmIn(store, from, to, data, { ackId })
      } catch (err) {
        acks.delete(ackId)
        clearTimeout(timer)
        throw err
      }
      const settled = await reply
      clearTimeout(timer)
      if (!settled.ok) throw roomFailureError(settled)
      return settled.value
    })
  }

  /** The recipient stores its reply before committing its frame, so a reply whose frame was lost is still read, in the
   *  call `store` is of. The stored reply's delete is cleanup, which tells that call nothing. */
  private async settleTimedOut(store: RoomStore, from: string, ackId: string): Promise<void> {
    const { id, inc } = this.incarnation
    let reply: RoomResult
    try {
      const stored = await readDmReply(store, id, inc, from, ackId)
      if (stored !== null) await dropDmReply(getRoomPlane().direct, id, inc, from, ackId)
      reply = stored ?? DM_FAILURE.timeout
    } catch (err) {
      reply = toRoomFailure(err, reportRoomError, ROOM_BUG_MESSAGE)
    }
    this.acks.settle(ackId, reply)
  }

  private async publishDm(
    from: string,
    to: string,
    data: unknown,
    options: { field?: string } = {},
  ): Promise<RoomSendReceipt> {
    return await getRoomPlane().call((store) => this.publishDmIn(store, from, to, data, options))
  }

  /** The DM as steps of the call `store` is of: the reap that finding the recipient may do, its send guard, the send,
   *  and its after-send hook. */
  private async publishDmIn(
    store: RoomStore,
    from: string,
    to: string,
    data: unknown,
    { ackId, field }: { ackId?: string; field?: string } = {},
  ): Promise<RoomSendReceipt> {
    if (this.model.closed) throw roomClosedError(this.incarnation.id)
    const inbox = inboxOf(this.incarnation.id, this.incarnation.inc, to)
    const dm = new ParticipantDm(inbox.lane, () => {
      const envelope: RoomDmEnvelope = {
        __r: 'dm',
        to,
        from,
        ...this.stampedSender(from, this.memberSender(from)),
        data,
      }
      if (ackId) envelope.ackId = ackId
      if (field !== undefined) envelope.field = field
      return envelope
    })
    const target = await this.resolveMember(to, store, commitFitsBeforeWriting(store, dm))
    if (!target) throw participantGoneError(to)
    // Its size is checked before the guard, whether the view or the store listed the recipient.
    dm.checked(store)
    const sender = this.memberSender(from)
    const onBeforeSend = this.owner.guards?.onBeforeSend
    if (onBeforeSend) {
      await onBeforeSend(sender, target, data)
      dm.guarded()
    }
    const payload = dm.bytes()
    const committed = await commitLane(store, inbox, payload, { required: hostedMembers(from, to) })
    const receipt = acceptedOrThrow(this.incarnation.id, committed)
    if (receipt.lost !== undefined) throw messageLostError()
    const info: RoomSendReceipt = { seq: receipt.seq, timestamp: receipt.timestamp }
    const onAfterSend = this.owner.guards?.onAfterSend
    if (onAfterSend) await runAfterHook(() => onAfterSend(sender, target, data, info))
    return info
  }

  /** A sender that left had its wait failed by its leave; only a closed room is an error. The stored reply and the
   *  frame are the same bytes, so whichever the sender reads first, it reads the same answer. */
  async publishDmAck(to: string, ackId: string, reply: RoomResult): Promise<void> {
    const { id, inc } = this.incarnation
    const { direct } = getRoomPlane()
    const target = inboxOf(id, inc, to)
    const frame = dmAckFrame(direct, target.lane, { __r: 'dm-ack', to, ackId, ...reply })
    await storeDmReply(direct, id, inc, to, ackId, frame)
    const acked = await commitLane(direct, target, frame, { required: hostedMembers(to) })
    if (acked.kind === 'stale-incarnation') throw roomClosedError(id)
  }

  /** Its reads and writes, a reap of a lapsed lease among them, are steps of the call `store` is of; a lookup in the
   *  store runs `fits` before them. */
  private async resolveMember(id: string, store: RoomStore, fits: CommitFitsBeforeWriting): Promise<Sender | null> {
    const entry = this.model.entry(id)
    if (entry) return senderSnapshot(id, entry.meta.object, entry.identity)
    const [member] = await reapAndReadMembersById(store, this.incarnation, [id], fits)
    return member === undefined ? null : senderSnapshot(id, member.meta, member.identity ?? null)
  }
}

/** A participant's DM to one member, checked for its size before the send guard runs: by the lookup that finds the
 *  member in the store, before that lookup's reap, or else by `checked()` once the view lists it. A guard may change
 *  the message it is given, so where one ran, the message it let through is encoded again, and its commit applies the
 *  store's size rule to it; else the bytes checked are the bytes committed. */
class ParticipantDm implements CommitsOf {
  private message: RoomDmEnvelope | null = null
  private encoded: Uint8Array | null = null
  private sized = false

  /** `envelope` is made once, with the sender as it is before the guard waits. */
  constructor(
    private readonly lane: LaneId,
    private readonly envelope: () => RoomDmEnvelope,
  ) {}

  commits(members: readonly MemberSnapshot[]): SizedCommit[] {
    if (members.length === 0) return []
    this.sized = true
    return [{ lane: this.lane, payload: this.bytes(), kept: 'delivered' }]
  }

  /** Checks the envelope against `store`'s size rule unless the lookup did. */
  checked(store: RoomStore): void {
    if (!this.sized) store.assertCommittable(this.lane, this.bytes(), 'delivered')
    this.sized = true
  }

  /** The send guard ran on the message the envelope holds: it is encoded again. */
  guarded(): void {
    this.encoded = null
  }

  bytes(): Uint8Array {
    this.message ??= this.envelope()
    this.encoded ??= encodeRoomRecord(this.message)
    return this.encoded
  }
}

/** `envelope` encoded once; one the store would refuse on `lane` is replaced by its refusal, as the sender's answer. */
function dmAckFrame(store: RoomStore, lane: LaneId, envelope: RoomDmAckEnvelope): Uint8Array {
  const frame = encodeRoomRecord(envelope)
  try {
    store.assertCommittable(lane, frame, 'stored')
    return frame
  } catch (error) {
    if (!isChannelOverflowError(error)) throw error
    const { to, ackId } = envelope
    const overflow: RoomDmAckEnvelope = {
      __r: 'dm-ack',
      to,
      ackId,
      ok: false,
      kind: 'overflow',
      message: error.message,
    }
    return encodeRoomRecord(overflow)
  }
}

async function runAfterHook(hook: () => unknown): Promise<void> {
  try {
    await hook()
  } catch (error) {
    handleCallbackBug(error)
  }
}
