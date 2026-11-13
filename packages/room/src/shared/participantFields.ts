export { defineParticipantField, sendPrivate, participantFields, fieldValue, frozenFieldValue }
export type { ParticipantField, ParticipantFieldView }

import { getGlobalObject } from '../utils/getGlobalObject.js'
import { freezeMeta } from './freezeMeta.js'
import { recipientId } from './arguments.js'
import type { ParticipantBase } from './ParticipantBase.js'
import type { LocalParticipant, Room, Sender } from '../types.js'

/** What a package's field gets on each view of a room: the room, how to set a member's value there, and the package's
 *  messages. */
type ParticipantFieldView<T> = {
  room: Omit<Room, 'join' | 'getParticipants' | 'getParticipant'>
  /** Freezes `value` deeply; a value equal to the member's changes nothing, and one for a member the view doesn't
   *  know is dropped: the package's source sets it again with the member. */
  set(memberId: string, value: T): void
  /** The package's messages to a member this view holds (`sendPrivate()`), by the sender's id. */
  onPrivate(callback: (from: string, data: unknown) => void): () => void
}

/** A value a package keeps on every participant: `initial` until the package sets one, and `attach`, which runs on each
 *  view of a room while it is in use (a page's until it closes, a server's while something besides the field observes
 *  it), registers its listeners before it returns, and returns what removes them. */
type ParticipantField<T> = {
  initial: T
  attach(view: ParticipantFieldView<T>): () => void
}

// One registry for every copy of Room in the process: a second copy of the package defines its fields again.
const { fields } = getGlobalObject('shared/participantFields.ts', () => ({
  fields: new Map<string, ParticipantField<unknown>>(),
}))

/** `name` on every participant, and on every participant of `room.snapshot()`, as the package sets it. */
function defineParticipantField<T>(name: string, field: ParticipantField<T>): void {
  fields.set(name, {
    initial: frozenFieldValue(field.initial),
    attach: field.attach as ParticipantField<unknown>['attach'],
  })
}

/** A package's message to `to`, which `to`'s side hands to the field `field`'s `onPrivate()`, never to the app's
 *  `listen()`. It goes through the room's send guards as any message a member sends. */
function sendPrivate(me: LocalParticipant, to: string | Sender, field: string, data: unknown): Promise<void> {
  return (me as ParticipantBase)._sendPrivate(recipientId(to), field, data)
}

function participantFields(): ReadonlyMap<string, ParticipantField<unknown>> {
  return fields
}

/** A member's value of the field `name`: the one set, else the field's initial value. */
function fieldValue(fieldsSet: Readonly<Record<string, unknown>>, name: string): unknown {
  return name in fieldsSet ? fieldsSet[name] : fields.get(name)?.initial
}

function frozenFieldValue<T>(value: T): T {
  return typeof value === 'object' && value !== null ? freezeMeta(value) : value
}
