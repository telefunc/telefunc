export { defineParticipantField, participantFields, fieldValue, frozenFieldValue }
export type { ParticipantField, ParticipantFieldView }

import { getGlobalObject } from '../utils/getGlobalObject.js'
import { freezeMeta } from './freezeMeta.js'
import type { Room } from '../types.js'

/** What a package's field gets on each view of a room: the room, and how to set a member's value there. */
type ParticipantFieldView<T> = {
  room: Omit<Room, 'join' | 'getParticipants' | 'getParticipant'>
  /** Freezes `value` deeply; a value equal to the member's changes nothing, and one for a member the view doesn't
   *  know is dropped: the package's source sets it again with the member. */
  set(memberId: string, value: T): void
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
