// For packages built on Room, never for apps: what a package adds to every participant, its private messages, and who
// receives a group of a participant's tracks.
export { defineParticipantField, sendPrivate } from './shared/participantFields.js'
export type { ParticipantField, ParticipantFieldView } from './shared/participantFields.js'
export { watchersOf } from './shared/ParticipantBase.js'
