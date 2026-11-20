export { inboxRegistrations }
export type { InboxRegistration }

/** Test-only: what the authority of a room does with the next inbox registration it gets, by the authority's name. */
type InboxRegistration =
  | { kind: 'hold'; reached: PromiseWithResolvers<void>; release: Promise<void> }
  | { kind: 'refuse' }

const inboxRegistrations = new Map<string, InboxRegistration>()
