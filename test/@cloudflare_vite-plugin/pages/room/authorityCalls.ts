export { authorityCalls, authorityClocks }
export type { AuthorityCall }

/** Test-only: what the authority of a room does with the next call it gets that `matches`, by the authority's name:
 *  holds it where it reached the authority until the test releases it, or refuses it with `refusal`. */
type AuthorityCall = { matches: (method: string, args: unknown[]) => boolean } & (
  | { kind: 'hold'; reached: PromiseWithResolvers<void>; release: Promise<void> }
  | { kind: 'refuse'; refusal: string }
)

const authorityCalls = new Map<string, AuthorityCall>()

/** Test-only: by the authority's name, how many ms ahead of the real clock the calls it serves read the time. */
const authorityClocks = new Map<string, number>()
