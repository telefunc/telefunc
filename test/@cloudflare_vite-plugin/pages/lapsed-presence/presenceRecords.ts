export { LAPSED_PRESENCE_REFUSED_FOR_MS, lapsedPresences }

// Long enough for the original lease to expire after 90 s.
const LAPSED_PRESENCE_REFUSED_FOR_MS = 100_000

const lapsedPresences = new Map<string, { lease: string; recordedAt: number; rerecorded: boolean }>()
