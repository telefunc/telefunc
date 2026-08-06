// The single source of truth for game rules and geometry — imported by the authoritative server
// simulation (server/sim/*), the client renderer (app/engine/*), and the HUD. Keeping one table
// on both sides is what lets the client derive fog-of-war vision from its own units without the
// server shipping a fog mask (see app/engine/fog.ts): both sides compute sight from the same
// numbers, so "what the client draws as visible" is exactly "what the server chose to reveal".

export type Team = 0 | 1 | 2 // 0 = neutral, 1 = red, 2 = blue
export const NEUTRAL = 0
export const RED = 1
export const BLUE = 2

// Entity kinds (must fit u8 on the wire — see shared/protocol.ts).
export const Kind = {
  HQ: 1,
  BARRACKS: 2,
  FACTORY: 3,
  DEPOT: 4,
  WORKER: 10,
  SOLDIER: 11,
  TANK: 12,
  CRYSTAL: 20,
} as const
export type Kind = (typeof Kind)[keyof typeof Kind]

// Per-entity state (u8) — drives the client's animation/annotation, not just debugging.
export const State = {
  IDLE: 0,
  MOVE: 1,
  ATTACK: 2,
  GATHER: 3,
  RETURN: 4,
  BUILD: 5,
  CONSTRUCTING: 6,
} as const
export type State = (typeof State)[keyof typeof State]

// Ephemeral per-tick visual events (u8) — tracers, deaths, hits. Visibility-filtered per audience,
// carried in the same binary frame, rendered as short-lived particles then dropped.
export const Ev = {
  SHOT: 0,
  EXPLOSION: 1,
  HIT: 2,
  COMPLETE: 3,
} as const
export type Ev = (typeof Ev)[keyof typeof Ev]

// World geometry. Coordinates are world units, encoded as i16 on the wire, so everything must fit
// in [-32768, 32767]. The map is a 3200×3200 field with bases at opposite corners.
export const MAP_W = 3200
export const MAP_H = 3200
export const TICK_HZ = 10 // authoritative sim + broadcast rate; clients interpolate to 60fps
export const TICK_MS = 1000 / TICK_HZ
export const KEYFRAME_TICKS = 30 // periodic full snapshot (~3s) as a late-joiner / robustness net

export type Stats = {
  kind: Kind
  name: string
  hp: number
  radius: number // collision + render radius (world units)
  speed: number // world units / second (0 for buildings/resources)
  sight: number // vision radius (world units)
  damage: number
  range: number // attack range (0 = cannot attack)
  cooldown: number // seconds between attacks
  cost: number // crystal
  buildTime: number // seconds to construct (buildings) / produce (units)
  supply: number // supply consumed (units) — buildings are 0
  supplyProvided: number // supply cap added (buildings)
  isBuilding: boolean
  dropoff: boolean // workers return crystal here
  produces: Kind[] // what this building can produce
  builds: Kind[] // what this (worker) unit can construct
  carry: number // worker crystal per trip
}

const B = (s: Partial<Stats> & Pick<Stats, 'kind' | 'name' | 'hp'>): Stats => ({
  radius: 46,
  speed: 0,
  sight: 460,
  damage: 0,
  range: 0,
  cooldown: 0,
  cost: 0,
  buildTime: 0,
  supply: 0,
  supplyProvided: 0,
  isBuilding: true,
  dropoff: false,
  produces: [],
  builds: [],
  carry: 0,
  ...s,
})

const U = (s: Partial<Stats> & Pick<Stats, 'kind' | 'name' | 'hp'>): Stats => ({
  radius: 13,
  speed: 90,
  sight: 360,
  damage: 0,
  range: 0,
  cooldown: 1,
  cost: 0,
  buildTime: 0,
  supply: 1,
  supplyProvided: 0,
  isBuilding: false,
  dropoff: false,
  produces: [],
  builds: [],
  carry: 0,
  ...s,
})

export const STATS: Record<Kind, Stats> = {
  [Kind.HQ]: B({
    kind: Kind.HQ,
    name: 'HQ',
    hp: 2200,
    radius: 58,
    sight: 560,
    buildTime: 0,
    supplyProvided: 15,
    dropoff: true,
    produces: [Kind.WORKER],
  }),
  [Kind.BARRACKS]: B({
    kind: Kind.BARRACKS,
    name: 'Barracks',
    hp: 900,
    radius: 48,
    sight: 420,
    cost: 150,
    buildTime: 18,
    produces: [Kind.SOLDIER],
  }),
  [Kind.FACTORY]: B({
    kind: Kind.FACTORY,
    name: 'Factory',
    hp: 1200,
    radius: 52,
    sight: 420,
    cost: 250,
    buildTime: 26,
    produces: [Kind.TANK],
  }),
  [Kind.DEPOT]: B({
    kind: Kind.DEPOT,
    name: 'Depot',
    hp: 500,
    radius: 40,
    sight: 360,
    cost: 75,
    buildTime: 10,
    supplyProvided: 10,
  }),
  [Kind.WORKER]: U({
    kind: Kind.WORKER,
    name: 'Worker',
    hp: 60,
    speed: 95,
    sight: 340,
    damage: 5,
    range: 22,
    cooldown: 0.7,
    cost: 50,
    buildTime: 7,
    supply: 1,
    builds: [Kind.BARRACKS, Kind.FACTORY, Kind.DEPOT],
    carry: 8,
  }),
  [Kind.SOLDIER]: U({
    kind: Kind.SOLDIER,
    name: 'Soldier',
    hp: 100,
    speed: 78,
    sight: 380,
    damage: 12,
    range: 165,
    cooldown: 0.8,
    cost: 75,
    buildTime: 11,
    supply: 2,
  }),
  [Kind.TANK]: U({
    kind: Kind.TANK,
    name: 'Tank',
    hp: 340,
    radius: 20,
    speed: 56,
    sight: 420,
    damage: 40,
    range: 205,
    cooldown: 1.5,
    cost: 200,
    buildTime: 22,
    supply: 4,
  }),
  [Kind.CRYSTAL]: B({
    kind: Kind.CRYSTAL,
    name: 'Crystal',
    hp: 1,
    radius: 34,
    sight: 0,
    isBuilding: false,
  }),
}

export const START_CRYSTAL = 200
export const CRYSTAL_AMOUNT = 4000 // per node
export const HARVEST_TIME = 1.1 // seconds a worker spends mining before it carries a load

// Team display colors (renderer + HUD). Player/lobby colors come from server/identity.ts.
export const TEAM_COLOR: Record<Team, number> = {
  [NEUTRAL]: 0x8b93a7,
  [RED]: 0xff4d5e,
  [BLUE]: 0x4d9dff,
}
export const TEAM_NAME: Record<Team, string> = { [NEUTRAL]: 'Neutral', [RED]: 'Red', [BLUE]: 'Blue' }

export function isBuildingKind(kind: number): boolean {
  return kind === Kind.HQ || kind === Kind.BARRACKS || kind === Kind.FACTORY || kind === Kind.DEPOT
}
