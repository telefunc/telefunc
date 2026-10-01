declare namespace Cloudflare {
  interface Env {}
}

type DurableObjectLocationHint = string
type DurableObjectJurisdiction = string
interface DurableObjectState {}
interface ExecutionContext {}

declare module 'cloudflare:workers' {
  export class DurableObject {
    constructor(ctx: DurableObjectState, env: Cloudflare.Env)
  }
}

declare module 'bun' {
  export interface Server<T> {}
  export interface ServerWebSocket<T> {}
  export interface WebSocketHandler<T> {}
}
