// The workerd module for the local-workerd conformance lane. esbuild bundles this (and the production DO
// it imports) into the script Miniflare runs, so the SAME `TelefuncRoomDurableObject` that ships dark is
// what the conformance suite exercises — no reimplementation, only a controlled clock injected through
// the DO's own authority-time seam.
//
// This file is NOT compiled by tsc (the cloudflare conformance dir is excluded); it is bundled at fixture
// startup and only ever runs inside workerd.

import { __setRoomAuthorityNowHook, TelefuncRoomDurableObject } from '../../../server/adapter/cloudflare/room/do.js'

// The controlled authority clock. It is a module global (shared with the DO instances in this isolate —
// the same seam production leaves as Date.now()), set by the facade through the control fetch below, and
// read by every DO via authorityNow(). Starting value is overwritten by the fixture on startup.
let controlledClock = 0
__setRoomAuthorityNowHook(() => controlledClock)

export { TelefuncRoomDurableObject }

export default {
  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url)
    if (url.pathname === '/clock/set') {
      controlledClock = Number(url.searchParams.get('v'))
      return new Response('ok')
    }
    if (url.pathname === '/clock/get') {
      return new Response(String(controlledClock))
    }
    return new Response('telefunc-room-conformance-worker')
  },
}
