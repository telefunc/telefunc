/// <reference types="@cloudflare/workers-types" />

import { DurableObject } from 'cloudflare:workers'
import { bytesToBase64 } from './codec.js'

export type SubscriberRouteIdentity = {
  roomId: string
  inc: string
  laneKey: string
  subscriber: string
}

export type SubscriberDeliveryRequest = SubscriberRouteIdentity & {
  probe?: boolean
  frame?: Uint8Array
  seq?: number
  timestamp?: number
}

type SubscriberEnv = {
  TELEFUNC_ROOM_DELIVERY_RELAY: Fetcher
}

function identityKey(identity: SubscriberRouteIdentity): string {
  return JSON.stringify([identity.roomId, identity.inc, identity.laneKey, identity.subscriber])
}

// The representative subscriber DO. Registration probes this RPC surface, rather than consulting a
// process-local map. Its installed identities model the isolate-local dispatch table: a DO eviction drops
// them, so an accepted frame can be lost at most once without inventing a retry.
export class TelefuncRoomSubscriberDurableObject extends DurableObject {
  readonly #installed = new Set<string>()
  #deliveryGate: Promise<void> | null = null
  #releaseDeliveryGate: (() => void) | null = null

  async installRoute(identity: SubscriberRouteIdentity): Promise<void> {
    this.#installed.add(identityKey(identity))
  }

  async uninstallRoute(identity: SubscriberRouteIdentity): Promise<void> {
    this.#installed.delete(identityKey(identity))
  }

  // Dark conformance controls: they pause before the at-most-once addressability check, allowing workerd
  // to force an eviction precisely between room acceptance and subscriber attempt.
  async holdDeliveries(): Promise<void> {
    if (this.#deliveryGate !== null) return
    this.#deliveryGate = new Promise<void>((resolve) => {
      this.#releaseDeliveryGate = resolve
    })
  }

  async releaseDeliveries(): Promise<void> {
    this.#releaseDeliveryGate?.()
    this.#releaseDeliveryGate = null
    this.#deliveryGate = null
  }

  async telefuncBroadcastDeliver(request: SubscriberDeliveryRequest): Promise<void> {
    if (request.probe !== true) await this.#deliveryGate
    const key = identityKey(request)
    if (request.probe === true) {
      if (!this.#installed.has(key)) throw new Error('subscriber route is not installed')
      return
    }
    // Eviction/unsubscribe between the room's acceptance snapshot and this attempt loses the frame. It is
    // deliberately not a target failure and is never retried.
    if (!this.#installed.has(key)) return
    if (request.frame === undefined || request.seq === undefined || request.timestamp === undefined) {
      throw new Error('subscriber delivery is missing frame metadata')
    }
    const response = await (this.env as SubscriberEnv).TELEFUNC_ROOM_DELIVERY_RELAY.fetch(
      new Request('https://telefunc.invalid/deliver', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          roomId: request.roomId,
          inc: request.inc,
          laneKey: request.laneKey,
          subscriber: request.subscriber,
          payloadB64: bytesToBase64(request.frame),
          seq: request.seq,
          timestamp: request.timestamp,
        }),
      }),
    )
    if (!response.ok) throw new Error(`subscriber delivery failed: ${response.status}`)
  }
}
