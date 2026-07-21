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
  leaseId: string
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
  readonly #installed = new Map<string, string>()
  #deliveryGate: Promise<void> | null = null
  #releaseDeliveryGate: (() => void) | null = null
  #pendingDeliveries = 0
  readonly #deliveryWaiters = new Set<() => void>()
  #probeGate: Promise<void> | null = null
  #releaseProbeGate: (() => void) | null = null
  #pendingProbes = 0
  readonly #probeWaiters = new Set<() => void>()

  async installRoute(identity: SubscriberRouteIdentity, leaseId: string): Promise<void> {
    this.#installed.set(identityKey(identity), leaseId)
  }

  async uninstallRoute(identity: SubscriberRouteIdentity, leaseId: string): Promise<void> {
    const key = identityKey(identity)
    if (this.#installed.get(key) === leaseId) this.#installed.delete(key)
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

  async waitForDelivery(): Promise<void> {
    if (this.#pendingDeliveries > 0) return
    await new Promise<void>((resolve) => this.#deliveryWaiters.add(resolve))
  }

  async holdProbes(): Promise<void> {
    if (this.#probeGate !== null) return
    this.#probeGate = new Promise<void>((resolve) => {
      this.#releaseProbeGate = resolve
    })
  }

  async releaseProbes(): Promise<void> {
    this.#releaseProbeGate?.()
    this.#releaseProbeGate = null
    this.#probeGate = null
  }

  async waitForProbe(): Promise<void> {
    if (this.#pendingProbes > 0) return
    await new Promise<void>((resolve) => this.#probeWaiters.add(resolve))
  }

  async telefuncBroadcastDeliver(request: SubscriberDeliveryRequest): Promise<void> {
    if (request.probe === true) {
      this.#pendingProbes += 1
      for (const waiter of this.#probeWaiters) waiter()
      this.#probeWaiters.clear()
      try {
        await this.#probeGate
      } finally {
        this.#pendingProbes -= 1
      }
    } else {
      this.#pendingDeliveries += 1
      for (const waiter of this.#deliveryWaiters) waiter()
      this.#deliveryWaiters.clear()
      try {
        await this.#deliveryGate
      } finally {
        this.#pendingDeliveries -= 1
      }
    }
    const key = identityKey(request)
    if (request.probe === true) {
      if (this.#installed.get(key) !== request.leaseId) throw new Error('subscriber route is not installed')
      return
    }
    // Eviction/unsubscribe between the room's acceptance snapshot and this attempt loses the frame. It is
    // deliberately not a target failure and is never retried.
    if (this.#installed.get(key) !== request.leaseId) return
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
