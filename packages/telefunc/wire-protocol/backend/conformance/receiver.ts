import type { BackendReceiver } from '../spi.js'

export type ReceiverFrame = { payload: string; seq: number; timestamp: number }
export type ReceiverCommand =
  | { kind: 'collect' }
  | { kind: 'throw'; message: string; payload?: string }
  | { kind: 'stall' }
  | { kind: 'seeded' }
  | { kind: 'sequence'; outcomes: Array<'collect' | 'throw'>; message: string }

export type RemoteReceiverObservation = ReceiverFrame & { source?: 'seed' | 'live' }

type RemoteReceiverControl = {
  poll(): Promise<RemoteReceiverObservation[]>
  release?(): Promise<void>
  seed?(): Promise<void>
}

type ReceiverDescriptor = {
  id: string
  command: ReceiverCommand
  observe(observations: RemoteReceiverObservation[]): void
  remotes: Map<string, RemoteReceiverControl>
}

const descriptors = new WeakMap<BackendReceiver, ReceiverDescriptor>()
let receiverSequence = 0

export function conformanceReceiver(
  command: ReceiverCommand,
  local: BackendReceiver,
  observe: (observations: RemoteReceiverObservation[]) => void,
): BackendReceiver {
  descriptors.set(local, { id: `receiver-${++receiverSequence}`, command, observe, remotes: new Map() })
  return local
}

export function receiverDescriptor(receiver: BackendReceiver): Readonly<ReceiverDescriptor> | undefined {
  return descriptors.get(receiver)
}

export function bindRemoteReceiver(
  receiver: BackendReceiver,
  attachmentId: string,
  control: RemoteReceiverControl,
): void {
  const descriptor = descriptors.get(receiver)
  if (descriptor === undefined) throw new Error('Cloudflare conformance requires an instrumented receiver command')
  descriptor.remotes.set(attachmentId, control)
}

export function unbindRemoteReceiver(receiver: BackendReceiver, attachmentId: string): void {
  descriptors.get(receiver)?.remotes.delete(attachmentId)
}

export async function pollRemoteReceiver(receiver: BackendReceiver): Promise<void> {
  const descriptor = descriptors.get(receiver)
  if (descriptor === undefined) return
  for (const remote of descriptor.remotes.values()) descriptor.observe(await remote.poll())
}

export async function releaseRemoteReceiver(receiver: BackendReceiver): Promise<boolean> {
  const remotes = [...(descriptors.get(receiver)?.remotes.values() ?? [])]
  const releases = remotes.flatMap((remote) => (remote.release === undefined ? [] : [remote.release()]))
  if (releases.length === 0) return false
  await Promise.all(releases)
  return true
}

export async function seedRemoteReceiver(receiver: BackendReceiver): Promise<boolean> {
  const remotes = [...(descriptors.get(receiver)?.remotes.values() ?? [])]
  const seeds = remotes.flatMap((remote) => (remote.seed === undefined ? [] : [remote.seed()]))
  if (seeds.length === 0) return false
  await Promise.all(seeds)
  return true
}
