import type { LaneReceiver } from '../spi.js'

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
  remote?: RemoteReceiverControl
}

const descriptors = new WeakMap<LaneReceiver, ReceiverDescriptor>()
let receiverSequence = 0

export function conformanceReceiver(
  command: ReceiverCommand,
  local: LaneReceiver,
  observe: (observations: RemoteReceiverObservation[]) => void,
): LaneReceiver {
  descriptors.set(local, { id: `receiver-${++receiverSequence}`, command, observe })
  return local
}

export function receiverDescriptor(receiver: LaneReceiver): Readonly<ReceiverDescriptor> | undefined {
  return descriptors.get(receiver)
}

export function bindRemoteReceiver(receiver: LaneReceiver, control: RemoteReceiverControl): void {
  const descriptor = descriptors.get(receiver)
  if (descriptor === undefined) throw new Error('Cloudflare conformance requires an instrumented receiver command')
  descriptor.remote = control
}

export async function pollRemoteReceiver(receiver: LaneReceiver): Promise<void> {
  const descriptor = descriptors.get(receiver)
  if (descriptor?.remote === undefined) return
  descriptor.observe(await descriptor.remote.poll())
}

export async function releaseRemoteReceiver(receiver: LaneReceiver): Promise<boolean> {
  const release = descriptors.get(receiver)?.remote?.release
  if (release === undefined) return false
  await release()
  return true
}

export async function seedRemoteReceiver(receiver: LaneReceiver): Promise<boolean> {
  const seed = descriptors.get(receiver)?.remote?.seed
  if (seed === undefined) return false
  await seed()
  return true
}
