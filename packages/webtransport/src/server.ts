// Starts this process's WebTransport listener and offers it to every page in the RECONCILED of its channels' wire.
import { getGlobalObject, offerChannelTransport } from 'telefunc/__internal'
import { listen } from './server/listeners/quico.js'
import { CERTIFICATE_RENEW_MS, createCertificate } from './server/certificate.js'
import { WtWire } from './server/WtServerTransport.js'
import type { Listener } from './server/listener.js'

type Offer = { port: number; certificateHash: string }

// One listener per process, however many copies of this package it loaded.
const globalObject = getGlobalObject('@telefunc/webtransport/server.ts', {
  started: false,
  offer: undefined as Offer | undefined,
})

if (!globalObject.started) {
  globalObject.started = true
  offerChannelTransport('webtransport', () => globalObject.offer)
  void start()
}

/** On the UDP port of the same number as the app's HTTP server where `PORT` names it, else on any free one. */
async function start(): Promise<void> {
  const certificate = createCertificate()
  const onSession = (session: ConstructorParameters<typeof WtWire>[0]) => new WtWire(session)
  const port = Number(process.env.PORT) || 0
  let listener: Listener
  try {
    listener = await listen({ port, certificate, onSession }).catch(() => listen({ port: 0, certificate, onSession }))
  } catch {
    // UDP isn't open to this process: pages keep their WebSocket or SSE wire.
    return
  }
  globalObject.offer = { port: listener.port, certificateHash: certificate.hash }
  setInterval(() => renew(listener), CERTIFICATE_RENEW_MS).unref()
}

function renew(listener: Listener): void {
  const certificate = createCertificate()
  listener.setCertificate(certificate)
  globalObject.offer = { port: listener.port, certificateHash: certificate.hash }
}
