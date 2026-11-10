export { createCertificate, CERTIFICATE_RENEW_MS }
export type { Certificate }

import { X509Certificate, createHash, generateKeyPairSync, randomBytes, sign } from 'node:crypto'
import type { TlsCertificate } from './listener.js'

/** Chrome takes a certificate by its hash (`serverCertificateHashes`) only while it is valid for at most 14 days. */
const CERTIFICATE_LIFETIME_MS = 14 * 24 * 60 * 60 * 1000
/** Renewed halfway, so a page given its hash finds it served for days after. */
const CERTIFICATE_RENEW_MS = CERTIFICATE_LIFETIME_MS / 2

type Certificate = TlsCertificate & {
  /** The SHA-256 of its DER bytes, base64. */
  hash: string
}

/** A self-signed ECDSA P-256 certificate, valid from now for `CERTIFICATE_LIFETIME_MS`. */
function createCertificate(): Certificate {
  const { privateKey, publicKey } = generateKeyPairSync('ec', { namedCurve: 'P-256' })
  const now = Date.now()
  const name = sequence(set(sequence(oid('2.5.4.3'), der(0x0c, Buffer.from('telefunc')))))
  const ecdsaWithSha256 = sequence(oid('1.2.840.10045.4.3.2'))
  const tbs = sequence(
    der(0xa0, integer(Buffer.from([2]))),
    integer(randomBytes(16)),
    ecdsaWithSha256,
    name,
    sequence(time(new Date(now)), time(new Date(now + CERTIFICATE_LIFETIME_MS))),
    name,
    publicKey.export({ type: 'spki', format: 'der' }),
    // Chrome's QUIC stack parses a certificate only with its extensions: one basicConstraints, not a CA.
    der(0xa3, sequence(sequence(oid('2.5.29.19'), der(0x04, sequence())))),
  )
  const signature = sign('sha256', tbs, privateKey)
  const bytes = sequence(tbs, ecdsaWithSha256, der(0x03, Buffer.concat([Buffer.from([0]), signature])))
  const cert = new X509Certificate(bytes).toString()
  return {
    key: privateKey.export({ type: 'pkcs8', format: 'pem' }).toString(),
    cert,
    hash: createHash('sha256').update(bytes).digest('base64'),
  }
}

function der(tag: number, content: Buffer): Buffer {
  const length = content.byteLength
  const head =
    length < 0x80
      ? Buffer.from([tag, length])
      : length < 0x100
        ? Buffer.from([tag, 0x81, length])
        : Buffer.from([tag, 0x82, length >> 8, length & 0xff])
  return Buffer.concat([head, content])
}

function sequence(...items: Buffer[]): Buffer {
  return der(0x30, Buffer.concat(items))
}

function set(...items: Buffer[]): Buffer {
  return der(0x31, Buffer.concat(items))
}

/** A positive INTEGER: a leading 0 where its first bit is set. */
function integer(bytes: Buffer): Buffer {
  return der(0x02, bytes[0]! & 0x80 ? Buffer.concat([Buffer.from([0]), bytes]) : bytes)
}

function oid(dotted: string): Buffer {
  const [first, second, ...rest] = dotted.split('.').map(Number)
  const body = [first! * 40 + second!]
  for (const arc of rest) {
    const groups = [arc & 0x7f]
    for (let left = arc >> 7; left > 0; left >>= 7) groups.unshift((left & 0x7f) | 0x80)
    body.push(...groups)
  }
  return der(0x06, Buffer.from(body))
}

/** UTCTime, as RFC 5280 asks for years before 2050. */
function time(date: Date): Buffer {
  const iso = date.toISOString()
  return der(
    0x17,
    Buffer.from(
      `${iso.slice(2, 4)}${iso.slice(5, 7)}${iso.slice(8, 10)}${iso.slice(11, 13)}${iso.slice(14, 16)}${iso.slice(17, 19)}Z`,
    ),
  )
}
