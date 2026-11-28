export { testFileUpload }

import { page, test, expect, expectLog, autoRetry, getServerUrl } from '@brillout/test-e2e'
import { navigate, getResult, serverRuntime } from '../../e2e-utils'
import { Agent, request } from 'node:http'
import { request as requestOverTls } from 'node:https'

// Calls sent as the Telefunc client sends a binary call (`x-telefunc-request: bin`, then
// `[u32 metadata length][metadata][the files' bytes]`).
const telefuncFile = '/pages/file-upload/FileUpload.telefunc.ts'
const encoder = new TextEncoder()

/** A binary call to `name` with one File of `size` bytes. */
function binaryCall(name: string, size: number): Uint8Array<ArrayBuffer> {
  const metadata = encoder.encode(
    JSON.stringify({
      file: telefuncFile,
      name,
      args: [`!TelefuncFile:${JSON.stringify({ index: 0, size, name: 'upload.txt', type: '', lastModified: 0 })}`],
    }),
  )
  const body = new Uint8Array(4 + metadata.length + size)
  new DataView(body.buffer).setUint32(0, metadata.length)
  body.set(metadata, 4)
  body.fill(120, 4 + metadata.length)
  return body
}

/** Sends `bodies` one after the other on one HTTP/1.1 keep-alive connection, without waiting for an answer in between. */
async function postOnOneConnection(bodies: { kind: 'bin' | 'txt'; body: Uint8Array }[]) {
  const agent = new Agent({ keepAlive: true, maxSockets: 1 })
  try {
    return await Promise.all(
      bodies.map(
        ({ kind, body }) =>
          new Promise<{ status: number; text: string; socket: unknown }>((resolve, reject) => {
            const req = request(`${getServerUrl()}/_telefunc`, {
              method: 'POST',
              agent,
              headers: { 'x-telefunc-request': kind, 'content-length': body.byteLength },
            })
            req.on('response', (response) => {
              let text = ''
              response.setEncoding('utf8')
              response.on('data', (chunk: string) => (text += chunk))
              response.on('end', () => resolve({ status: response.statusCode!, text, socket: req.socket }))
            })
            req.on('error', reject)
            req.end(body)
          }),
      ),
    )
  } finally {
    agent.destroy()
  }
}

/** `cookie` sends a call to the server instance that set it, behind the Docker lanes' sticky load balancer. */
async function post(kind: 'bin' | 'txt', body: BodyInit, cookie?: string) {
  const headers: Record<string, string> = { 'x-telefunc-request': kind }
  if (cookie !== undefined) headers.cookie = cookie
  const response = await fetch(`${getServerUrl()}/_telefunc`, { method: 'POST', headers, body })
  return {
    status: response.status,
    text: await response.text(),
    cookie: response.headers.get('set-cookie')?.split(';', 1)[0],
  }
}

function testFileUpload(isDev = false, inDocker = false) {
  test('file upload: single file + text arg', async () => {
    await navigate(`${getServerUrl()}/file-upload`)

    await page.click('#test-single')
    await autoRetry(async () => {
      const result = await getResult('#upload-result')
      expect(result).deep.equal({
        fileName: 'test.txt',
        fileSize: 5,
        fileType: 'text/plain',
        content: 'hello',
        description: 'desc1',
      })
    })
  })

  test('file upload: multiple files read in parallel', async () => {
    await page.click('#test-multiple')
    await autoRetry(async () => {
      const result = await getResult('#upload-result')
      expect(result).deep.equal({
        file1: { name: 'a.txt', content: 'aaa' },
        file2: { name: 'b.txt', content: 'bbb' },
      })
    })
  })

  test('file upload: File[] array', async () => {
    await page.click('#test-array')
    await autoRetry(async () => {
      const result = await getResult('#upload-result')
      expect(result).length(3)
      expect(result[0]).deep.equal({ name: 'x.txt', content: 'xxx' })
      expect(result[1]).deep.equal({ name: 'y.txt', content: 'yyy' })
      expect(result[2]).deep.equal({ name: 'z.txt', content: 'zzz' })
    })
  })

  test('file upload: file.stream()', async () => {
    await page.click('#test-stream')
    await autoRetry(async () => {
      const result = await getResult('#upload-result')
      expect(result.totalBytes).toBe(800_000)
      expect(result.chunkCount).greaterThan(1)
    })
  })

  test('file upload: file.arrayBuffer()', async () => {
    await page.click('#test-arraybuffer')
    await autoRetry(async () => {
      const result = await getResult('#upload-result')
      expect(result).deep.equal({ content: 'buffered', byteLength: 8 })
    })
  })

  test('file upload: file.slice()', async () => {
    await page.click('#test-slice')
    await autoRetry(async () => {
      const result = await getResult('#upload-result')
      expect(result).deep.equal({ content: 'hello', sliceSize: 5, originalSize: 11 })
    })
  })

  test('file upload: one-shot read (read twice throws)', async () => {
    await page.click('#test-read-twice')
    await autoRetry(async () => {
      const result = await getResult('#upload-result')
      expect(result.error).toBeTruthy()
      expect(result.error).toContain('already been consumed')
    })
  })

  test('file upload: out-of-order access drains skipped file', async () => {
    await page.click('#test-out-of-order')
    await autoRetry(async () => {
      const result = await getResult('#upload-result')
      expect(result.text2).toBe('BBB')
      expect(result.file1Error).toBeTruthy()
    })
  })

  test('file upload: cancelling a partially read first file and an unread second file leaves the third intact', async () => {
    await page.click('#test-cancel-second')
    await autoRetry(async () => {
      const result = await getResult('#upload-result')
      expect(result.firstBytes).greaterThan(0)
      expect(result.firstBytes).lessThan(16 * 1024 * 1024)
      expect(result.text3).toBe('CCC')
    })
  })

  test('file upload: after a skipped file fails to start, the next file reads its own bytes', async () => {
    await page.click('#test-after-failed-start')
    await autoRetry(async () => {
      const result = await getResult('#upload-result')
      expect(result.text2).toBe('BBB')
      expect(result.file1Error).toContain('has already been consumed')
      expect(result.text3).toBe('CCC')
    })
  })

  // 100 MB upload via a 1 MB repeated chunk (client allocates only ~1 MB).
  // Server sleeps 3 s before reading — if backpressure works, OS/server buffers
  // should not absorb the full 100 MB during the sleep, keeping RSS growth low.
  // maxChunkSize verifies the body was never assembled into a single large buffer.
  test('file upload: 100 MB streams without buffering (max chunk < 128 KB)', async () => {
    await page.click('#test-backpressure')
    await autoRetry(
      async () => {
        const result = await getResult('#upload-result')
        expect(result.done).toBe(true)
        expect(result.totalMB).toBe(100)
        expect(result.chunkCount).greaterThan(0)
        // No single chunk should be near the full file size.
        // Observed empirically: 64 KB (one TCP frame). 128 KB gives 2× headroom.
        // Not on Bun: Bun.serve hands request body chunks of up to 256 KB.
        if (serverRuntime() !== 'bun') expect(result.maxChunkKB).lessThan(128)
        // RSS growth during the 3 s sleep must be well below the 100 MB file size.
        // If the client blasted everything into server buffers, RSS would spike here.
        expect(result.rssGrowthMB).lessThan(5)
      },
      { timeout: 60_000 },
    )
  })

  // --- Stress tests ---

  test('file upload: empty file (0 bytes)', async () => {
    await page.click('#test-empty')
    await autoRetry(async () => {
      const result = await getResult('#upload-result')
      expect(result).deep.equal({ name: 'empty.txt', size: 0, content: '', isEmpty: true })
    })
  })

  test('file upload: 20 files in array', async () => {
    await page.click('#test-many-files')
    await autoRetry(async () => {
      const result = await getResult('#upload-result')
      expect(result.count).toBe(20)
      for (let i = 0; i < 20; i++) {
        expect(result.results[i]).deep.equal({ name: `file${i}.txt`, content: `content${i}` })
      }
    })
  })

  test('file upload: binary content round-trip', async () => {
    await page.click('#test-binary')
    await autoRetry(async () => {
      const result = await getResult('#upload-result')
      expect(result.byteLength).toBe(1024)
      expect(result.checksum).toBe(130560)
    })
  })

  test('file upload: mixed args with deeply nested file', async () => {
    await page.click('#test-mixed')
    await autoRetry(async () => {
      const result = await getResult('#upload-result')
      expect(result).deep.equal({
        text1: 'hello',
        label: 'my-label',
        count: 42,
        text2: 'world',
        tags: ['a', 'b'],
        flag: true,
        name1: 'm1.txt',
        name2: 'm2.txt',
      })
    })
  })

  test('file upload: 50MB file', async () => {
    await page.click('#test-large')
    await autoRetry(
      async () => {
        const result = await getResult('#upload-result')
        expect(result.totalBytes).toBe(50 * 1024 * 1024)
        expect(result.chunkCount).greaterThan(100)
        expect(result.name).toBe('large.bin')
        expect(result.size).toBe(50 * 1024 * 1024)
      },
      { timeout: 30_000 },
    )
  })

  test('file upload: slice middle of file', async () => {
    await page.click('#test-slice-middle')
    await autoRetry(async () => {
      const result = await getResult('#upload-result')
      expect(result).deep.equal({ content: 'defg', sliceSize: 4, originalSize: 10 })
    })
  })

  test('file upload: slice with negative index', async () => {
    await page.click('#test-slice-negative')
    await autoRetry(async () => {
      const result = await getResult('#upload-result')
      expect(result).deep.equal({ content: 'ghij', sliceSize: 4, originalSize: 10 })
    })
  })

  test('file upload: empty slice (0,0)', async () => {
    await page.click('#test-slice-empty')
    await autoRetry(async () => {
      const result = await getResult('#upload-result')
      expect(result).deep.equal({ content: '', sliceSize: 0, originalSize: 10 })
    })
  })

  test('file upload: File properties round-trip', async () => {
    await page.click('#test-props')
    await autoRetry(async () => {
      const result = await getResult('#upload-result')
      expect(result).deep.equal({
        name: 'props.txt',
        size: 5,
        type: 'text/plain',
        lastModified: 1700000000000,
      })
    })
  })

  test('file upload: 5 concurrent uploads', async () => {
    await page.click('#test-concurrent')
    await autoRetry(async () => {
      const result = await getResult('#upload-result')
      expect(result).length(5)
      for (let i = 0; i < 5; i++) {
        expect(result[i]).deep.equal({ id: i, content: `data${i}`, name: `c${i}.txt` })
      }
    })
  })

  test('file upload: files sent but never read on server', async () => {
    await page.click('#test-ignored')
    await autoRetry(async () => {
      const result = await getResult('#upload-result')
      expect(result).deep.equal({ ok: true })
    })
  })

  test('file upload: reading a discarded file after its call ends reports server truncation', async () => {
    const ignored = await post('bin', binaryCall('onLeaveFileUnread', 1000))
    expect(ignored.status).toBe(200)
    expect(ignored.text).toContain('ignored')
    const read = await post(
      'txt',
      JSON.stringify({ file: telefuncFile, name: 'onReadDiscardedFile', args: [] }),
      ignored.cookie,
    )
    expect(read.text).toContain('The server answered before reading the request body to its end')
  })

  test('file upload: a binary call whose body ends before its envelope is complete is refused as malformed (400)', async () => {
    const statuses: number[] = []
    for (const body of ['', new Uint8Array([0, 0]), new Uint8Array([0, 0, 3, 232, 123])]) {
      statuses.push((await post('bin', body)).status)
    }
    expect(statuses).deep.equal([400, 400, 400])
    if (isDev) {
      await autoRetry(() => {
        expectLog('Malformed request in development', { filter: (log) => log.logSource === 'stderr' })
      })
    }
  })

  // On Node's HTTP/1.1 server, which keeps the connection for the next call.
  if (process.env.NO_HTTPS && serverRuntime() === 'node')
    test('file upload: on an HTTP/1.1 keep-alive connection, a call that leaves its 4 MiB File unread does not hold up the next call', async () => {
      const [ignored, greeted] = await postOnOneConnection([
        { kind: 'bin', body: binaryCall('onIgnoreFile', 4 * 1024 * 1024) },
        { kind: 'txt', body: encoder.encode(JSON.stringify({ file: telefuncFile, name: 'onGreet', args: [] })) },
      ])
      expect(ignored!.text).toContain('ignored')
      expect(greeted!.text).toContain('hello')
      expect(greeted!.socket).toBe(ignored!.socket)
    })

  // Caddy in Docker holds a half-open 4-byte upload stream until its read timeout; direct server lanes cover this.
  if (!inDocker)
    test('file upload: a binary call whose metadata length says 2 GiB is answered 400 before any metadata is sent', async () => {
      const started = Date.now()
      const status = await new Promise<number>((resolve, reject) => {
        const url = `${getServerUrl()}/_telefunc`
        const options = { method: 'POST', headers: { 'x-telefunc-request': 'bin' } }
        const req = url.startsWith('https:')
          ? requestOverTls(url, { ...options, rejectUnauthorized: false })
          : request(url, options)
        req.on('response', (response) => {
          resolve(response.statusCode!)
          req.destroy()
        })
        req.on('error', reject)
        const header = new Uint8Array(4)
        new DataView(header.buffer).setUint32(0, 2 ** 31)
        // The body stays open: no metadata follows the length.
        req.write(header)
      })
      expect(status).toBe(400)
      expect(Date.now() - started).lessThan(1000)
      if (isDev) {
        await autoRetry(() => {
          expectLog('Malformed request in development', { filter: (log) => log.logSource === 'stderr' })
        })
      }
    })

  test("file upload: a telefunction that transfers one File's chunks away reads the next File's bytes intact", async () => {
    await navigate(`${getServerUrl()}/file-upload`)
    await page.click('#test-transfer-first-file')
    await autoRetry(async () => {
      expect(await getResult('#upload-result')).deep.equal({ firstBytes: 1024 * 1024, secondSum: 2 * 1024 * 1024 })
    })
  })
}
