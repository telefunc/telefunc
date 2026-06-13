export { testFileUpload }

import { page, test, expect, autoRetry, getServerUrl } from '@brillout/test-e2e'

function testFileUpload() {
  test('file upload: single file + text arg', async () => {
    await page.goto(`${getServerUrl()}/file-upload`)
    // Wait for React hydration before clicking any buttons
    await autoRetry(async () => {
      expect(await page.locator('#hydrated').count()).toBe(1)
    })

    await page.click('#test-single')
    await autoRetry(async () => {
      const result = JSON.parse((await page.textContent('#upload-result'))!)
      expect(result).deep.equal({
        fileName: 'test.txt',
        fileSize: 5,
        fileType: 'text/plain',
        content: 'hello',
        description: 'desc1',
      })
    })
  })

  test('file upload: multiple files', async () => {
    await page.click('#test-multiple')
    await autoRetry(async () => {
      const result = JSON.parse((await page.textContent('#upload-result'))!)
      expect(result).deep.equal({
        file1: { name: 'a.txt', content: 'aaa' },
        file2: { name: 'b.txt', content: 'bbb' },
      })
    })
  })

  test('file upload: File[] array', async () => {
    await page.click('#test-array')
    await autoRetry(async () => {
      const result = JSON.parse((await page.textContent('#upload-result'))!)
      expect(result).length(3)
      expect(result[0]).deep.equal({ name: 'x.txt', content: 'xxx' })
      expect(result[1]).deep.equal({ name: 'y.txt', content: 'yyy' })
      expect(result[2]).deep.equal({ name: 'z.txt', content: 'zzz' })
    })
  })

  test('file upload: file.stream()', async () => {
    await page.click('#test-stream')
    await autoRetry(async () => {
      const result = JSON.parse((await page.textContent('#upload-result'))!)
      expect(result.totalBytes).toBe(800_000)
      expect(result.chunkCount).greaterThan(1)
    })
  })

  test('file upload: file.arrayBuffer()', async () => {
    await page.click('#test-arraybuffer')
    await autoRetry(async () => {
      const result = JSON.parse((await page.textContent('#upload-result'))!)
      expect(result).deep.equal({ content: 'buffered', byteLength: 8 })
    })
  })

  test('file upload: file.slice()', async () => {
    await page.click('#test-slice')
    await autoRetry(async () => {
      const result = JSON.parse((await page.textContent('#upload-result'))!)
      expect(result).deep.equal({ content: 'hello', sliceSize: 5, originalSize: 11 })
    })
  })

  test('file upload: one-shot read (read twice throws)', async () => {
    await page.click('#test-read-twice')
    await autoRetry(async () => {
      const result = JSON.parse((await page.textContent('#upload-result'))!)
      expect(result.error).toBeTruthy()
      expect(result.error).toContain('already been consumed')
    })
  })

  test('file upload: out-of-order access drains skipped file', async () => {
    await page.click('#test-out-of-order')
    await autoRetry(async () => {
      const result = JSON.parse((await page.textContent('#upload-result'))!)
      expect(result.text2).toBe('BBB')
      expect(result.file1Error).toBeTruthy()
    })
  })

  test('file upload: backpressure (slow consumer)', async () => {
    await page.click('#test-backpressure')
    await autoRetry(async () => {
      const result = JSON.parse((await page.textContent('#upload-result'))!)
      expect(result.totalBytes).toBe(800_000)
      expect(result.chunkCount).greaterThan(1)
      expect(result.elapsed).greaterThanOrEqual(result.chunkCount * 30)
    })
  })

  // --- Stress tests ---

  test('file upload: empty file (0 bytes)', async () => {
    await page.click('#test-empty')
    await autoRetry(async () => {
      const result = JSON.parse((await page.textContent('#upload-result'))!)
      expect(result).deep.equal({ name: 'empty.txt', size: 0, content: '', isEmpty: true })
    })
  })

  test('file upload: 20 files in array', async () => {
    await page.click('#test-many-files')
    await autoRetry(async () => {
      const result = JSON.parse((await page.textContent('#upload-result'))!)
      expect(result.count).toBe(20)
      for (let i = 0; i < 20; i++) {
        expect(result.results[i]).deep.equal({ name: `file${i}.txt`, content: `content${i}` })
      }
    })
  })

  test('file upload: binary content round-trip', async () => {
    await page.click('#test-binary')
    await autoRetry(async () => {
      const result = JSON.parse((await page.textContent('#upload-result'))!)
      expect(result.byteLength).toBe(1024)
      // Sum of bytes 0..255 = 32640, repeated 4 times = 130560
      expect(result.checksum).toBe(130560)
    })
  })

  test('file upload: mixed args with deeply nested file', async () => {
    await page.click('#test-mixed')
    await autoRetry(async () => {
      const result = JSON.parse((await page.textContent('#upload-result'))!)
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

  test('file upload: 5MB file', async () => {
    await page.click('#test-large')
    await autoRetry(
      async () => {
        const result = JSON.parse((await page.textContent('#upload-result'))!)
        expect(result.totalBytes).toBe(5 * 1024 * 1024)
        expect(result.chunkCount).greaterThan(1)
        expect(result.name).toBe('large.bin')
        expect(result.size).toBe(5 * 1024 * 1024)
      },
      { timeout: 30_000 },
    )
  })

  test('file upload: slice middle of file', async () => {
    await page.click('#test-slice-middle')
    await autoRetry(async () => {
      const result = JSON.parse((await page.textContent('#upload-result'))!)
      expect(result).deep.equal({ content: 'defg', sliceSize: 4, originalSize: 10 })
    })
  })

  test('file upload: slice with negative index', async () => {
    await page.click('#test-slice-negative')
    await autoRetry(async () => {
      const result = JSON.parse((await page.textContent('#upload-result'))!)
      expect(result).deep.equal({ content: 'ghij', sliceSize: 4, originalSize: 10 })
    })
  })

  test('file upload: empty slice (0,0)', async () => {
    await page.click('#test-slice-empty')
    await autoRetry(async () => {
      const result = JSON.parse((await page.textContent('#upload-result'))!)
      expect(result).deep.equal({ content: '', sliceSize: 0, originalSize: 10 })
    })
  })

  test('file upload: File properties round-trip', async () => {
    await page.click('#test-props')
    await autoRetry(async () => {
      const result = JSON.parse((await page.textContent('#upload-result'))!)
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
      const result = JSON.parse((await page.textContent('#upload-result'))!)
      expect(result).length(5)
      for (let i = 0; i < 5; i++) {
        expect(result[i]).deep.equal({ id: i, content: `data${i}`, name: `c${i}.txt` })
      }
    })
  })

  test('file upload: files sent but never read on server', async () => {
    await page.click('#test-ignored')
    await autoRetry(async () => {
      const result = JSON.parse((await page.textContent('#upload-result'))!)
      expect(result).deep.equal({ ok: true })
    })
  })
}
