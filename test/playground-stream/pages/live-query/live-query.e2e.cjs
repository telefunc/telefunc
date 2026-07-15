// Self-contained, TERMINATING live-query e2e for the channel/SSE dev server.
//
// Runs the two contracted cases (T2.F3 local meta.invalidates, T2.F4 global server-owned
// cross-client invalidation) AND the on-the-wire T2.B7 check (the outgoing live-query telefunc
// requests carry no live extension bytes), then prints the results and exits 0/1.
//
// It boots `vike dev` directly (skipping the playground `pnpm dev` script's POSIX-only
// fuser/rm/-/dev/null prelude, which cmd.exe cannot parse on Windows) and tears its own server down.
//
// Prereq (once): build the workspace packages the playground imports —
//   pnpm --filter telefunc --filter @telefunc/tanstack-query --filter @telefunc/redis --filter @telefunc/rxjs run build
// Run (from repo root):
//   node test/playground-stream/pages/live-query/live-query.e2e.cjs

const { spawn } = require('node:child_process')
const path = require('node:path')
const { chromium } = require('playwright-chromium')

const PLAYGROUND = path.resolve(__dirname, '..', '..') // test/playground-stream
const APP_URL = 'http://localhost:3000/live-query'

function startServer() {
  return new Promise((resolve, reject) => {
    const env = {
      ...process.env,
      PUBLIC_ENV__STREAM_TRANSPORT: 'channel',
      PUBLIC_ENV__CHANNEL_TRANSPORTS: JSON.stringify(['sse']),
      NO_HTTPS: 'true',
      NODE_OPTIONS: '--expose-gc',
    }
    const child = spawn('npx', ['vike', 'dev'], { cwd: PLAYGROUND, env, shell: true })
    let ready = false
    const scan = (buf) => {
      if (!ready && /localhost:3000/i.test(buf.toString())) {
        ready = true
        resolve(child)
      }
    }
    child.stdout.on('data', scan)
    child.stderr.on('data', scan)
    child.on('exit', (code) => {
      if (!ready) reject(new Error('dev server exited before ready (code ' + code + ')'))
    })
    setTimeout(() => {
      if (!ready) reject(new Error('dev server did not become ready within 90s'))
    }, 90000)
  })
}

function killTree(child) {
  return new Promise((resolve) => {
    if (!child || child.exitCode !== null) return resolve()
    if (process.platform === 'win32') {
      // Absolute path: `taskkill` is not always on the spawn PATH. `/T` kills the vike child tree.
      const taskkill = process.env.SystemRoot
        ? path.join(process.env.SystemRoot, 'System32', 'taskkill.exe')
        : 'C:\\Windows\\System32\\taskkill.exe'
      const tk = spawn(taskkill, ['/PID', String(child.pid), '/F', '/T'], { stdio: 'ignore' })
      tk.on('error', () => {
        try {
          child.kill('SIGKILL')
        } catch {}
        resolve()
      })
      tk.on('exit', () => resolve())
    } else {
      try {
        child.kill('SIGKILL')
      } catch {}
      resolve()
    }
  })
}

const waitText = (page, sel, text, timeout = 20000) =>
  page.waitForFunction(
    (a) => {
      const el = document.querySelector(a.sel)
      return !!el && (el.textContent || '').includes(a.text)
    },
    { sel, text },
    { timeout },
  )
;(async () => {
  const results = []
  const server = await startServer()
  const browser = await chromium.launch()
  try {
    const page = await browser.newPage()

    // T2.B7 (on the wire): observe every outgoing live-query telefunc POST body.
    const telefuncBodies = []
    page.on('request', (req) => {
      if (req.method() === 'POST' && req.url().includes('/_telefunc')) telefuncBodies.push(req.postData() || '')
    })

    await page.goto(APP_URL, { waitUntil: 'domcontentloaded' })
    await waitText(page, '#local-todo-list', 'Local todo 1')
    await waitText(page, '#global-todo-list', 'Global todo 1')

    // T2.F3 — local key: meta.invalidates refetches on the current client.
    const localText = 'Local check ' + Date.now()
    await page.fill('#local-todo-input', localText)
    await page.click('#local-todo-add')
    await waitText(page, '#local-todo-list', localText)
    results.push('T2.F3 local meta.invalidates refetch: PASS')

    // T2.F4 — global key: server-owned invalidation reaches a second client.
    const ctx2 = await browser.newContext()
    const page2 = await ctx2.newPage()
    await page2.goto(APP_URL, { waitUntil: 'domcontentloaded' })
    await waitText(page2, '#global-todo-list', 'Global todo 1')
    const globalText = 'Global live ' + Date.now()
    await page.fill('#global-todo-input', globalText)
    await page.click('#global-todo-add')
    await waitText(page, '#global-todo-list', globalText)
    await waitText(page2, '#global-todo-list', globalText)
    await ctx2.close()
    results.push('T2.F4 global server-owned cross-client invalidation: PASS')

    // T2.B7 — the captured request bodies must carry no live extension bytes.
    const leaked = telefuncBodies.filter((b) => b.includes('@telefunc/tanstack-query') || b.includes('invalidates'))
    if (leaked.length) throw new Error('T2.B7 FAIL: live bytes on the wire → ' + leaked[0].slice(0, 200))
    results.push('T2.B7 outgoing telefunc requests carry no live bytes (' + telefuncBodies.length + ' inspected): PASS')

    console.log(results.join('\n'))
    console.log('ALL LIVE-QUERY E2E CHECKS PASSED')
  } finally {
    await browser.close()
    await killTree(server)
  }
})().catch((err) => {
  console.error('LIVE-QUERY E2E FAILED:', err && err.message ? err.message : err)
  process.exitCode = 1
  setTimeout(() => process.exit(1), 200)
})
