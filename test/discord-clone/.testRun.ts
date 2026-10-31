export { testRun }

// Two real users (separate browser contexts = separate sessions) drive the app end to end.
// Chromium runs with fake media devices (see test-e2e.config.mjs), so voice, camera, and
// screen share exercise the full WebCodecs → Room binary-lane pipeline for real.

import { autoRetry, expect, getServerUrl, page, run, test } from '@brillout/test-e2e'
import type { Page } from 'playwright-chromium'

const uid = Math.random().toString(36).slice(2, 7)
const OWNER = `owner-${uid}` // registers first → owns the server (admin)
const BOB = `bob-${uid}`
const CAROL = `carol-${uid}`
const CHANNEL = `ops-${uid}`

let b: Page // the second user's page (own browser context)
const extraContexts: Array<() => Promise<void>> = []

function testRun(cmd: 'pnpm dev' | 'pnpm preview') {
  run(cmd, {
    serverIsReadyMessage: (log) =>
      log.includes('Listening on') || (log.includes('Local:') && log.includes('http://localhost:3000')),
    tolerateError(log) {
      return (
        // This app keeps telefunctions in telefunc/ (an API layer) instead of collocating.
        log.logText.includes('We recommend to collocate') ||
        log.logText.includes('ERR_ALPN_NEGOTIATION_FAILED') ||
        // node:sqlite prints this once at boot (Node < 24 marks it experimental).
        log.logText.includes('SQLite is an experimental feature') ||
        // Page navigations/closures tear down live connections mid-flight.
        log.logText.includes('Channel timed out: client did not reconnect') ||
        log.logText.includes('The user aborted a request') ||
        log.logText.includes('Telefunc call cancelled') ||
        (log.logText.includes('WebSocket connection to') && log.logText.includes('failed'))
      )
    },
  })

  test('discord-clone: register the owner — session cookie, guild presence, bot greeting', async () => {
    await registerUser(page, OWNER)
    await autoRetry(async () => {
      expect(await page.textContent('#channel-name')).toBe('general')
      expect(await page.$(`[data-member="${OWNER}"]`)).not.toBe(null)
      expect(await page.$('[data-member="RoomBot"]')).not.toBe(null)
      expect(await page.textContent('#messages')).toContain(`Welcome @${OWNER}`)
    })
  })

  test('discord-clone: messages, bot commands, and the moderation guard', async () => {
    await page.fill('#composer', 'first post')
    await page.press('#composer', 'Enter')
    await autoRetry(async () => {
      expect(await page.textContent('#messages')).toContain('first post')
    })

    await page.fill('#composer', '!ping')
    await page.press('#composer', 'Enter')
    await autoRetry(async () => {
      expect(await page.textContent('#messages')).toContain('pong')
    })

    // The banned-word guard rejects through the publish ack — the error lands in a toast.
    await page.fill('#composer', 'oh heck')
    await page.press('#composer', 'Enter')
    await autoRetry(async () => {
      expect(await page.textContent('#toast')).toContain('not allowed')
    })
    expect(await page.textContent('#messages')).not.toContain('oh heck')
  })

  test('discord-clone: create a channel, set its topic', async () => {
    await page.click('#add-text-channel')
    await page.fill('#add-text-channel-name', CHANNEL)
    await page.press('#add-text-channel-name', 'Enter')
    await autoRetry(async () => {
      expect(await page.$(`[data-channel="${CHANNEL}"]`)).not.toBe(null)
    })
    await page.click(`[data-channel="${CHANNEL}"]`)
    await autoRetry(async () => {
      expect(await page.textContent('#channel-name')).toBe(CHANNEL)
    })
    await page.click('#channel-topic')
    await page.fill('#topic-input', 'war room')
    await page.press('#topic-input', 'Enter')
    await autoRetry(async () => {
      expect(await page.textContent('#channel-topic')).toContain('war room')
    })
    await page.click('[data-channel="general"]')
    await autoRetry(async () => {
      expect(await page.textContent('#channel-name')).toBe('general')
    })
  })

  test('discord-clone: a second user — live cross-user chat and typing indicators', async () => {
    b = await newContextPage()
    await registerUser(b, BOB)
    await autoRetry(async () => {
      expect(await page.$(`[data-member="${BOB}"]`)).not.toBe(null) // A sees B appear
      expect(await b.$(`[data-channel="${CHANNEL}"]`)).not.toBe(null) // B got the full channel list
    })

    await b.fill('#composer', 'typing test in progress')
    await autoRetry(async () => {
      expect(await page.textContent('#typing')).toContain(BOB) // ephemeral publish, cross-user
    })
    await b.press('#composer', 'Enter')
    await autoRetry(async () => {
      expect(await page.textContent('#messages')).toContain('typing test in progress')
      expect(await page.textContent('#typing')).not.toContain(BOB) // cleared by the message
    })
  })

  test('discord-clone: unread badges and history pagination', async () => {
    await b.click(`[data-channel="${CHANNEL}"]`)
    await autoRetry(async () => {
      expect(await b.textContent('#channel-name')).toBe(CHANNEL)
    })
    await b.fill('#composer', 'psst')
    await b.press('#composer', 'Enter')
    // The unread dot rides the guild announce lane (the channel's `onAfterPublish` hook pings it),
    // so A pays for one guild subscription, not the full text lane of channels it isn't reading.
    await autoRetry(async () => {
      expect(await page.$(`[data-unread-for="${CHANNEL}"]`)).not.toBe(null) // A, sitting in #general
    })

    // More messages than one page (DISCORD_CLONE_PAGE_SIZE=5) → "Load older" appears.
    for (let i = 0; i < 7; i++) {
      await b.fill('#composer', `history-${i}`)
      await b.press('#composer', 'Enter')
      await autoRetry(async () => {
        expect(await b.textContent('#messages')).toContain(`history-${i}`)
      })
    }
    await page.click(`[data-channel="${CHANNEL}"]`)
    await autoRetry(async () => {
      expect(await page.$('#load-older')).not.toBe(null)
    })
    await page.click('#load-older')
    await autoRetry(async () => {
      expect(await page.textContent('#messages')).toContain('psst') // the older page arrived
      expect(await page.$(`[data-unread-for="${CHANNEL}"]`)).toBe(null) // opening cleared the badge
    })
  })

  test('discord-clone: direct messages — live delivery, rail badge, conversation list', async () => {
    await page.hover(`[data-member="${BOB}"]`)
    await page.click(`[data-dm="${BOB}"]`)
    await page.fill('#dm-composer', 'hey bob')
    await page.press('#dm-composer', 'Enter')
    await autoRetry(async () => {
      expect(await page.textContent('#dm-messages')).toContain('hey bob')
      expect(await b.$('#rail-home-badge')).not.toBe(null) // B's Home rail lights up
    })
    await b.click('#rail-home')
    await b.click(`[data-dm-conversation="${OWNER}"]`)
    await autoRetry(async () => {
      expect(await b.textContent('#dm-messages')).toContain('hey bob')
    })
    await b.fill('#dm-composer', 'hi owner')
    await b.press('#dm-composer', 'Enter')
    await autoRetry(async () => {
      expect(await page.textContent('#dm-messages')).toContain('hi owner')
    })
  })

  test('discord-clone: the bot answers DMs', async () => {
    await b.click('#rail-guild')
    await b.hover('[data-member="RoomBot"]')
    await b.click('[data-dm="RoomBot"]')
    await b.fill('#dm-composer', 'hello bot')
    await b.press('#dm-composer', 'Enter')
    await autoRetry(async () => {
      expect(await b.textContent('#dm-messages')).toContain('You said: "hello bot"')
    })
  })

  test('discord-clone: Do Not Disturb blocks incoming DMs', async () => {
    await page.click('#rail-guild')
    await b.click('#rail-guild')
    await page.selectOption('#status-select', 'dnd')
    await autoRetry(async () => {
      expect(await b.$(`[data-member="${OWNER}"] [data-status="dnd"]`)).not.toBe(null) // propagated
    })
    await b.click('#rail-home')
    await b.click(`[data-dm-conversation="${OWNER}"]`)
    await b.fill('#dm-composer', 'are you there?')
    await b.press('#dm-composer', 'Enter')
    await autoRetry(async () => {
      expect(await b.textContent('#toast')).toContain('Do Not Disturb') // rejected server-side
    })
    await page.selectOption('#status-select', 'online')
  })

  test('discord-clone: DMs reach offline users (database-first delivery)', async () => {
    // Carol registers, starts a conversation with the owner, then goes offline.
    const carol = await newContextPage()
    await registerUser(carol, CAROL)
    await autoRetry(async () => {
      expect(await page.$(`[data-member="${CAROL}"]`)).not.toBe(null)
    })
    await page.hover(`[data-member="${CAROL}"]`)
    await page.click(`[data-dm="${CAROL}"]`)
    await page.fill('#dm-composer', 'ping while online')
    await page.press('#dm-composer', 'Enter')
    await autoRetry(async () => {
      expect(await page.textContent('#dm-messages')).toContain('ping while online')
    })
    await carol.context().close()
    await autoRetry(async () => {
      expect(await page.$(`[data-member="${CAROL}"]`)).toBe(null) // presence noticed the closed tab
    })

    // Message her while she's gone — persisted, no live participant anywhere.
    await page.fill('#dm-composer', 'read this later')
    await page.press('#dm-composer', 'Enter')
    await autoRetry(async () => {
      expect(await page.textContent('#dm-messages')).toContain('read this later')
    })

    // She logs back in and finds it in her conversation list.
    const carolAgain = await newContextPage()
    await loginUser(carolAgain, CAROL)
    await carolAgain.click('#rail-home')
    await carolAgain.click(`[data-dm-conversation="${OWNER}"]`)
    await autoRetry(async () => {
      expect(await carolAgain.textContent('#dm-messages')).toContain('read this later')
    })
    await carolAgain.context().close()
  })

  test('discord-clone: voice — presence, real camera frames, mute, across two users', async () => {
    await page.click('#rail-guild')
    await b.click('#rail-guild')
    await page.click('[data-join-voice="lounge"]')
    await autoRetry(async () => {
      expect(await page.$(`[data-voice-occupant="${OWNER}"]`)).not.toBe(null) // my chip
      expect(await b.$(`[data-voice-occupant="${OWNER}"]`)).not.toBe(null) // visible to observers too
    })
    await b.click('[data-join-voice="lounge"]')
    await autoRetry(async () => {
      expect(await page.$(`[data-tile="${BOB}"]`)).not.toBe(null) // B's tile in A's call view
    })

    // A turns the (fake) camera on → VP8 flows member-selectively; B decodes real frames.
    await page.click('#call-camera')
    await autoRetry(async () => {
      expect(await b.$(`[data-tile="${OWNER}"] canvas[data-live]`)).not.toBe(null)
    })

    // B mutes — metadata replace, propagated to every observer.
    await b.click('#call-mute')
    await autoRetry(async () => {
      expect(await page.$(`[data-tile="${BOB}"] [data-muted]`)).not.toBe(null)
    })
  })

  test('discord-clone: screen share crosses the wire', async () => {
    await page.click('#call-screen')
    await autoRetry(async () => {
      expect(await b.$(`[data-screen-tile="${OWNER}"] canvas[data-live]`)).not.toBe(null)
    })
    await page.click('#call-screen') // stop sharing
    await autoRetry(async () => {
      expect(await b.$(`[data-screen-tile="${OWNER}"]`)).toBe(null)
    })
  })

  test('discord-clone: announcements, channel deletion, and kicking a user', async () => {
    // Server-wide banner (room-authored announce).
    await page.fill('#announce-input', 'maintenance at noon')
    await page.press('#announce-input', 'Enter')
    await autoRetry(async () => {
      expect(await b.textContent('#banner')).toContain('maintenance at noon')
    })

    // Deleting a channel closes its room — every holder drops it, no announcement needed.
    await page.hover(`[data-channel="${CHANNEL}"]`)
    await page.click(`[data-delete-channel="${CHANNEL}"]`)
    await autoRetry(async () => {
      expect(await b.$(`[data-channel="${CHANNEL}"]`)).toBe(null)
    })

    // Kick B: notice → removal → sweep of every room (B is in voice!) → announce.
    await page.hover(`[data-member="${BOB}"]`)
    await page.click(`[data-kick="${BOB}"]`)
    await autoRetry(async () => {
      expect(await b.$('#kicked-screen')).not.toBe(null) // B knows why
      expect(await page.$(`[data-member="${BOB}"]`)).toBe(null) // gone from the guild
      expect(await page.$(`[data-voice-occupant="${BOB}"]`)).toBe(null) // swept from voice too
    })
    await closeExtraContexts()
  })
}

// --- Helpers ---

async function newContextPage(): Promise<Page> {
  const browser = page.context().browser()
  if (browser === null) throw new Error('No browser')
  const context = await browser.newContext()
  extraContexts.push(() => context.close())
  const extraPage = await context.newPage()
  await extraPage.goto(getServerUrl())
  await extraPage.waitForSelector('#hydrated')
  return extraPage
}

async function registerUser(target: Page, name: string): Promise<void> {
  if (target === page) {
    await page.goto(`${getServerUrl()}/`)
    await page.waitForSelector('#hydrated')
  }
  await target.click('#auth-tab-register')
  await target.fill('#auth-name', name)
  await target.fill('#auth-password', 'hunter2222')
  await target.click('#auth-submit')
  await target.waitForSelector('#composer')
}

async function loginUser(target: Page, name: string): Promise<void> {
  await target.click('#auth-tab-login')
  await target.fill('#auth-name', name)
  await target.fill('#auth-password', 'hunter2222')
  await target.click('#auth-submit')
  await target.waitForSelector('#composer')
}

async function closeExtraContexts(): Promise<void> {
  for (const close of extraContexts.splice(0)) await close().catch(() => {})
}
