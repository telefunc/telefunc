export { Features }

import React from 'react'
import { Emoji } from '@brillout/docpress'
import './Features.css'

function Features() {
  return (
    <div
      id="feature-list"
      style={{
        display: 'grid',
        gridGap: 30,
        paddingLeft: 'var(--main-view-padding)',
        paddingRight: 'var(--main-view-padding)',
        margin: 'auto',
        maxWidth: 1000,
      }}
    >
      <div>
        <h2>
          <Emoji name="dizzy" /> Zero boilerplate
        </h2>
        <>
          <p>
            Export a function from a <code>.telefunc.ts</code> file, import and call it from the client.{' '}
            <b>No routers, no procedure builders, no link chains.</b>
          </p>
          <p>End-to-end type safety and automatic runtime validation — no codegen needed.</p>
        </>
      </div>
      <div>
        <h2>
          <Emoji name="sparkles" /> Streaming
        </h2>
        <>
          <p>
            Return <b>async generators</b>, <b>ReadableStreams</b>, and <b>promises</b> — mix multiple streams and plain
            values in a single return. AI chat, live feeds, progressive loading.
          </p>
        </>
      </div>
      <div>
        <h2>
          <Emoji name="high-voltage" /> Real-time channels
        </h2>
        <>
          <p>
            <b>Bidirectional WebSocket channels</b> with acknowledged sends, auto-reconnect, and frame replay.
            Multiplexed over a single connection.
          </p>
        </>
      </div>
      <div>
        <h2>
          <Emoji name="gear" /> Function passing
        </h2>
        <>
          <p>
            Pass <b>callbacks across the wire</b> — the server calls your client function, the client calls a server
            function. Both directions, transparently proxied over WebSocket.
          </p>
        </>
      </div>
      <div>
        <h2>
          <Emoji name="package" /> File upload
        </h2>
        <>
          <p>
            Pass <code>File</code> and <code>Blob</code> as <b>regular arguments</b> mixed with any other types. Files
            stream lazily from the HTTP body — <b>constant memory</b>, no FormData ceremony.
          </p>
        </>
      </div>
      <div>
        <h2>
          <Emoji name="plug" /> Any stack
        </h2>
        <>
          <p>
            Works with <b>Next.js</b>, <b>Vike</b>, <b>Nuxt</b>, <b>SvelteKit</b>, <b>React Native</b>, and any
            framework built on Vite, Webpack, or Babel. Deploy to Node.js, Cloudflare Workers, or Deno.
          </p>
        </>
      </div>
    </div>
  )
}
