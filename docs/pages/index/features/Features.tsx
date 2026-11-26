export { Features }

import React from 'react'
import iconShield from './shield.svg'
import iconTypescript from './typescript.svg'
import './Features.css'

function Features() {
  return (
    <div
      id="feature-list"
      style={{
        display: 'grid',
        gridGap: 30,
        paddingLeft: 20,
        paddingRight: 20,
        margin: 'auto',
        maxWidth: 1000,
      }}
    >
      <div>
        <h2>💫 Simple</h2>
        <>
          <p>
            With Telefunc, you get a <b>simple frontend-backend relationship</b>: it's just a set of remote functions.
          </p>
          <p>
            Your frontend can <b>directly use any SQL/ORM query</b> to retrieve & mutate data.
          </p>
        </>
      </div>
      <div>
        <h2>
          <EmojiImg src={iconShield} /> Permissions
        </h2>
        <>
          <p>
            Telefunc enables <b>programmatically defined permissions</b>. It's both <b>simple</b> and <b>flexible</b>.
          </p>
          <p>Telefunc introduces new techniques to define advanced permissions and increase safety.</p>
        </>
      </div>
      <div>
        <h2>🔌 Any Stack</h2>
        <>
          <p>
            Telefunc supports <b>Next.js</b>, <b>Nuxt</b>, <b>Vite</b>, <b>Vike</b>, <b>React Native</b>,{' '}
            <b>SvelteKit</b> and other frameworks based on Webpack, Babel, Parcel or Vite.
          </p>
        </>
      </div>
      <div>
        <h2>⚡ Performance</h2>
        <>
          <p>
            The frontend can directly tap into the <b>full power of the server</b>. Use tailored SQL/ORM queries for
            highly performant data retrieval and mutations.
          </p>
        </>
      </div>
      <div>
        <h2>⭕ Real-time &amp; streaming</h2>
        <>
          <p>
            Telefunctions can <b>stream values</b> — AI tokens, progress, file uploads &amp; downloads — and power{' '}
            <b>real-time</b> features with channels &amp; broadcasts, by returning or passing them like any other value.
          </p>
        </>
      </div>
      <div>
        <h2>
          <EmojiImg src={iconTypescript} /> TypeScript
        </h2>
        <>
          <p>
            <b>Seamless TypeScript support</b> out-of-the-box including all your favorite IDE features such as
            auto-import, replace-all-occurrences, etc.
          </p>
          <p>
            <b>Automatic runtime validation</b> for end-to-end type safety.
          </p>
        </>
      </div>
      <div>
        <h2>💎 Rock-solid</h2>
        <>
          <p>
            The source code of Telefunc has <b>no known bug</b> (bugs are fixed swiftly) and every release is assailed
            against a heavy suite of <b>automated tests</b>.
          </p>
        </>
      </div>
    </div>
  )
}

// Emojis that render poorly as text
function EmojiImg({ src }: { src: string }) {
  return <img src={src} alt="" style={{ verticalAlign: 'text-top', width: '1.15em' }} />
}
