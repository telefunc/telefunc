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
        gridGap: 20,
        paddingLeft: 'var(--main-view-padding)',
        paddingRight: 'var(--main-view-padding)',
        margin: 'auto',
        maxWidth: 1000,
      }}
    >
      <div>
        <h2>
          <Emoji name="typescript" /> Type-Safe by definition
        </h2>
        <>
          <p>
            Telefunc <b>automatically generates runtime shields</b> from argument types. Inference and autocompletion
            are default.
          </p>
        </>
      </div>
      <div>
        <h2>
          <Emoji name="gem-stone" /> Schemaless by design
        </h2>
        <>
          <p>
            <b>The types are the contract.</b> Just import and call telefunctions like any other function. Telefunc does
            the rest.
          </p>
        </>
      </div>
      <div>
        <h2>
          <SaucerEmoji /> Build without constraints
        </h2>
        <>
          <p>
            Iterate flexibly and rapidly. <b>Add telefunctions as you go,</b> instead of getting bogged down with a
            back-end API schema.
          </p>
        </>
      </div>
      <div>
        <h2>
          <Emoji name="high-voltage" /> Minimal footprint
        </h2>
        <>
          <p>
            Telefunc isn't just small, it lets you write small. <b>Send only the data you need</b> for optimal
            performance and security.
          </p>
        </>
      </div>
      <div>
        <h2>
          <Emoji name="dizzy" /> Remote made simple
        </h2>
        <>
          <p>
            <b>Couple code, not environments.</b> Telefunctions let you call server-side tools like databases or
            third-party clients without worrying about the boilerplate.
          </p>
        </>
      </div>
      <div>
        <h2>
          <Emoji name="plug" /> Plug in to any server
        </h2>
        <>
          <p>
            It works out-of-box with <b>Next.js, Nuxt, SvelteKit, Vike, and React Native</b>, as well as bundlers like
            Vite, Webpack, Babel, or Parcel.
          </p>
        </>
      </div>
    </div>
  )
}

function SaucerEmoji() {
  const emoji = () => {
    try {
      return String.fromCodePoint(0x1f6f8)
    } catch {
      // fall-back to rocket
      return String.fromCodePoint(0x1f680)
    }
  }

  return (
    <div
      style={{
        width: '1.15em',
        display: 'inline',
        verticalAlign: 'text-top',
        fontFamily: 'emoji',
        fontSize: '1em',
      }}
    >
      {emoji()}
    </div>
  )
}
