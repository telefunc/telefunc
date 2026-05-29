import React from 'react'
import { NoteWithCustomIcon } from '@brillout/docpress'
import DefineTelefunctionSnippet from './SnippetA.mdx'
import ServerSetupSnippet from './SnippetB.mdx'
import SimpleQuerySnippet from './SnippetC.mdx'
import './QuickStart.css'
import { CTALink } from '../../../components/CTALink'

export { Quickstart }

function Quickstart() {
  return (
    <div
      style={{
        padding: '0px 20px',
        display: 'flex',
        flexDirection: 'column',
        maxWidth: 1000,
        margin: '0 auto',
        marginTop: 80,
        lineHeight: 1.75,
      }}
    >
      <div className="setup-step" style={{ display: 'flex', padding: 8, columnGap: 48 }}>
        <div className="setup-info">
          <div style={{ display: 'flex', alignItems: 'center', marginBottom: 4 }}>
            <Digit value={1} />
            <h2 style={{ margin: 0 }}>They're just functions!</h2>
          </div>
          <p style={{ lineHeight: 2 }}>
            Telefunctions are slim server functions that are scoped to UI events or interactions.
          </p>
          <p style={{ lineHeight: 2 }}>
            Telefunc automatically generates a runtime <b>shield</b> from your argument types, so we don't need to worry
            about validation. In this example, we just need to check user permissions, then run our SQL.
          </p>
        </div>
        <div style={{ flexGrow: 1 }}>
          <DefineTelefunctionSnippet />
        </div>
      </div>
      <div className="setup-step" style={{ display: 'flex', padding: 8, columnGap: 48 }}>
        <div className="setup-info">
          <div style={{ display: 'flex', alignItems: 'center', marginBottom: 4 }}>
            <Digit value={2} />
            <h2 style={{ margin: 0 }}>Built on Web Standards</h2>
          </div>
          <p style={{ lineHeight: 2 }}>
            Telefunc works out-of-box with any <code>Request</code>- or Node.js-
            <code>req</code>-compatible server.
          </p>
          <NoteWithCustomIcon style={{ lineHeight: 2 }} icon={'ℹ️'}>
            Both standard JSON and <code>File</code> data are fully-supported, and&nbsp;
            <a href="https://github.com/telefunc/telefunc/pull/236" style={{ fontWeight: 'bold' }}>
              streaming is coming soon
            </a>
            !
          </NoteWithCustomIcon>
        </div>
        <div style={{ flexGrow: 1 }}>
          <ServerSetupSnippet />
        </div>
      </div>
      <div className="setup-step" style={{ display: 'flex', padding: 8, columnGap: 48 }}>
        <div className="setup-info">
          <div style={{ display: 'flex', alignItems: 'center', marginBottom: 4 }}>
            <Digit value={3} />
            <h2 style={{ margin: 0 }}>Minus the boilerplate</h2>
          </div>
          <p style={{ lineHeight: 2 }}>
            With Telefunc added to our server, we just need to import and call our telefunction!
          </p>
        </div>
        <div style={{ flexGrow: 1 }}>
          <SimpleQuerySnippet />
        </div>
      </div>
      <div>
        <h2>You may not need an API schema</h2>
        <figure>
          <blockquote>Premature optimization is the root of all evil.</blockquote>
          <figcaption style={{ display: 'flex', justifyContent: 'flex-end' }}>
            <small>
              <em>Turing Award winner, Donald Knuth</em>
            </small>
          </figcaption>
        </figure>
        <p>
          Telefunc enables you to stay lean, iterating faster and pivoting more flexibly. Many apps will never need a
          public (or schema-full) API, but if that changes, adopting REST or GraphQL is fairly straightforward.
          Telefunctions are just functions.
        </p>
        <p>
          If your goal is to enable third party developers to access your data, then you need a generic API and you'll
          have to use REST or GraphQL.
        </p>
        <p>
          But but if your goal is to seamlessly add data and interactivity to a front-end, then Telefunc can improve DX
          and enable security and performance optimizations.
        </p>
      </div>
      <div
        className="cta-stack"
        style={{
          display: 'flex',
          justifyContent: 'center',
          columnGap: 20,
          rowGap: 20,
          marginTop: 30,
        }}
      >
        <CTALink href="/start" size="lg">
          Start calling!
        </CTALink>
        <CTALink href="/why-telefunc" size="lg" variant="secondary">
          Learn more
        </CTALink>
      </div>
    </div>
  )
}

function Digit({ value }: { value: number }) {
  return (
    <div
      style={{
        width: 36,
        height: 36,
        background: 'rgb(247, 224, 24)',
        borderRadius: '50%',
        margin: 4,
        marginRight: 16,
        textAlign: 'center',
        verticalAlign: 'middle',
        lineHeight: 2.25,
        fontWeight: 'bold',
        float: 'left',
      }}
    >
      {value}
    </div>
  )
}
