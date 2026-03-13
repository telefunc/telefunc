import React from 'react'
import './Header.css'

export { Header }

function Header() {
  return (
    <div id="header">
      <CenterHorizontal>
        <Replaces />
      </CenterHorizontal>

      <h1 style={{ margin: 0, marginTop: 0 }}>
        <CenterHorizontal>
          <div
            id="tagline-primary"
            style={{
              margin: 0,
            }}
          >
            Remote Functions.
          </div>
          <div
            id="tagline-secondary"
            style={{
              textAlign: 'center',
              width: '100%',
              lineHeight: 1.35,
              fontWeight: 450,
              color: '#878787',
              maxWidth: 800,
            }}
          >
            <span style={{ whiteSpace: 'nowrap' }}>Defined on the server,</span>{' '}
            <span style={{ whiteSpace: 'nowrap' }}>callable in the browser.</span>
          </div>
        </CenterHorizontal>
      </h1>
    </div>
  )
}

function CenterHorizontal({ children }: { children: React.ReactNode }) {
  return (
    <div
      style={{
        display: 'flex',
        alignItems: 'center',
        flexDirection: 'column',
      }}
    >
      {children}
    </div>
  )
}

function Replaces() {
  const slashOpacity = 0.65
  const Slash = () => <span style={{ opacity: slashOpacity }}> / </span>
  return (
    <div
      id="hero-badge"
      style={{
        display: 'inline-flex',
        fontSize: 11,
        fontWeight: 600,
        marginTop: -6,
        marginBottom: 6,
        borderRadius: 6,
        overflow: 'hidden',
        boxShadow: '0 1px 3px rgba(0, 0, 0, 0.12)',
        border: '1px solid #d1d5db',
      }}
    >
      <div
        style={{
          backgroundColor: '#eee',
          paddingRight: 8,
          paddingLeft: 10,
          display: 'flex',
          alignItems: 'center',
          letterSpacing: '0.05em',
          lineHeight: 1.3,
        }}
      >
        REPLACES
      </div>
      <div
        style={{
          backgroundColor: '#8d8d8d',
          color: 'white',
          padding: '4px 0',
          paddingLeft: 8,
          paddingRight: 11,
          paddingTop: 6,
          fontSize: '1.19em',
          lineHeight: 1.2,
          fontWeight: 550,
        }}
      >
        REST
        <Slash />
        GraphQL
        <Slash />
        tRPC
        <Slash />
        Server Actions
        {/*
    <Slash/>
        <span style={{ opacity: slashOpacity }}>...</span>
        */}
      </div>
    </div>
  )
}
