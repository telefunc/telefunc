import React from 'react'
import './Header.css'
import iconTelefunc from '../../icons/telefunc.svg'

export { Header }

function Header() {
  return (
    <CenterHorizontal>
      <div
        id="header-logo"
        style={{
          display: 'flex',
          alignItems: 'center',
          marginTop: 10,
        }}
      >
        <img src={iconTelefunc} />
        <h1>Telefunc</h1>
      </div>
      <div id="header-tagline">
        Remote Functions.
        <br id="header-tagline-newline" /> Instead of API.
      </div>
    </CenterHorizontal>
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
