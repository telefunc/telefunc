# `@telefunc/rxjs`

RxJS support for Telefunc — pass `Observable` and `Subject` instances directly between client and server, in both directions, with every RxJS operator working across the boundary.

## Install

```sh
npm install @telefunc/rxjs rxjs
```

That's it — the Telefunc bundler plugin (Vite, webpack, Next.js, Babel) detects `@telefunc/rxjs` in your dependencies and registers it automatically. Without a Telefunc bundler plugin, register it manually: `import '@telefunc/rxjs/server'` in your server entry and `import '@telefunc/rxjs/client'` in your client entry.

## Usage

Return an Observable from a telefunction and subscribe to it on the client:

```ts
// StockPrice.telefunc.ts
// Environment: server

import { interval, map } from 'rxjs'

export async function onStockPrice(symbol: string) {
  return interval(1000).pipe(
    map(() => ({ symbol, price: getLatestPrice(symbol) }))
  )
}
```

```ts
// StockPrice.ts
// Environment: client

import { filter, take } from 'rxjs'

const price$ = await onStockPrice('AAPL')
price$.pipe(
  filter(p => p.price > 150),
  take(10)
).subscribe(p => updateChart(p))
```

Observables also work as telefunction arguments (client → server), and a shared `Subject` multicasts among all connected clients.

See [telefunc.com/rxjs](https://telefunc.com/rxjs) for the full documentation.
