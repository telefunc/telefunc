/// <reference types="@cloudflare/workers-types" />
export { durableObjectState, webSocket }

/** A member of Cloudflare's runtime object the test doesn't reach: it fails the test if it is. */
function unreached(member: string): () => never {
  return () => {
    throw new Error(`The test reached ${member}, which its fake of Cloudflare's runtime doesn't give`)
  }
}

/** The state Cloudflare hands the Durable Object named `name`: its id names it, and the rest fails the test once
 *  reached. */
function durableObjectState(name: string): DurableObjectState {
  return {
    id: { name, toString: () => name, equals: (other) => other.name === name },
    props: undefined,
    storage: {
      get: unreached('storage.get()'),
      list: unreached('storage.list()'),
      put: unreached('storage.put()'),
      delete: unreached('storage.delete()'),
      deleteAll: unreached('storage.deleteAll()'),
      transaction: unreached('storage.transaction()'),
      getAlarm: unreached('storage.getAlarm()'),
      setAlarm: unreached('storage.setAlarm()'),
      deleteAlarm: unreached('storage.deleteAlarm()'),
      sync: unreached('storage.sync()'),
      sql: {
        exec: unreached('storage.sql.exec()'),
        get databaseSize() {
          return unreached('storage.sql.databaseSize')()
        },
        Cursor: SqlCursor,
        Statement: SqlStatement,
      },
      kv: {
        get: unreached('storage.kv.get()'),
        list: unreached('storage.kv.list()'),
        put: unreached('storage.kv.put()'),
        delete: unreached('storage.kv.delete()'),
      },
      transactionSync: unreached('storage.transactionSync()'),
      getCurrentBookmark: unreached('storage.getCurrentBookmark()'),
      getBookmarkForTime: unreached('storage.getBookmarkForTime()'),
      onNextSessionRestoreBookmark: unreached('storage.onNextSessionRestoreBookmark()'),
    },
    facets: {
      get: unreached('facets.get()'),
      abort: unreached('facets.abort()'),
      delete: unreached('facets.delete()'),
    },
    waitUntil: unreached('waitUntil()'),
    blockConcurrencyWhile: unreached('blockConcurrencyWhile()'),
    acceptWebSocket: unreached('acceptWebSocket()'),
    getWebSockets: unreached('getWebSockets()'),
    setWebSocketAutoResponse: unreached('setWebSocketAutoResponse()'),
    getWebSocketAutoResponse: unreached('getWebSocketAutoResponse()'),
    getWebSocketAutoResponseTimestamp: unreached('getWebSocketAutoResponseTimestamp()'),
    setHibernatableWebSocketEventTimeout: unreached('setHibernatableWebSocketEventTimeout()'),
    getHibernatableWebSocketEventTimeout: unreached('getHibernatableWebSocketEventTimeout()'),
    getTags: unreached('getTags()'),
    abort: unreached('abort()'),
  }
}

/** The runtime's cursor and statement classes, which a fake storage names and no test constructs. */
abstract class SqlCursor<T extends Record<string, SqlStorageValue>> {
  abstract next(): { done?: false; value: T } | { done: true; value?: never }
  abstract toArray(): T[]
  abstract one(): T
  abstract raw<U extends SqlStorageValue[]>(): IterableIterator<U>
  abstract columnNames: string[]
  abstract get rowsRead(): number
  abstract get rowsWritten(): number
  abstract [Symbol.iterator](): IterableIterator<T>
}
abstract class SqlStatement {}

/** A hibernatable WebSocket Cloudflare hands the Durable Object: `close` is the one the test gives, and the rest fails
 *  the test once reached. */
function webSocket(close: WebSocket['close']): WebSocket {
  return {
    close,
    accept: unreached('accept()'),
    send: unreached('send()'),
    serializeAttachment: unreached('serializeAttachment()'),
    deserializeAttachment: unreached('deserializeAttachment()'),
    addEventListener: unreached('addEventListener()'),
    removeEventListener: unreached('removeEventListener()'),
    dispatchEvent: unreached('dispatchEvent()'),
    readyState: WebSocket.OPEN,
    url: '',
    protocol: '',
    extensions: '',
    binaryType: 'arraybuffer',
    // The DOM's WebSocket, whose declaration this program's types merge with the runtime's.
    bufferedAmount: 0,
    onopen: null,
    onmessage: null,
    onerror: null,
    onclose: null,
    CONNECTING: 0,
    OPEN: 1,
    CLOSING: 2,
    CLOSED: 3,
  }
}
