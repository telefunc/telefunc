import { AsyncLocalStorage } from 'node:async_hooks'
import { installAsyncContext } from './context/async.js'

installAsyncContext(AsyncLocalStorage)
