export { sendWithBeforeCommit, runInOrder }

import type { FollowUp, Statement } from '../liveDatabase.js'

type Rows = (string | null)[][]

/**
 * Sends `statement` with the queries of ours its commit needs (`Statement.beforeCommit`), one after the other on its
 * connection, which the caller doesn't use until this returns: `send` sends the statement, `sendOurs` one of ours. Gives
 * the statement's result; when one fails, rolls the transaction back and throws its error.
 */
async function sendWithBeforeCommit<R>(
  statement: Pick<Statement, 'beforeCommit'>,
  send: () => Promise<R>,
  sendOurs: (text: string) => Promise<Rows>,
): Promise<R> {
  if (!statement.beforeCommit) return send()
  const { queries, ownTransaction } = statement.beforeCommit
  if (!ownTransaction) {
    await rollingBackOnFailure(() => runInOrder(queries, sendOurs), sendOurs)
    return send()
  }
  await sendOurs('BEGIN')
  const result = await rollingBackOnFailure(async () => {
    const result = await send()
    await runInOrder(queries, sendOurs)
    return result
  }, sendOurs)
  await sendOurs('COMMIT')
  return result
}

/** Runs `queries` one after the other through `sendOurs`; throws the first error. */
async function runInOrder(queries: FollowUp[], sendOurs: (text: string) => Promise<Rows>): Promise<void> {
  for (const query of queries) query.done(await sendOurs(query.text))
}

async function rollingBackOnFailure<T>(run: () => Promise<T>, sendOurs: (text: string) => Promise<Rows>): Promise<T> {
  try {
    return await run()
  } catch (err) {
    // A connection that broke ended the transaction already.
    await sendOurs('ROLLBACK').catch(() => {})
    throw err
  }
}
