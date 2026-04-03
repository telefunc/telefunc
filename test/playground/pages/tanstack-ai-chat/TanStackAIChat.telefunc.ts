export { onChat }

import { convertMessagesToModelMessages } from '@tanstack/ai'
import type { StreamChunk, UIMessage } from '@tanstack/ai'
import { getContext } from 'telefunc'

async function* onChat(messages: UIMessage[]): AsyncGenerator<StreamChunk> {
  const { signal } = getContext()
  const modelMessages = convertMessagesToModelMessages(messages)
  const prompt = modelMessages[modelMessages.length - 1]!.content as string
  yield* callProvider(prompt, signal)
}

async function* callProvider(prompt: string, signal: AbortSignal): AsyncGenerator<StreamChunk> {
  const runId = crypto.randomUUID()
  const messageId = crypto.randomUUID()
  const now = Date.now()

  yield { type: 'RUN_STARTED', runId, timestamp: now }
  yield { type: 'TEXT_MESSAGE_START', messageId, role: 'assistant', timestamp: now }

  // In a real app: yield* chat({ adapter: anthropicText({ model: '...' }), messages, signal })
  const words = `You asked: "${prompt}". Here is a streamed response, word by word.`.split(' ')
  let content = ''
  for (const word of words) {
    await new Promise((r) => setTimeout(r, 80))
    const delta = (content ? ' ' : '') + word
    content += delta
    yield { type: 'TEXT_MESSAGE_CONTENT', messageId, delta, timestamp: Date.now() }
  }

  yield { type: 'TEXT_MESSAGE_END', messageId, timestamp: Date.now() }
  yield { type: 'RUN_FINISHED', runId, finishReason: 'stop', timestamp: Date.now() }
}
