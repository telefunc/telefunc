export { onGetHistory, onSendMessage, onClearHistory }
export type { Message }

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

type Message = {
  prompt: string
  response: string
}

let history: Message[] = []

async function onGetHistory() {
  return history
}

async function onClearHistory() {
  history = []
}

async function* onSendMessage(prompt: string) {
  const words = `You asked: "${prompt}". Here is a streamed response, word by word.`.split(' ')
  const collected: string[] = []
  for (const word of words) {
    await sleep(120)
    collected.push(word)
    yield word
  }
  history.push({ prompt, response: collected.join(' ') })
}
