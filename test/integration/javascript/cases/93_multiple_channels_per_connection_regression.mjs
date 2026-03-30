import { expect, openConnection, unique } from '../00_test_helper.mjs'

export const name = 'regression multiple channels per connection'

export async function run(suite) {
  const queue = unique('compat.multi-ch')
  const numChannels = 20
  const channels = []
  const conn = await openConnection(suite.amqpURL)

  try {
    for (let i = 0; i < numChannels; i++) {
      const ch = await conn.createChannel()
      ch.on('error', () => {})
      channels.push(ch)
    }

    for (const ch of channels) {
      await ch.assertQueue(queue, { durable: false })
      ch.sendToQueue(queue, Buffer.from('from-ch'))
    }
  } finally {
    try {
      await suite.withChannel(async (ch) => {
        await ch.assertQueue(queue, { durable: false })
        await consumeAndAckCount(ch, queue, numChannels)
      })
    } finally {
      for (const channel of channels) {
        await channel.close().catch(() => {})
      }
      await conn.close().catch(() => {})
    }
  }
}

async function consumeAndAckCount(ch, queue, expectedCount) {
  await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`timed out waiting for ${expectedCount} messages on ${queue}`)), 3000)
    let received = 0
    let consumerTag = ''

    ch.consume(queue, async (msg) => {
      if (!msg) {
        clearTimeout(timer)
        reject(new Error('consumer cancelled unexpectedly'))
        return
      }

      ch.ack(msg)
      received++
      if (received !== expectedCount) {
        return
      }

      clearTimeout(timer)
      if (consumerTag) {
        await ch.cancel(consumerTag).catch(() => {})
      }
      resolve()
    }, { noAck: false })
      .then((result) => {
        consumerTag = result.consumerTag
      })
      .catch((err) => {
        clearTimeout(timer)
        reject(err)
      })
  })

  const state = await ch.checkQueue(queue)
  expect(state.messageCount === 0, `expected queue ${queue} to be drained`)
}
