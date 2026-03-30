import { expect, openConnection, unique } from '../00_test_helper.mjs'

export const name = 'regression multiple concurrent connections'

export async function run(suite) {
  const queue = unique('compat.multi-conn')
  const numConnections = 10
  const connections = []
  const channels = []

  for (let i = 0; i < numConnections; i++) {
    connections.push(await openConnection(suite.amqpURL))
  }

  for (const conn of connections) {
    const ch = await conn.createChannel()
    ch.on('error', () => {})
    channels.push(ch)
    await ch.assertQueue(queue, { durable: false })
    ch.sendToQueue(queue, Buffer.from('from-conn'))
  }

  await suite.withChannel(async (ch) => {
    await ch.assertQueue(queue, { durable: false })
    await consumeAndAckCount(ch, queue, numConnections)
  })

  for (const channel of channels) {
    await channel.close().catch(() => {})
  }
  for (const conn of connections) {
    await conn.close().catch(() => {})
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
