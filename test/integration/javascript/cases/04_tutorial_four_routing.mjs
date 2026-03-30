import { expect, unique } from '../00_test_helper.mjs'

export const name = 'tutorial four routing'

export async function run(suite) {
  await suite.withConnection(async (conn) => {
    const publisher = await conn.createChannel()
    const infoConsumer = await conn.createChannel()
    const errorConsumer = await conn.createChannel()

    try {
      const exchange = unique('tutorial.four.direct')
      await publisher.assertExchange(exchange, 'direct', { durable: false })

      const infoQueue = await infoConsumer.assertQueue('', { exclusive: true })
      const errorQueue = await errorConsumer.assertQueue('', { exclusive: true })

      await infoConsumer.bindQueue(infoQueue.queue, exchange, 'info')
      await errorConsumer.bindQueue(errorQueue.queue, exchange, 'error')

      const infoMessage = consumeOne(infoConsumer, infoQueue.queue, 'info consumer')

      publisher.publish(exchange, 'info', Buffer.from('info message'))
      expect(await infoMessage === 'info message', 'info consumer received unexpected body')
      const errorState = await errorConsumer.checkQueue(errorQueue.queue)
      expect(errorState.messageCount === 0, 'error consumer should not receive info severity')

      const nextError = consumeOne(errorConsumer, errorQueue.queue, 'error consumer second delivery')
      publisher.publish(exchange, 'error', Buffer.from('error message'))
      expect(await nextError === 'error message', 'error consumer received unexpected body')
      const infoState = await infoConsumer.checkQueue(infoQueue.queue)
      expect(infoState.messageCount === 0, 'info consumer should not receive error severity')
    } finally {
      await errorConsumer.close().catch(() => {})
      await infoConsumer.close().catch(() => {})
      await publisher.close().catch(() => {})
    }
  })
}

async function consumeOne(ch, queue, label) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`timed out waiting for ${label}`)), 3000)
    let consumerTag = ''

    ch.consume(queue, async (msg) => {
      if (!msg) {
        clearTimeout(timer)
        reject(new Error(`consumer cancelled unexpectedly for ${label}`))
        return
      }
      clearTimeout(timer)
      if (consumerTag) {
        await ch.cancel(consumerTag).catch(() => {})
      }
      resolve(msg.content.toString('utf8'))
    }, { noAck: true })
      .then((result) => {
        consumerTag = result.consumerTag
      })
      .catch((err) => {
        clearTimeout(timer)
        reject(err)
      })
  })
}
