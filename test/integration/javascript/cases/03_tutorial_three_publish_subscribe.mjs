import { expect, unique } from '../00_test_helper.mjs'

export const name = 'tutorial three publish subscribe'

export async function run(suite) {
  await suite.withConnection(async (conn) => {
    const publisher = await conn.createChannel()
    const consumerOne = await conn.createChannel()
    const consumerTwo = await conn.createChannel()

    try {
      const exchange = unique('tutorial.three.logs')
      await publisher.assertExchange(exchange, 'fanout', { durable: false })

      const queueOne = await consumerOne.assertQueue('', { exclusive: true })
      const queueTwo = await consumerTwo.assertQueue('', { exclusive: true })

      await consumerOne.bindQueue(queueOne.queue, exchange, '')
      await consumerTwo.bindQueue(queueTwo.queue, exchange, '')

      const receivedOne = consumeOne(consumerOne, queueOne.queue, 'consumer one')
      const receivedTwo = consumeOne(consumerTwo, queueTwo.queue, 'consumer two')

      publisher.publish(exchange, '', Buffer.from('info: publish subscribe'))

      expect(await receivedOne === 'info: publish subscribe', 'consumer one received unexpected body')
      expect(await receivedTwo === 'info: publish subscribe', 'consumer two received unexpected body')
    } finally {
      await consumerTwo.close().catch(() => {})
      await consumerOne.close().catch(() => {})
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
