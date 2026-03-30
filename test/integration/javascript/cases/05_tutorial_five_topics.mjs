import { expect, unique } from '../00_test_helper.mjs'

export const name = 'tutorial five topics'

export async function run(suite) {
  await suite.withConnection(async (conn) => {
    const publisher = await conn.createChannel()
    const kernelConsumer = await conn.createChannel()
    const criticalConsumer = await conn.createChannel()
    const allConsumer = await conn.createChannel()

    try {
      const exchange = unique('tutorial.five.topic')
      await publisher.assertExchange(exchange, 'topic', { durable: false })

      const kernelQueue = await kernelConsumer.assertQueue('', { exclusive: true })
      const criticalQueue = await criticalConsumer.assertQueue('', { exclusive: true })
      const allQueue = await allConsumer.assertQueue('', { exclusive: true })

      await kernelConsumer.bindQueue(kernelQueue.queue, exchange, 'kern.*')
      await criticalConsumer.bindQueue(criticalQueue.queue, exchange, '*.critical')
      await allConsumer.bindQueue(allQueue.queue, exchange, '#')

      const kernelMessage = consumeOne(kernelConsumer, kernelQueue.queue, 'kernel consumer')
      const criticalMessage = consumeOne(criticalConsumer, criticalQueue.queue, 'critical consumer')
      const allMessage = consumeOne(allConsumer, allQueue.queue, 'all consumer')

      publisher.publish(exchange, 'kern.critical', Buffer.from('kernel critical'))

      expect(await kernelMessage === 'kernel critical', 'kernel consumer received unexpected body')
      expect(await criticalMessage === 'kernel critical', 'critical consumer received unexpected body')
      expect(await allMessage === 'kernel critical', 'all consumer received unexpected body')

      const nextAll = consumeOne(allConsumer, allQueue.queue, 'all consumer second delivery')
      publisher.publish(exchange, 'auth.info', Buffer.from('auth info'))

      expect(await nextAll === 'auth info', 'all consumer second delivery received unexpected body')
      const kernelState = await kernelConsumer.checkQueue(kernelQueue.queue)
      const criticalState = await criticalConsumer.checkQueue(criticalQueue.queue)
      expect(kernelState.messageCount === 0, 'kernel consumer should not receive auth.info')
      expect(criticalState.messageCount === 0, 'critical consumer should not receive auth.info')
    } finally {
      await allConsumer.close().catch(() => {})
      await criticalConsumer.close().catch(() => {})
      await kernelConsumer.close().catch(() => {})
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
