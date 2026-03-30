import { expect, unique } from '../00_test_helper.mjs'

export const name = 'hello world'

const msg = 'Hello World!'

export async function run(suite) {
  const queue = unique('tutorial.one.hello')

  await suite.withConnection(async (conn) => {
    const sender = await conn.createChannel()
    const receiver = await conn.createChannel()
    try {
      await sender.assertQueue(queue, { durable: false })
      await receiver.assertQueue(queue, { durable: false })

      const received = await new Promise((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error('timed out waiting for hello world message')), 3000)
        receiver.consume(queue, (m) => {
          if (!m) {
            clearTimeout(timer)
            reject(new Error('consumer cancelled unexpectedly'))
            return
          }
          clearTimeout(timer)
          resolve(m.content.toString())
        }, { noAck: true }).catch((err) => {
          clearTimeout(timer)
          reject(err)
        })
        sender.sendToQueue(queue, Buffer.from(msg))
      })

      expect(received === msg, 'unexpected body from hello world tutorial flow')
    } finally {
      await receiver.close().catch(() => {})
      await sender.close().catch(() => {})
    }
  })
}
