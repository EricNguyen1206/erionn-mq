import { expect, openConnection, unique, waitForOne } from '../00_test_helper.mjs'

export const name = 'regression connection close cleans up channels'

export async function run(suite) {
  const queue = unique('compat.conn-close')

  const conn = await openConnection(suite.amqpURL)
  const ch = await conn.createChannel()
  await ch.assertQueue(queue, { durable: false })
  await conn.close()

  await suite.withConnection(async (conn2) => {
    const ch2 = await conn2.createChannel()
    try {
      await ch2.assertQueue(queue, { durable: false })
      const received = waitForOne(ch2, queue, async (msg) => {
        ch2.ack(msg)
        return msg.content.toString('utf8')
      })
      ch2.sendToQueue(queue, Buffer.from('ok'))
      expect(await received === 'ok', 'unexpected body after reconnect')
    } finally {
      await ch2.close().catch(() => {})
    }
  })
}
