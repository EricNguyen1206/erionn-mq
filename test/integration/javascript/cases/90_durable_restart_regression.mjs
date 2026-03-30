import { expect, publishConfirmed, unique, waitForOne } from '../00_test_helper.mjs'

export const name = 'regression durable restart'

export async function run(suite) {
  const queue = unique('compat.durable')
  await suite.withConnection(async (conn) => {
    const ch = await conn.createConfirmChannel()
    try {
      await ch.assertQueue(queue, { durable: true })
      await publishConfirmed(ch, '', queue, Buffer.from('persisted'), { persistent: true })
    } finally {
      await ch.close().catch(() => {})
    }
  })

  await suite.restartBroker()

  await suite.withChannel(async (ch) => {
    await ch.assertQueue(queue, { durable: true })
    const received = await waitForOne(ch, queue, async (msg) => {
      ch.ack(msg)
      return msg.content.toString('utf8')
    })
    expect(received === 'persisted', 'durable message should survive restart')
  })
}
