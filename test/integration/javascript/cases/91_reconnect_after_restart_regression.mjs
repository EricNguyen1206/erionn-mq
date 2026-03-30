import { expect, unique, waitForOne } from '../00_test_helper.mjs'

export const name = 'regression reconnect after restart'

export async function run(suite) {
  const queue = unique('compat.reconnect')
  await suite.withChannel(async (ch) => {
    await ch.assertQueue(queue, { durable: false })
  })

  await suite.restartBroker()

  await suite.withChannel(async (ch) => {
    await ch.assertQueue(queue, { durable: false })
    const received = waitForOne(ch, queue, async (msg) => {
      ch.ack(msg)
      return msg.content.toString('utf8')
    })
    ch.sendToQueue(queue, Buffer.from('after-reconnect'))
    expect(await received === 'after-reconnect', 'new connection should work after restart')
  })
}
