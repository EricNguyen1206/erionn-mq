import { unique } from '../00_test_helper.mjs'

export const name = 'regression channel close cleans up consumers'

export async function run(suite) {
  const queue = unique('compat.ch-close')

  await suite.withConnection(async (conn) => {
    const ch = await conn.createChannel()
    await ch.assertQueue(queue, { durable: false })
    await ch.consume(queue, () => {}, { consumerTag: 'closer', noAck: false })
    await ch.close()

    const ch2 = await conn.createChannel()
    try {
      await ch2.consume(queue, () => {}, { consumerTag: 'closer', noAck: false })
      await ch2.cancel('closer')
    } finally {
      await ch2.close().catch(() => {})
    }
  })
}
