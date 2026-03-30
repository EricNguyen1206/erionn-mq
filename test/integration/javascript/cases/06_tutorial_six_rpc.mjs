import { expect, openConnection, unique } from '../00_test_helper.mjs'

export const name = 'tutorial six rpc'

export async function run(suite) {
  const serverConn = await openConnection(suite.amqpURL)
  const clientConn = await openConnection(suite.amqpURL)
  const server = await serverConn.createChannel()
  const client = await clientConn.createChannel()

  try {
    await server.assertQueue('rpc_queue', { durable: true })
    await server.prefetch(1)

    server.consume('rpc_queue', async (msg) => {
      if (!msg) {
        return
      }
      const n = Number.parseInt(msg.content.toString('utf8'), 10)
      const response = fib(n).toString()
      server.sendToQueue(msg.properties.replyTo, Buffer.from(response), {
        correlationId: msg.properties.correlationId,
      })
      server.ack(msg)
    }, { noAck: false })

    const replyQueue = await client.assertQueue('', { exclusive: true })
    const correlationId = unique('tutorial.six.rpc')
    const reply = new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('timed out waiting for rpc reply')), 3000)
      client.consume(replyQueue.queue, (msg) => {
        if (!msg) {
          clearTimeout(timer)
          reject(new Error('rpc consumer cancelled unexpectedly'))
          return
        }
        if (msg.properties.correlationId !== correlationId) {
          return
        }
        clearTimeout(timer)
        resolve(msg.content.toString('utf8'))
      }, { noAck: true }).catch((err) => {
        clearTimeout(timer)
        reject(err)
      })
    })

    client.sendToQueue('rpc_queue', Buffer.from('8'), {
      correlationId,
      replyTo: replyQueue.queue,
    })

    expect(await reply === '21', 'unexpected rpc reply body')
  } finally {
    await client.close().catch(() => {})
    await server.close().catch(() => {})
    await clientConn.close().catch(() => {})
    await serverConn.close().catch(() => {})
  }
}

function fib(n) {
  if (n < 2) {
    return n
  }
  return fib(n - 1) + fib(n - 2)
}
