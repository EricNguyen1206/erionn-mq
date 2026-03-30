import { expect, openConnection, sleep, unique } from '../00_test_helper.mjs'

export const name = 'tutorial two work queues'

export async function run(suite) {
  await persistentTaskSurvivesBrokerRestart(suite)
  await unackedTaskIsRedeliveredToAnotherWorker(suite)
  await prefetchOneKeepsSecondTaskUntilFirstIsAcked(suite)
}

async function persistentTaskSurvivesBrokerRestart(suite) {
  const queue = unique('tutorial.two.tasks')

  await suite.withConnection(async (conn) => {
    const ch = await conn.createChannel()
    try {
      await ch.assertQueue(queue, { durable: true })
      ch.sendToQueue(queue, Buffer.from('durable task'), { persistent: true })
    } finally {
      await ch.close().catch(() => {})
    }
  })

  await suite.restartBroker()

  await suite.withChannel(async (ch) => {
    await ch.assertQueue(queue, { durable: true })
    const message = await consumeOne(ch, queue, 'durable task after restart')
    expect(message.content.toString('utf8') === 'durable task', 'unexpected durable task body after restart')
    ch.ack(message)
  })
}

async function unackedTaskIsRedeliveredToAnotherWorker(suite) {
  const queue = unique('tutorial.two.redelivery')

  await suite.withChannel(async (ch) => {
    await ch.assertQueue(queue, { durable: true })
    ch.sendToQueue(queue, Buffer.from('important task'), { persistent: true })
  })

  const workerOneConn = await openConnection(suite.amqpURL)
  const workerOne = await workerOneConn.createChannel()
  await workerOne.prefetch(1)
  const firstDelivery = await consumeOne(workerOne, queue, 'first worker delivery')
  expect(firstDelivery.content.toString('utf8') === 'important task', 'unexpected first worker body')
  await workerOneConn.close().catch(() => {})

  await suite.withConnection(async (workerTwoConn) => {
    const workerTwo = await workerTwoConn.createChannel()
    try {
      await workerTwo.prefetch(1)
      const redelivered = await consumeOne(workerTwo, queue, 'redelivered task')
      expect(redelivered.fields.redelivered, 'expected task to be redelivered after worker disconnect')
      expect(redelivered.content.toString('utf8') === 'important task', 'unexpected redelivered task body')
      workerTwo.ack(redelivered)
    } finally {
      await workerTwo.close().catch(() => {})
    }
  })
}

async function prefetchOneKeepsSecondTaskUntilFirstIsAcked(suite) {
  const queue = unique('tutorial.two.prefetch')

  await suite.withConnection(async (conn) => {
    const ch = await conn.createChannel()
    try {
      await ch.assertQueue(queue, { durable: true })
      await ch.prefetch(1)
      ch.sendToQueue(queue, Buffer.from('first task'), { persistent: true })
      ch.sendToQueue(queue, Buffer.from('second task'), { persistent: true })

      let firstSeen
      let secondSeen = false
      let resolveFirst
      let resolveSecond
      const first = new Promise((resolve) => { resolveFirst = resolve })
      const second = new Promise((resolve) => { resolveSecond = resolve })

      const { consumerTag } = await ch.consume(queue, (msg) => {
        if (!msg) {
          return
        }
        if (!firstSeen) {
          firstSeen = msg
          resolveFirst(msg)
          return
        }
        secondSeen = true
        resolveSecond(msg)
      }, { noAck: false })

      const firstMessage = await withTimeout(first, 3000, 'first work queue task')
      expect(firstMessage.content.toString('utf8') === 'first task', 'unexpected first task body')

      await sleep(300)
      expect(!secondSeen, 'second task arrived before first task was acked')

      ch.ack(firstMessage)

      const secondMessage = await withTimeout(second, 3000, 'second work queue task')
      expect(secondMessage.content.toString('utf8') === 'second task', 'unexpected second task body')
      ch.ack(secondMessage)
      await ch.cancel(consumerTag)
    } finally {
      await ch.close().catch(() => {})
    }
  })
}

async function consumeOne(ch, queue, label) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`timed out waiting for ${label}`)), 3000)
    let consumerTag = ''

    ch.consume(queue, (msg) => {
      if (!msg) {
        clearTimeout(timer)
        reject(new Error(`consumer cancelled while waiting for ${label}`))
        return
      }
      clearTimeout(timer)
      Promise.resolve()
        .then(async () => {
          if (consumerTag) {
            await ch.cancel(consumerTag).catch(() => {})
          }
          resolve(msg)
        })
        .catch(reject)
    }, { noAck: false })
      .then((result) => {
        consumerTag = result.consumerTag
      })
      .catch((err) => {
        clearTimeout(timer)
        reject(err)
      })
  })
}

function withTimeout(promise, ms, label) {
  return Promise.race([
    promise,
    sleep(ms).then(() => {
      throw new Error(`timed out: ${label}`)
    }),
  ])
}
