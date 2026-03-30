import { createSuite } from './00_test_helper.mjs'
import * as helloWorld from './cases/01_tutorial_one_hello_world.mjs'
import * as workQueues from './cases/02_tutorial_two_work_queues.mjs'
import * as publishSubscribe from './cases/03_tutorial_three_publish_subscribe.mjs'
import * as routing from './cases/04_tutorial_four_routing.mjs'
import * as topics from './cases/05_tutorial_five_topics.mjs'
import * as rpc from './cases/06_tutorial_six_rpc.mjs'
import * as durableRestart from './cases/90_durable_restart_regression.mjs'
import * as reconnectAfterRestart from './cases/91_reconnect_after_restart_regression.mjs'
import * as multipleConcurrentConnections from './cases/92_multiple_concurrent_connections_regression.mjs'
import * as multipleChannelsPerConnection from './cases/93_multiple_channels_per_connection_regression.mjs'
import * as channelCloseCleanup from './cases/94_channel_close_cleanup_regression.mjs'
import * as connectionCloseCleanup from './cases/95_connection_close_cleanup_regression.mjs'

const cases = [
  helloWorld,
  workQueues,
  publishSubscribe,
  routing,
  topics,
  rpc,
  durableRestart,
  reconnectAfterRestart,
  multipleConcurrentConnections,
  multipleChannelsPerConnection,
  channelCloseCleanup,
  connectionCloseCleanup,
]

try {
  for (const testCase of cases) {
    const suite = createSuite()
    try {
      await suite.setup()
      await suite.run(testCase.name, testCase.run)
    } catch (err) {
      console.error(err?.stack ?? err)
      if (suite.brokerLogs.length > 0) {
        console.error('\nBroker logs:\n' + suite.brokerLogs.join(''))
      }
      process.exitCode = 1
      break
    } finally {
      await suite.teardown()
    }
  }
  if (process.exitCode !== 1) {
    console.log('amqplib compatibility suite passed')
  }
} catch (err) {
  console.error(err?.stack ?? err)
  process.exitCode = 1
}
