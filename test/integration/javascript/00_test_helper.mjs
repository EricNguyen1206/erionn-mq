import amqplib from 'amqplib'
import { spawn, execFile } from 'node:child_process'
import { randomUUID } from 'node:crypto'
import { once } from 'node:events'
import { mkdtemp, rm } from 'node:fs/promises'
import net from 'node:net'
import os from 'node:os'
import path from 'node:path'
import process from 'node:process'
import { fileURLToPath } from 'node:url'

const here = path.dirname(fileURLToPath(import.meta.url))
const root = path.resolve(here, '..', '..', '..')

export function createSuite() {
  return new CompatibilitySuite()
}

export async function openConnection(url) {
  const conn = await amqplib.connect(url)
  conn.on('error', () => {})
  return conn
}

class CompatibilitySuite {
  constructor() {
    this.workDir = ''
    this.binary = ''
    this.dataDir = ''
    this.amqpPort = 0
    this.managementPort = 0
    this.amqpURL = ''
    this.managementURL = ''
    this.authHeader = `Basic ${Buffer.from('guest:guest').toString('base64')}`
    this.broker = undefined
    this.brokerLogs = []
  }

  async setup() {
    this.workDir = await mkdtemp(path.join(os.tmpdir(), 'gobitmq-compat-'))
    this.binary = path.join(this.workDir, process.platform === 'win32' ? 'gobitmq.exe' : 'gobitmq')
    this.dataDir = path.join(this.workDir, 'data')
    this.amqpPort = await freePort()
    this.managementPort = await freePort()
    this.amqpURL = `amqp://127.0.0.1:${this.amqpPort}`
    this.managementURL = `http://127.0.0.1:${this.managementPort}`
    await this.buildBroker()
    this.broker = await this.startBroker()
  }

  async teardown() {
    await this.stopBroker(this.broker)
    if (this.workDir) {
      await rm(this.workDir, { recursive: true, force: true })
    }
  }

  async run(name, fn) {
    process.stdout.write(`- ${name}... `)
    await fn(this)
    console.log('ok')
  }

  async buildBroker() {
    await execFileAsync('go', ['build', '-o', this.binary, './cmd'], { cwd: root })
  }

  async startBroker() {
    this.brokerLogs = []
    const child = spawn(this.binary, [], {
      cwd: root,
      env: {
        ...process.env,
        GOBITMQ_AMQP_ADDR: `127.0.0.1:${this.amqpPort}`,
        GOBITMQ_MGMT_ADDR: `127.0.0.1:${this.managementPort}`,
        GOBITMQ_DATA_DIR: this.dataDir,
        GOBITMQ_MGMT_USERS: 'guest:guest:admin',
      },
      stdio: ['ignore', 'pipe', 'pipe'],
    })
    child.stdout?.on('data', (chunk) => this.brokerLogs.push(chunk.toString('utf8')))
    child.stderr?.on('data', (chunk) => this.brokerLogs.push(chunk.toString('utf8')))
    await Promise.all([waitForPort(this.amqpPort, child), waitForPort(this.managementPort, child)])
    return child
  }

  async restartBroker() {
    await this.stopBroker(this.broker)
    this.broker = await this.startBroker()
  }

  async stopBroker(child) {
    if (!child || child.exitCode !== null) {
      return
    }
    child.kill()
    if (await waitForExit(child, 5000)) {
      return
    }
    if (process.platform === 'win32') {
      await execFileAsync('taskkill', ['/pid', String(child.pid), '/t', '/f']).catch(() => {})
    } else {
      child.kill('SIGKILL')
    }
    await waitForExit(child, 5000)
  }

  async withConnection(run) {
    const conn = await openConnection(this.amqpURL)
    try {
      return await run(conn)
    } finally {
      await conn.close().catch(() => {})
    }
  }

  async withChannel(run) {
    return this.withConnection(async (conn) => {
      const ch = await conn.createChannel()
      try {
        return await run(ch)
      } finally {
        await ch.close().catch(() => {})
      }
    })
  }

  async request(resource, { auth = false } = {}) {
    const headers = auth ? { Authorization: this.authHeader } : {}
    const res = await fetch(`${this.managementURL}${resource}`, { headers })
    const text = await res.text()
    let body = text
    if (text) {
      try {
        body = JSON.parse(text)
      } catch {
        body = text
      }
    }
    return { status: res.status, body }
  }
}

/**
 * Wait to consume at least one message on a queue.
 * @param {import('amqplib').Channel} ch
 * @param {string} queue
 * @param {(msg: import('amqplib').Message) => Promise<any>} handler
 * @returns {Promise<any>}
 */
export async function waitForOne(ch, queue, handler) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`timed out waiting for message on ${queue}`)), 3000)
    let consumerTag = ''
    ch.consume(queue, (msg) => {
      if (!msg) {
        clearTimeout(timer)
        reject(new Error('consumer cancelled unexpectedly'))
        return
      }
      Promise.resolve(handler(msg))
        .then(async (value) => {
          clearTimeout(timer)
          if (consumerTag) {
            await ch.cancel(consumerTag).catch(() => {})
          }
          resolve(value)
        })
        .catch((err) => {
          clearTimeout(timer)
          reject(err)
        })
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

export async function publishConfirmed(ch, exchange, routingKey, body, options = {}) {
  await new Promise((resolve, reject) => {
    ch.publish(exchange, routingKey, body, options, (err) => {
      if (err) {
        reject(err)
        return
      }
      resolve()
    })
  })
}

export function unique(prefix) {
  return `${prefix}.${randomUUID().slice(0, 8)}`
}

export function expect(condition, message) {
  if (!condition) {
    throw new Error(message)
  }
}

export function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

export async function withTimeout(promise, ms, label) {
  return Promise.race([
    promise,
    sleep(ms).then(() => {
      throw new Error(`timed out: ${label}`)
    }),
  ])
}

async function waitForPort(port, child) {
  const started = Date.now()
  while (Date.now() - started < 10000) {
    if (child.exitCode !== null) {
      throw new Error(`broker exited before port ${port} was ready`)
    }
    try {
      await new Promise((resolve, reject) => {
        const socket = net.createConnection({ host: '127.0.0.1', port }, () => {
          socket.destroy()
          resolve()
        })
        socket.on('error', reject)
      })
      return
    } catch {
      await sleep(100)
    }
  }
  throw new Error(`timed out waiting for port ${port}`)
}

async function waitForExit(child, ms) {
  return Promise.race([
    once(child, 'exit').then(() => true),
    sleep(ms).then(() => false),
  ])
}

function freePort() {
  return new Promise((resolve, reject) => {
    const server = net.createServer()
    server.listen(0, '127.0.0.1', () => {
      const address = server.address()
      if (!address || typeof address === 'string') {
        server.close(() => reject(new Error('failed to allocate free port')))
        return
      }
      server.close((err) => {
        if (err) {
          reject(err)
          return
        }
        resolve(address.port)
      })
    })
    server.on('error', reject)
  })
}

function execFileAsync(command, args, options = {}) {
  return new Promise((resolve, reject) => {
    execFile(command, args, options, (err, stdout, stderr) => {
      if (err) {
        err.stdout = stdout
        err.stderr = stderr
        reject(err)
        return
      }
      resolve({ stdout, stderr })
    })
  })
}
