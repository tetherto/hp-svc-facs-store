'use strict'

const path = require('path')
const fs = require('fs')
const Autobase = require('autobase')
const Hypercore = require('hypercore')
const Hyperbee = require('hyperbee')
const { test } = require('brittle')

const StoreFacility = require('../index')

test('facility', async (t) => {
  const caller = {}
  const ctx = { env: 'test' }
  const opts = { storeDir: path.join(__dirname) + '/store/1' }
  const fac = new StoreFacility(caller, opts, ctx)

  const cleanupStore = (storeDir) => {
    if (fs.existsSync(storeDir)) {
      fs.rmSync(storeDir, { recursive: true })
    }
  }

  t.teardown(async () => {
    await new Promise((resolve, reject) => fac.stop((err) => err ? reject(err) : resolve()))
    cleanupStore(opts.storeDir)
  })

  await t.test('start', async (t) => {
    t.comment('start tests')
    const storeDir = path.join(__dirname) + '/store/2'
    let fac2 = new StoreFacility(caller, {}, ctx)

    t.teardown(async () => {
      await new Promise((resolve, reject) => fac2.stop((err) => err ? reject(err) : resolve()))
      cleanupStore(storeDir)
    })

    t.comment('throws when storage path is missing')
    await t.exception(
      () => new Promise((resolve, reject) => fac2.start((err) => err ? reject(err) : resolve())),
      'ERR_FACS_STORE_STORAGE_PATH_INVALID'
    )

    t.comment('creates store when path is present')
    fac2 = new StoreFacility(caller, { storeDir }, ctx)
    await new Promise((resolve, reject) => fac2.start((err) => err ? reject(err) : resolve()))
    await fac2.store.ready()
    t.ok(fs.existsSync(storeDir))
  })

  await new Promise((resolve, reject) => fac.start((err) => err ? reject(err) : resolve()))

  await t.test('getCore', async (t) => {
    t.comment('getCore tests')

    const core = await fac.getCore({ name: 'core-1' })
    await core.ready()
    t.ok(core instanceof Hypercore)
    await core.append('log 1')
    await core.append('log 2')
    const value = await core.get(1)
    t.is(value.toString(), 'log 2')
  })

  await t.test('getBee', async (t) => {
    t.comment('getBee tests')

    const bee = await fac.getBee({ name: 'bee-1' }, { keyEncoding: 'utf-8', valueEncoding: 'utf-8' })
    await bee.ready()
    t.ok(bee instanceof Hyperbee)
    await bee.put('foo', 'bar')
    const res = await bee.get('foo')
    t.is(res.value, 'bar')
  })

  await t.test('getBase', async (t) => {
    t.comment('getBase tests')

    const base = await fac.getBase({
      optimistic: true,
      valueEncoding: 'json',
      open: (store) => new Hyperbee(store.get('base-1'), { keyEncoding: 'utf-8', valueEncoding: 'utf-8' }),
      apply: async (nodes, view, host) => {
        for (const node of nodes) {
          const { value } = node
          await host.ackWriter(node.from.key)
          await view.put(value.key, value.value)
        }
      }
    })
    await base.ready()
    t.ok(base instanceof Autobase)
    await base.append({ key: 'foo', value: 'bar' }, { optimistic: true })
    const view = base.view
    t.is((await view.get('foo')).value, 'bar')
  })
})
