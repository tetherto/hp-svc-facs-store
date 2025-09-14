'use strict'

const path = require('path')
const fs = require('fs')
const Autobase = require('autobase')
const Hypercore = require('hypercore')
const Hyperbee = require('hyperbee')
const { test, hook } = require('brittle')

const StoreFacility = require('../index')

test('facility', async (t) => {
  const caller = {}
  const ctx = { env: 'test' }
  const opts = { storeDir: path.join(__dirname) + '/store/1' }
  const fac = new StoreFacility(caller, opts, ctx)

  hook('setup hook', t => {
    cleanupStore(opts.storeDir)
    cleanupStore(path.join(__dirname) + '/store/2')
  })

  const cleanupStore = (storeDir) => {
    if (fs.existsSync(storeDir)) {
      fs.rmSync(storeDir, { recursive: true })
    }
  }

  const getCleanCheckpoint = async (bee, clearKey) => {
    const [last, next] = JSON.parse(await bee.core.getUserData(clearKey) || '[0,0]')
    return { last, next }
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

    const core = fac.getCore({ name: 'core-1' })
    await core.ready()
    t.ok(core instanceof Hypercore)
    await core.append('log 1')
    await core.append('log 2')
    const value = await core.get(1)
    t.is(value.toString(), 'log 2')
  })

  await t.test('getBee', async (t) => {
    t.comment('getBee tests')

    const bee = fac.getBee({ name: 'bee-1' }, { keyEncoding: 'utf-8', valueEncoding: 'utf-8' })
    await bee.ready()
    t.ok(bee instanceof Hyperbee)
    await bee.put('foo', 'bar')
    const res = await bee.get('foo')
    t.is(res.value, 'bar')
  })

  await t.test('getBase', async (t) => {
    t.comment('getBase tests')

    const base = fac.getBase({
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

  await t.test('clearBeeCache - initial run skips cleanup, db cleanup is always 1 version behind', async (t) => {
    const clearKey = 'clearBeeCache-0'
    const bee = fac.getBee({ name: 'clearBeeCache-0' }, { keyEncoding: 'utf-8', valueEncoding: 'utf-8' })
    await bee.ready()

    await bee.put('keyToBeRemoved', 1)
    await bee.put('keyToBePutOnce', 2)
    await bee.del('keyToBeRemoved')

    await fac.clearBeeCache(bee, clearKey)

    const checkpoint = await getCleanCheckpoint(bee, clearKey)
    const entryToBePutOnce = await bee.get('keyToBePutOnce')

    t.not(await bee.core.get(1, { wait: false }), null)
    t.is(entryToBePutOnce?.seq, 2)
    t.is(entryToBePutOnce?.value, '2')
    t.not(await bee.core.get(3, { wait: false }), null)

    t.is(checkpoint.last, 0)
    t.is(checkpoint.next, 3)
  })

  await t.test('clearBeeCache - block deletion behavior in one execution', async (t) => {
    const clearKey = 'clearBeeCache-1'
    const bee = fac.getBee({ name: 'clearBeeCache-1' }, { keyEncoding: 'utf-8', valueEncoding: 'utf-8' })
    await bee.ready()

    await bee.put('keyToBeDeletedAndAddedAgain', 1)
    await bee.put('keyToBePutOnce', 2)
    await bee.put('keyToBeDeletedAndAddedAgain', 3)
    await bee.del('keyToBeDeletedAndAddedAgain')
    await bee.put('keyToBeDeletedAndAddedAgain', 5)
    await bee.put('keyToBePutTwice', 6)
    await bee.put('keyToBePutTwice', 7)
    await bee.put('keyToBeRemoved', 8)
    await bee.del('keyToBeRemoved')
    await bee.put('keyToBeRemainUnchanged', 10)
    await bee.del('keyToBeRemainUnchanged')

    await fac.clearBeeCache(bee, clearKey) // skip initial run
    await bee.put('keyToBeInsertedAfterClean', 12)
    await bee.put('keyToBeInsertedAndDeletedAfterClean', 13)
    await bee.del('keyToBeInsertedAndDeletedAfterClean')

    await fac.clearBeeCache(bee, clearKey)
    const checkpoint = await getCleanCheckpoint(bee, clearKey)

    const entryToBeDeletedAndAddedAgain = await bee.get('keyToBeDeletedAndAddedAgain', { wait: false })
    const entryToBePutTwice = await bee.get('keyToBePutTwice')
    const entryToBePutOnce = await bee.get('keyToBePutOnce')

    t.alike(await bee.core.get(1, { wait: false }), null)
    t.is(entryToBePutOnce?.seq, 2)
    t.is(entryToBePutOnce?.value, '2')
    t.alike(await bee.core.get(3, { wait: false }), null)
    t.alike(await bee.core.get(4, { wait: false }), null)
    t.is(entryToBeDeletedAndAddedAgain?.seq, 5)
    t.is(entryToBeDeletedAndAddedAgain?.value, '5')
    t.alike(await bee.core.get(6, { wait: false }), null)
    t.is(entryToBePutTwice?.seq, 7)
    t.is(entryToBePutTwice?.value, '7')
    t.alike(await bee.core.get(8, { wait: false }), null)
    t.alike(await bee.core.get(9, { wait: false }), null)
    t.alike(await bee.core.get(10, { wait: false }), null)
    t.ok(await bee.core.get(11, { wait: false }), 'Last block should remain')
    t.not(await bee.core.get(12, { wait: false }), null)
    t.not(await bee.core.get(13, { wait: false }), null)
    t.not(await bee.core.get(14, { wait: false }), null)

    t.is(checkpoint.last, 11)
    t.is(checkpoint.next, 14)
  })

  await t.test('clearBeeCache - block deletion behavior across executions', async t => {
    const clearKey = 'clearBeeCache-2'
    const bee = fac.getBee({ name: 'clearBeeCache-2' }, { keyEncoding: 'utf-8', valueEncoding: 'utf-8' })
    await bee.ready()

    // values correspond to their block seq. numbers
    await bee.put('irrelevantKey1', 1)
    await bee.put('keyToBeDeletedInNextRun', 2)
    await bee.del('irrelevantKey1', 3)
    await bee.put('irrelevantKey2', 4)

    await fac.clearBeeCache(bee, clearKey)
    let checkpoint = await getCleanCheckpoint(bee, clearKey)
    t.is(checkpoint.last, 0)
    t.is(checkpoint.next, 4)

    await bee.del('keyToBeDeletedInNextRun') // seq # 5
    await bee.put('irrelevantKey3', 6)

    await fac.clearBeeCache(bee, clearKey)
    checkpoint = await getCleanCheckpoint(bee, clearKey)
    t.is(checkpoint.last, 4)
    t.is(checkpoint.next, 6)

    await fac.clearBeeCache(bee, clearKey)

    t.is(await bee.core.get(2, { wait: false }), null)
    t.is(await bee.core.get(5, { wait: false }), null)

    checkpoint = await getCleanCheckpoint(bee, clearKey)
    t.is(checkpoint.last, 6)
    t.is(checkpoint.next, 6)
  })

  await t.test('clearBeeCache - block deletion behavior with max range', async t => {
    const clearKey = 'clearBeeCache-3'
    const bee = fac.getBee({ name: 'clearBeeCache-3' }, { keyEncoding: 'utf-8', valueEncoding: 'utf-8' })
    await bee.ready()

    // values correspond to their block seq. numbers
    await bee.put('irrelevantKey1', 1)
    await bee.put('keyToBeDeletedInNextRun', 2)
    await bee.del('irrelevantKey1', 3)
    await bee.put('irrelevantKey2', 4)

    await fac.clearBeeCache(bee, clearKey)
    let checkpoint = await getCleanCheckpoint(bee, clearKey)
    t.is(checkpoint.last, 0)
    t.is(checkpoint.next, 4)

    await bee.del('keyToBeDeletedInNextRun') // seq # 5
    await bee.put('irrelevantKey3', 6)

    await fac.clearBeeCache(bee, clearKey, 1)
    checkpoint = await getCleanCheckpoint(bee, clearKey)
    t.is(checkpoint.last, 4)
    t.is(checkpoint.next, 5)

    await fac.clearBeeCache(bee, clearKey, 1)

    t.is(await bee.core.get(2, { wait: false }), null)
    t.not(await bee.core.get(5, { wait: false }), null)

    checkpoint = await getCleanCheckpoint(bee, clearKey)
    t.is(checkpoint.last, 5)
    t.is(checkpoint.next, 6)
  })
})
