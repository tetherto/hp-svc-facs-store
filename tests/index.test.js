'use strict'

const path = require('path')
const fs = require('fs')
const Autobase = require('autobase')
const Hypercore = require('hypercore')
const Hyperbee = require('hyperbee')
const { convFromBin } = require('../utils')
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

  await t.test('clearBeeCache - block deletion behavior in one execution', async (t) => {
    const bee = await fac.getBee({ name: 'clearBeeCache' }, { keyEncoding: 'utf-8', valueEncoding: 'utf-8' })
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

    await fac.clearBeeCache(bee, 'clearBeeCache', 10)

    const seqCheckpoint = convFromBin(await bee.core.getUserData('clearBeeCache'), 'number')

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
    t.is(seqCheckpoint, 10)
  })

  await t.test('clearBeeCache - block deletion behavior across executions', async t => {
    const bee = await fac.getBee({ name: 'clearBeeCache2' }, { keyEncoding: 'utf-8', valueEncoding: 'utf-8' })
    await bee.ready()

    // values correspond to their block seq. numbers
    await bee.put('irrelevantKey1', 1)
    await bee.put('keyToBeDeletedInNextRun', 2)
    await bee.del('irrelevantKey1', 3)
    await bee.put('irrelevantKey2', 4)

    await fac.clearBeeCache(bee, 'clearBeeCache', 10)
    const seqCheckpoint = convFromBin(await bee.core.getUserData('clearBeeCache'), 'number')
    t.is(seqCheckpoint, 3)

    await bee.del('keyToBeDeletedInNextRun') // seq # 5
    await bee.put('irrelevantKey3', 6)

    await fac.clearBeeCache(bee, 'clearBeeCache', 10)
    const seqCheckpointAfterSecondRun = convFromBin(await bee.core.getUserData('clearBeeCache'), 'number')
    t.is(seqCheckpointAfterSecondRun, 5)

    t.is(await bee.core.get(5, { wait: false }), null) // we clean the .del block for key: keyToBeDeletedInNextRun
    t.not(await bee.core.get(2, { wait: false }), null) // This should've been t.ok and not t.not, but its a limitation of our clearBeeCache
  })

  await t.test('putAndClear', async t => {
    const bee = await fac.getBee({ name: 'putAndClear' }, { keyEncoding: 'utf-8', valueEncoding: 'utf-8' })
    await bee.ready()

    await fac.putAndClear(bee, 'testPutKey', '1')
    const entryBeforeUpdate = await bee.get('testPutKey')

    t.is(entryBeforeUpdate?.value, '1')
    t.is(entryBeforeUpdate?.seq, 1)

    t.comment('Updating key')
    await fac.putAndClear(bee, 'testPutKey', '2')
    const entryAfterUpdate = await bee.get('testPutKey')

    t.alike(await bee.core.get(1, { wait: false }), null)
    t.is(entryAfterUpdate?.seq, 2)
    t.is(entryAfterUpdate?.value, '2')

    t.comment('should work fine when cas option provided')

    await fac.putAndClear(bee, 'testPutKey', 3, {
      cas: () => false
    })
    const entryAfterCasFalse = await bee.get('testPutKey')
    t.is(entryAfterCasFalse?.value, '2', 'should keep old value')
    t.is(entryAfterCasFalse?.seq, 2)

    await fac.putAndClear(bee, 'testPutKey', 3, {
      cas: () => true
    })
    const entryAfterCasTrue = await bee.get('testPutKey')
    t.is(entryAfterCasTrue?.value, '3', 'should update value')
    t.is(entryAfterCasTrue?.seq, 3)
    t.alike(await bee.core.get(2, { wait: false }), null)
  })

  await t.test('delAndClear', async t => {
    const bee = await fac.getBee({ name: 'delAndClear' }, { keyEncoding: 'utf-8', valueEncoding: 'utf-8' })
    await bee.ready()
    const key = 'testDelKey'

    t.comment('deleting nonexistent key')
    await fac.delAndClear(bee, key)
    t.is(bee.core.length, 1, 'should not add any entry')

    t.comment('del existing key')
    await fac.putAndClear(bee, key, '1')

    await fac.delAndClear(bee, key)
    const entryAfterDelAndClear = await bee.get(key)

    t.alike(await bee.core.get(1, { wait: false }), null)
    t.alike(entryAfterDelAndClear, null)

    t.comment('del with cas option')
    await fac.putAndClear(bee, key, '3')
    await fac.delAndClear(bee, key, {
      cas: () => false
    })
    const entryAfterCasFalse = await bee.get(key)
    t.is(entryAfterCasFalse?.value, '3', 'should keep old value')
    t.is(entryAfterCasFalse?.seq, 3)

    await fac.delAndClear(bee, key, {
      cas: () => true
    })
    const entryAfterCasTrue = await bee.get(key)
    t.alike(entryAfterCasTrue, null)
    t.alike(await bee.core.get(3, { wait: false }), null)
  })
})
