'use strict'

const path = require('path')
const fs = require('fs')
const Hypercore = require('hypercore')
const Hyperbee = require('hyperbee')
const { test } = require('brittle')

const PrevCorestore = require('@prev/corestore')
const PrevHypercore = require('@prev/corestore/node_modules/hypercore')

const StoreFacility = require('../index')

test('compatiblity', async (t) => {
  const caller = {}
  const ctx = { env: 'test' }
  const storeDir = path.join(__dirname) + '/store/compatiblity'
  const beeOpts = { keyEncoding: 'utf-8', valueEncoding: 'utf-8' }

  const cleanupStore = () => {
    if (fs.existsSync(storeDir)) {
      fs.rmSync(storeDir, { recursive: true })
    }
  }

  t.teardown(async () => {
    await new Promise((resolve, reject) => fac.stop((err) => err ? reject(err) : resolve()))
    cleanupStore(storeDir)
  })

  t.comment('hypercore@10/corestore@6 write and hypercore@11/corestore@7 read')
  const prevStore = new PrevCorestore(storeDir)
  await prevStore.ready()

  const prevCore = prevStore.get({ name: 'core-1' })
  t.ok(prevCore instanceof PrevHypercore)
  await prevCore.ready()
  await prevCore.append('block-1')
  t.is((await prevCore.get(0)).toString(), 'block-1')

  const prevBee = new Hyperbee(prevStore.get({ name: 'bee-1' }), beeOpts)
  t.ok(prevBee instanceof Hyperbee)
  await prevBee.ready()
  await prevBee.put('b1', 'v1')
  t.is((await prevBee.get('b1')).value, 'v1')

  await prevBee.close()
  await prevCore.close()
  await prevStore.close()

  const fac = new StoreFacility(caller, { storeDir }, ctx)
  await new Promise((resolve, reject) => fac.start((err) => err ? reject(err) : resolve()))

  const facCore = await fac.getCore({ name: 'core-1' })
  t.ok(facCore instanceof Hypercore)
  await facCore.ready()
  t.is((await facCore.get(0)).toString(), 'block-1')

  const facBee = await fac.getBee({ name: 'bee-1' }, beeOpts)
  t.ok(facBee instanceof Hyperbee)
  await facBee.ready()
  t.is((await facBee.get('b1')).value, 'v1')

  t.comment('hypercore@11/corestore@7 write')
  facCore.append('block-2')
  t.is((await facCore.get(1)).toString(), 'block-2')

  await facBee.put('b2', 'v2')
  t.is((await facBee.get('b2')).value, 'v2')
})
