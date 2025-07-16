'use strict'

const async = require('async')
const { convFromBin, convIntToBin } = require('./utils')
const Autobase = require('autobase')
const Base = require('bfx-facs-base')
const Corestore = require('corestore')
const Hyperbee = require('hyperbee')
const Hyperswarm = require('hyperswarm')
const LimitedMap = require('./libs/limited-map')

class StoreFacility extends Base {
  constructor (caller, opts, ctx) {
    super(caller, opts, ctx)

    this.name = 'store'

    this.init()
  }

  async getCore (opts = {}) {
    return this.store.get(opts)
  }

  async getBee (opts = {}, beeOpts = {}) {
    const hc = this.store.get(opts)

    return new Hyperbee(hc, beeOpts)
  }

  async getBase (baseOpts, boostrapKey = null) {
    return new Autobase(this.store.session(), boostrapKey, baseOpts)
  }

  async clearBeeCache (bee, ckey, maxSize = 1000) {
    let seqCheckpoint = await bee.core.getUserData(ckey)
    seqCheckpoint = seqCheckpoint ? convFromBin(seqCheckpoint, 'number') : 0
    const map = new LimitedMap(maxSize)

    for await (const entry of bee.createHistoryStream({ gt: seqCheckpoint, lt: bee.core.length - 1 })) {
      if (entry.type === 'del') {
        await bee.core.setUserData(ckey, convIntToBin(entry.seq))
        await bee.core.clear(entry.seq)
        continue
      }

      let seq = map.get(entry.key)
      if (!seq) {
        const latest = await bee.get(entry.key)
        seq = latest?.seq ?? -1
        map.set(entry.key, seq)
      }

      if (seq === -1 || entry.seq !== seq) {
        await bee.core.setUserData(ckey, convIntToBin(entry.seq))
        await bee.core.clear(entry.seq)
      }
    }
    await bee.core.setUserData(ckey, convIntToBin(bee.core.length - 1))
  }

  async swarmBase (base) {
    const swarm = new Hyperswarm({ keypair: base.local.keyPair })
    swarm.on('connection', (connection) => base.replicate(connection))
    swarm.join(base.discoveryKey)
    return swarm
  }

  async exists (_key) {
    const core = this.store.get({ key: _key })
    return !!core
  }

  async unlink (_key) {
    const core = this.store.get({ key: _key })
    await core.clear(0, core.length)
    await core.truncate()
    await core.close()
  }

  _start (cb) {
    async.series([
      next => { super._start(next) },
      async () => {
        if (!this.opts.storeDir) {
          throw new Error('ERR_FACS_STORE_STORAGE_PATH_INVALID')
        }

        this.store = new Corestore(this.opts.storeDir, {
          primaryKey: this.opts.storePrimaryKey
            ? Buffer.from(this.opts.storePrimaryKey, 'hex')
            : null
        })
      }
    ], cb)
  }

  _stop (cb) {
    async.series([
      next => { super._stop(next) },
      async () => {
        await this.store.close()
        delete this.store
      }
    ], cb)
  }
}

module.exports = StoreFacility
