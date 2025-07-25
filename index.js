'use strict'

const async = require('async')
const Autobase = require('autobase')
const Base = require('bfx-facs-base')
const Corestore = require('corestore')
const Hyperbee = require('hyperbee')
const Hyperswarm = require('hyperswarm')

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

  async clearBeeCache (bee, prefix) {
    const prev = Number((await bee.core.getUserData(`${prefix}-cleared`) || '0'))
    const checkout = Number((await bee.core.getUserData(`${prefix}-checkout`) || '0'))

    const co = bee.checkout(checkout)

    for await (const entry of bee.createHistoryStream({ gt: prev, lt: bee.core.length - 1 })) {
      const key = entry.key
      const latestNode = await bee.get(key)
      const checkoutNode = await co.get(key, { wait: false }).catch(() => null)

      if (checkoutNode && (!latestNode || checkoutNode.seq !== latestNode.seq)) {
        await bee.core.clear(checkoutNode.seq)
      }

      if (!latestNode || latestNode.seq !== entry.seq) {
        await bee.core.setUserData(`${prefix}-cleared`, '' + (entry.seq + 1))
        await bee.core.clear(entry.seq)
      }
    }

    await bee.core.setUserData(`${prefix}-checkout`, '' + bee.core.length)

    await co.close()
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
