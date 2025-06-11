'use strict'

const async = require('async')
const Autobase = require('autobase')
const Corestore = require('corestore')
const Hyperbee = require('hyperbee')
const Base = require('bfx-facs-base')

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
    return new Autobase(this.store, boostrapKey, baseOpts)
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
