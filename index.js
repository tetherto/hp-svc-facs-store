'use strict'

const fs = require('fs/promises')
const path = require('path')
const async = require('async')
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

  _calcCorePath (_key) {
    const key = _key.toString('hex')
    const coreDir = path.join(this.opts.storeDir, 'cores', key.slice(0, 2), key.slice(2, 4), key)

    return coreDir
  }

  async exists (_key) {
    const coreDir = this._calcCorePath(_key)

    try {
      return await fs.stat(coreDir) ? true : false
    } catch (e) {
      return false
    }
  }

  async unlink (_key) {
    const coreDir = this._calcCorePath(_key)
    await fs.rm(coreDir, { recursive: true })
  }

  _start (cb) {
    async.series([
      next => { super._start(next) },
      async () => {
        if (!this.opts.storeDir) {
          throw new Error('ERR_FACS_STORE_STORAGE_PATH_INVALID')
        }

        this.store = new Corestore(this.opts.storeDir, {
          primaryKey: this.opts.storePrimaryKey ?
          Buffer.from(this.opts.storePrimaryKey, 'hex') : null
        })
      }
    ], cb)
  }

  _stop (cb) {
    async.series([
      next => { super._stop(next) },
      async () => {
        delete this.store
      }
    ], cb)
  }
}

module.exports = StoreFacility
