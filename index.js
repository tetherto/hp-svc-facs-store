'use strict'

const async = require('async')
const _ = require('lodash')
const Corestore = require('corestore')
const Hypercore = require('hypercore')
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

  _start (cb) {
    async.series([
      next => { super._start(next) },
      async () => {
        if (!this.opts.storeDir) {
          throw new Error('ERR_FACS_STORE_STORAGE_PATH_INVALID')
        }

        this.store = new Corestore(this.opts.storeDir)
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
