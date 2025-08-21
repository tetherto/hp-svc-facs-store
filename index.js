'use strict'

const async = require('async')
const Autobase = require('autobase')
const Base = require('bfx-facs-base')
const Corestore = require('corestore')
const Hyperbee = require('hyperbee')

class StoreFacility extends Base {
  constructor (caller, opts, ctx) {
    super(caller, opts, ctx)

    this.name = 'store'

    this.init()
  }

  /**
   * @param {object} opts
   * @returns {import('hypercore')}
   */
  getCore (opts = {}) {
    return this.store.get(opts)
  }

  /**
   * @param {object} opts
   * @param {object} beeOpts
   * @returns {Hyperbee}
   */
  getBee (opts = {}, beeOpts = {}) {
    const hc = this.store.get(opts)

    return new Hyperbee(hc, beeOpts)
  }

  /**
   * @param {object} baseOpts
   * @param {string|Buffer|Uint8Array} [boostrapKey]
   * @returns {Autobase}
   */
  getBase (baseOpts, boostrapKey = null) {
    return new Autobase(this.store.session(), boostrapKey, baseOpts)
  }

  /**
   * @param {Hyperbee} bee
   * @param {string} clearKey - key to store checkpoint on user data
   */
  async clearBeeCache (bee, clearKey) {
    const [lastCleared, nextClearing] = JSON.parse(await bee.core.getUserData(clearKey) || '[0,0]')
    await bee.clearUnlinked({ gte: lastCleared, lt: nextClearing })
    await bee.core.setUserData(clearKey, JSON.stringify([nextClearing, bee.version - 1]))
  }

  /**
   * @param {string|Buffer|Uint8Array} _key
   */
  async unlink (_key) {
    const core = this.store.get({ key: _key })
    await core.ready()
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
          ...(this.opts.storeOpts ?? {}),
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
