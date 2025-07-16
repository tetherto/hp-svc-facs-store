'use strict'

class LimitedMap {
  constructor (maxSize) {
    this.maxSize = maxSize
    this.map = new Map()
    this.keys = []
  }

  has (key) {
    return this.map.has(key)
  }

  set (key, val) {
    if (this.has(key)) {
      return
    }

    if (this.keys.length === this.maxSize) {
      this.del(this.keys[0])
    }

    this.map.set(key, val)
    this.keys.push(key)
  }

  get (key) {
    return this.map.get(key)
  }

  del (key) {
    const success = this.map.delete(key)
    if (!success) {
      return
    }
    this.keys.splice(this.keys.findIndex(k => k === key), 1)
  }

  size () {
    return this.map.size
  }

  clear () {
    this.map.clear()
    this.keys = []
  }
}

module.exports = LimitedMap
