'use strict'

function convIntToBin (val) {
  const buf = Buffer.allocUnsafe(6)
  buf.writeUIntBE(val, 0, 6)

  return buf
}

const convBigIntToBin = (val) => {
  let byteLength = 0
  let temp = val
  while (temp > 0n) {
    byteLength++
    temp >>= 8n
  }

  const buf = Buffer.alloc(byteLength)

  temp = val
  for (let i = byteLength - 1; i >= 0; i--) {
    buf[i] = Number(temp & 0xFFn)
    temp >>= 8n
  }

  return buf
}

const convBinToBigInt = (val) => {
  let result = 0n

  for (const byte of val) {
    result = (result << 8n) | BigInt(byte)
  }

  return result
}

/**
 * @param {string|number|bigint} val
 * @param {'string'|'number'|'bigint'} type
 * @returns {Buffer}
 */
const convToBin = (val, type) => {
  switch (type) {
    case 'string': return Buffer.from(val, 'utf-8')
    case 'number': return convIntToBin(val)
    case 'bigint': return convBigIntToBin(val)
    default: throw new Error('ERR_TYPE_NOT_SUPPORTED: ' + type)
  }
}

/**
 * @param {Buffer} val
 * @param {'string'|'number'|'bigint'} type
 * @returns {string|number|bigint}
 */
const convFromBin = (val, type) => {
  switch (type) {
    case 'string': return val.toString('utf-8')
    case 'number': return val.readUIntBE(0, 6)
    case 'bigint': return convBinToBigInt(val)
    default: throw new Error('ERR_TYPE_NOT_SUPPORTED: ' + type)
  }
}

module.exports = {
  convIntToBin,
  convBigIntToBin,
  convBinToBigInt,
  convToBin,
  convFromBin
}
