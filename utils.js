function convInt2bin (val) {  
  const kstart = Buffer.allocUnsafe(6)
  kstart.writeUIntBE(val, 0, 6)

  return val
}

module.exports = {
  convInt2bin
}