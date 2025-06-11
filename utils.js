function convIntToBin (val) {
  const buf = Buffer.allocUnsafe(6)
  buf.writeUIntBE(val, 0, 6)

  return buf
}

module.exports = {
  convIntToBin
}
