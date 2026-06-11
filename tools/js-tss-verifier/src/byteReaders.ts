/** Shared low-level byte-reading functions and constants for ArkWorks-serialized BN254 data. */

export const BN254_FR_ORDER = 0x30644e72e131a029b85045b68181585d2833e84879b9709143e1f593f0000001n;

/** Read a 32-byte little-endian unsigned integer from a buffer at the given offset. */
export function readU256LE(buf: Buffer, offset: number): bigint {
  let value = 0n;
  for (let i = 31; i >= 0; i--) {
    value = (value << 8n) | BigInt(buf[offset + i]);
  }
  return value;
}

/** Read an 8-byte little-endian u64 from a buffer. */
export function readU64LE(buf: Buffer, offset: number): number {
  let value = 0n;
  for (let i = 7; i >= 0; i--) {
    value = (value << 8n) | BigInt(buf[offset + i]);
  }
  return Number(value);
}

/** Read a BN254 Fr element (32 bytes LE). */
export function readFr(buf: Buffer, offset: number): bigint {
  return readU256LE(buf, offset);
}
