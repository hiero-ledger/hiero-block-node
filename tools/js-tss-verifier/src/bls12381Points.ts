/**
 * BLS12-381 point I/O for ArkWorks-serialized data.
 *
 * Uncompressed wire format (jni_util.rs serialize_uncompressed):
 *   G1: 96 bytes = x (48 bytes BE) + y (48 bytes BE)
 *   G2: 192 bytes = x.im (48 BE) + x.re (48 BE) + y.im (48 BE) + y.re (48 BE)
 *   Fr: 32 bytes LE
 *
 * Compressed format for Fiat-Shamir transcript (ZCash/ArkWorks BLS12-381):
 *   G1: 48 bytes BE x, flags in byte[0]: 0x80 always, 0x20 if y > (p-1)/2
 *   G2: 96 bytes (imaginary 48 BE + real 48 BE), same flags in byte[0]
 *   Fr: 32 bytes LE (unchanged)
 */

import { bls12_381 } from "@noble/curves/bls12-381.js";

const { Fp, Fp2 } = bls12_381.fields;
const G1 = bls12_381.G1.Point;
const G2 = bls12_381.G2.Point;

export type Bls12G1Point = InstanceType<typeof G1>;
export type Bls12G2Point = InstanceType<typeof G2>;

const BLS_FP_HALF = (Fp.ORDER - 1n) / 2n;

import { readU256LE } from "./byteReaders.js";

function readFpBE(buf: Buffer, offset: number): bigint {
  let v = 0n;
  for (let i = 0; i < 48; i++) {
    v = (v << 8n) | BigInt(buf[offset + i]);
  }
  return v;
}

/** Read a BLS12-381 Fr element (32 bytes LE). */
export const readBlsFr = readU256LE;

/** Read an uncompressed BLS12-381 G1 point (96 bytes: x BE + y BE). */
export function readG1Uncompressed(buf: Buffer, offset: number): Bls12G1Point {
  const x = readFpBE(buf, offset);
  const y = readFpBE(buf, offset + 48);
  if (x === 0n && y === 0n) return G1.ZERO;
  return G1.fromAffine({ x, y });
}

/**
 * Read an uncompressed BLS12-381 G2 point (192 bytes).
 * ArkWorks serializes imaginary (c0) first, then real (c1) — opposite of noble's convention.
 */
export function readG2Uncompressed(buf: Buffer, offset: number): Bls12G2Point {
  const xIm = readFpBE(buf, offset);        // imaginary (noble c1)
  const xRe = readFpBE(buf, offset + 48);   // real (noble c0)
  const yIm = readFpBE(buf, offset + 96);
  const yRe = readFpBE(buf, offset + 144);
  if (xRe === 0n && xIm === 0n && yRe === 0n && yIm === 0n) return G2.ZERO;
  return G2.fromAffine({
    x: Fp2.create({ c0: xRe, c1: xIm }),
    y: Fp2.create({ c0: yRe, c1: yIm }),
  });
}


function fpToBE48(v: bigint): Uint8Array {
  const buf = new Uint8Array(48);
  let val = v;
  for (let i = 47; i >= 0; i--) {
    buf[i] = Number(val & 0xffn);
    val >>= 8n;
  }
  return buf;
}

function frToLE32(v: bigint): Uint8Array {
  const buf = new Uint8Array(32);
  let val = v;
  for (let i = 0; i < 32; i++) {
    buf[i] = Number(val & 0xffn);
    val >>= 8n;
  }
  return buf;
}

/**
 * Compress a BLS12-381 G1 point to 48 bytes (ZCash/hedera-cryptography format).
 *
 * Format (confirmed from hedera-cryptography forPiotr transcript bytes):
 *   - x-coordinate: 48 bytes big-endian
 *   - Flags in top bits of byte[0] (MSB in BE):
 *     bit 7 (0x80): always set (indicates compressed point)
 *     bit 6 (0x40): PointAtInfinity (x bytes all zero)
 *     bit 5 (0x20): YIsNegative — set if y > (p-1)/2
 */
export function compressG1(point: Bls12G1Point): Uint8Array {
  if (point.equals(G1.ZERO)) {
    const buf = new Uint8Array(48);
    buf[0] = 0xc0; // compressed (0x80) + infinity (0x40)
    return buf;
  }
  const aff = point.toAffine();
  const buf = fpToBE48(aff.x);
  buf[0] |= 0x80; // compressed flag always set
  if (aff.y > BLS_FP_HALF) {
    buf[0] |= 0x20; // sign flag
  }
  return buf;
}

/**
 * Compress a BLS12-381 G2 point to 96 bytes (ZCash/hedera-cryptography format).
 *
 * Format (confirmed from hedera-cryptography forPiotr transcript bytes):
 *   - bytes[0..48]: imaginary (noble c1), BE
 *   - bytes[48..96]: real (noble c0), BE
 *   - Flags in top bits of byte[0] (MSB of imaginary component):
 *     bit 7 (0x80): always set (compressed)
 *     bit 6 (0x40): PointAtInfinity
 *     bit 5 (0x20): y-sign — set based on imaginary y component
 *
 * Y-sign for G2: set if imaginary(y) > (p-1)/2.
 * If imaginary(y) == 0, use real(y) > (p-1)/2.
 */
export function compressG2(point: Bls12G2Point): Uint8Array {
  if (point.equals(G2.ZERO)) {
    const buf = new Uint8Array(96);
    buf[0] = 0xc0; // compressed + infinity
    return buf;
  }
  const aff = point.toAffine();
  const { c0: xRe, c1: xIm } = aff.x as { c0: bigint; c1: bigint };
  const { c0: yRe, c1: yIm } = aff.y as { c0: bigint; c1: bigint };

  const buf = new Uint8Array(96);
  buf.set(fpToBE48(xIm), 0);   // imaginary first, BE
  buf.set(fpToBE48(xRe), 48);  // real second, BE
  buf[0] |= 0x80;

  const ySign = yIm > BLS_FP_HALF || (yIm === 0n && yRe > BLS_FP_HALF);
  if (ySign) {
    buf[0] |= 0x20;
  }
  return buf;
}

/** Serialize a BLS12-381 Fr element to 32 bytes LE (same in compressed/uncompressed). */
export function compressFr(v: bigint): Uint8Array {
  return frToLE32(v);
}
