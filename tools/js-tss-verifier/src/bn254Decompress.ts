/**
 * ArkWorks compressed G1/G2 point decompression for BN254.
 *
 * ArkWorks CanonicalSerialize compressed format uses 2 flag bits in the
 * MSBs (bits 7-6) of the last byte of each compressed element:
 *   bit 7 (0x80) = y is the larger root (y > (p-1)/2)
 *   bit 6 (0x40) = point at infinity
 *
 * G1: 32 bytes LE x-coordinate (bits 0-253), flags in byte[31] bits 7-6.
 * G2: 64 bytes LE Fp2 x (c0: [0..32), c1: [32..64)), flags in byte[63] bits 7-6.
 *   Fp2 lexicographic ordering for y sign: compare c1 first, then c0.
 */

import { bn254 } from "@noble/curves/bn254.js";
import { readU256LE } from "./byteReaders.js";

const { Fp, Fp2 } = bn254.fields;
const G1 = bn254.G1.Point;
const G2 = bn254.G2.Point;

export type G1Point = InstanceType<typeof G1>;
export type G2Point = InstanceType<typeof G2>;

const FP_HALF = (Fp.ORDER - 1n) / 2n;

// G2 twist coefficient: B' = 3 / (9 + u) in Fp2
// Pre-computed from noble's bn254.ts source.
const Fp2B = Fp2.create({
  c0: 19485874751759354771024239261021720505790618469301721065564631296452457478373n,
  c1: 266929791119991161246907387137283842545076965332900288569378510910307636690n,
});

/**
 * Decompress an ArkWorks-format compressed G1 point (32 bytes at offset).
 * Returns a noble ProjectivePoint on BN254 G1.
 *
 * ArkWorks SWFlags (2 bits in bits 7-6 of last byte):
 *   bit 7 (0x80) = y is the larger root (y > (p-1)/2)
 *   bit 6 (0x40) = point at infinity
 */
export function decompressG1(buf: Buffer, offset: number): G1Point {
  const lastByte = buf[offset + 31];
  const isPositive = (lastByte & 0x80) !== 0; // bit 7
  const isInfinity = (lastByte & 0x40) !== 0; // bit 6

  // Read x with top 2 bits cleared (bits 254-255 are flags)
  const raw = readU256LE(buf, offset);
  const x = raw & ((1n << 254n) - 1n);

  // Point at infinity: explicit flag OR x == 0
  if (isInfinity || x === 0n) {
    return G1.ZERO;
  }

  // y^2 = x^3 + 3
  const x2 = Fp.sqr(x);
  const x3 = Fp.mul(x2, x);
  const ySquared = Fp.add(x3, 3n);

  let y: bigint;
  try {
    const result = Fp.sqrt(ySquared);
    if (result === undefined) {
      throw new Error("not a quadratic residue");
    }
    y = result;
  } catch (err) {
    throw new Error(`G1 decompression failed at offset ${offset}: isPositive=${isPositive} x=0x${x.toString(16).slice(0, 16)}... — ${err instanceof Error ? err.message : err}`);
  }

  // Flag set (bit 7) → select the larger root (y > (p-1)/2)
  const yIsLarger = y > FP_HALF;
  if (isPositive !== yIsLarger) {
    y = Fp.neg(y);
  }

  return G1.fromAffine({ x, y });
}

/**
 * Decompress an ArkWorks-format compressed G2 point (64 bytes at offset).
 * Returns a noble ProjectivePoint on BN254 G2.
 *
 * Same flag scheme as G1, in bits 7-6 of byte[63] (last byte of c1).
 */
export function decompressG2(buf: Buffer, offset: number): G2Point {
  const lastByte = buf[offset + 63];
  const isPositive = (lastByte & 0x80) !== 0; // bit 7
  const isInfinity = (lastByte & 0x40) !== 0; // bit 6

  // Read Fp2 x-coordinate: c0 at [0..32), c1 at [32..64)
  const c0 = readU256LE(buf, offset);
  const rawC1 = readU256LE(buf, offset + 32);
  const c1 = rawC1 & ((1n << 254n) - 1n); // clear top 2 flag bits

  if (isInfinity || (c0 === 0n && c1 === 0n)) {
    return G2.ZERO;
  }

  const xFp2 = Fp2.create({ c0, c1 });

  // y^2 = x^3 + B_twist
  const x2 = Fp2.sqr(xFp2);
  const x3 = Fp2.mul(x2, xFp2);
  const ySquared = Fp2.add(x3, Fp2B);

  let y: { c0: bigint; c1: bigint };
  try {
    const result = Fp2.sqrt(ySquared);
    if (result === undefined) {
      throw new Error("not a quadratic residue in Fp2");
    }
    y = result;
  } catch (err) {
    throw new Error(`G2 decompression failed at offset ${offset}: isPositive=${isPositive} c0=0x${c0.toString(16).slice(0, 16)}... c1=0x${c1.toString(16).slice(0, 16)}... — ${err instanceof Error ? err.message : err}`);
  }

  // ArkWorks Fp2 lexicographic ordering: compare c1 (im) first, then c0 (re).
  // "Larger" means c1 > (p-1)/2, or (c1 == 0 and c0 > (p-1)/2).
  const yC1 = y.c1;
  const yC0 = y.c0;
  const yIsLarger = yC1 > FP_HALF || (yC1 === 0n && yC0 > FP_HALF);
  if (isPositive !== yIsLarger) {
    y = Fp2.neg(y);
  }

  return G2.fromAffine({ x: xFp2, y });
}
