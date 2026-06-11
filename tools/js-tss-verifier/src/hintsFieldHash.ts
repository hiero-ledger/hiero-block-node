/**
 * Fiat-Shamir challenge computation and field utilities for hinTS verification.
 *
 * The Fiat-Shamir challenge is computed by:
 * 1. Serializing 14 inputs in ArkWorks compressed format
 * 2. Hashing with DefaultFieldHasher<Sha256, 128> using DST "HINTS_SIG_BLS12381:FIAT_SHAMIR"
 * 3. Output: single BLS12-381 Fr element
 *
 * Also provides nth_root_of_unity for BLS12-381 Fr.
 */

import { sha256 } from "@noble/hashes/sha2.js";
import { bls12_381 } from "@noble/curves/bls12-381.js";
import {
  compressG1,
  compressG2,
  compressFr,
} from "./bls12381Points.js";
import type { HintsVerificationKey, HintsThresholdSignature } from "./deserializeHintsData.js";

const Fr = bls12_381.fields.Fr;

// BLS12-381 Fr order
const FR_ORDER = Fr.ORDER;

// L = ceil((255 + 128) / 8) = 48 (BLS12-381 Fr bits + security param 128)
const L = 48;
const SHA256_OUT_LEN = 32;

const FIAT_SHAMIR_DST = "HINTS_SIG_BLS12381:FIAT_SHAMIR";

/**
 * ArkWorks-compatible expand_message_xmd.
 *
 * Follows RFC 9380 §5.4.1 EXCEPT: ArkWorks 0.4.2 uses z_pad of L=48 bytes
 * instead of the standard s_in_bytes=64 (SHA-256 block size).
 * See ark-ff-0.4.2 src/fields/field_hashers/expander/mod.rs.
 *
 * len_in_bytes = L = 48. SHA-256 output = 32 bytes. ell = ceil(48/32) = 2.
 */
function arkworksExpandMessageXmd(
  msg: Uint8Array,
  dst: Uint8Array,
  lenInBytes: number,
): Uint8Array {
  const bLen = SHA256_OUT_LEN; // 32
  const ell = Math.ceil(lenInBytes / bLen); // ceil(48/32) = 2

  // dst_prime = dst || I2OSP(len(dst), 1)
  const dstPrime = new Uint8Array(dst.length + 1);
  dstPrime.set(dst);
  dstPrime[dst.length] = dst.length;

  // Z_pad = 48 zero bytes (ArkWorks uses block_size = L, NOT SHA-256 s_in_bytes)
  const zPad = new Uint8Array(L);

  // l_i_b_str = I2OSP(lenInBytes, 2)
  const lib = new Uint8Array(2);
  lib[0] = (lenInBytes >> 8) & 0xff;
  lib[1] = lenInBytes & 0xff;

  // b_0 = H(Z_pad || msg || l_i_b_str || 0x00 || dst_prime)
  const b0Input = new Uint8Array(zPad.length + msg.length + 2 + 1 + dstPrime.length);
  let pos = 0;
  b0Input.set(zPad, pos); pos += zPad.length;
  b0Input.set(msg, pos); pos += msg.length;
  b0Input.set(lib, pos); pos += 2;
  b0Input[pos++] = 0x00;
  b0Input.set(dstPrime, pos);
  const b0 = sha256(b0Input);

  // b_1 = H(b_0 || I2OSP(1, 1) || dst_prime)
  const b1Input = new Uint8Array(b0.length + 1 + dstPrime.length);
  b1Input.set(b0); b1Input[b0.length] = 0x01; b1Input.set(dstPrime, b0.length + 1);
  const b1 = sha256(b1Input);

  const blocks = [b1];
  let prev = b1;
  for (let i = 2; i <= ell; i++) {
    // b_i = H((b_0 XOR b_{i-1}) || I2OSP(i, 1) || dst_prime)
    const xored = new Uint8Array(b0.length);
    for (let j = 0; j < b0.length; j++) xored[j] = b0[j] ^ prev[j];
    const biInput = new Uint8Array(xored.length + 1 + dstPrime.length);
    biInput.set(xored); biInput[xored.length] = i; biInput.set(dstPrime, xored.length + 1);
    const bi = sha256(biInput);
    blocks.push(bi);
    prev = bi;
  }

  // uniform_bytes = b_1 || b_2 || ... [first lenInBytes]
  const uniformBytes = new Uint8Array(ell * bLen);
  for (let i = 0; i < ell; i++) uniformBytes.set(blocks[i], i * bLen);
  return uniformBytes.subarray(0, lenInBytes);
}

/**
 * Hash arbitrary bytes to a BLS12-381 Fr element.
 * Matches ArkWorks DefaultFieldHasher<Sha256, 128>::hash_to_field(&data, 1).
 *
 * Deviates from RFC 9380: uses z_pad of 48 bytes (= L) instead of 64.
 */
function hashToFr(data: Uint8Array, dst: string): bigint {
  const dstBytes = new TextEncoder().encode(dst);
  const expanded = arkworksExpandMessageXmd(data, dstBytes, L);

  // Interpret expanded bytes as big-endian integer and reduce mod Fr.ORDER
  let value = 0n;
  for (let i = 0; i < expanded.length; i++) {
    value = (value << 8n) | BigInt(expanded[i]);
  }
  return value % FR_ORDER;
}

/**
 * Compute the Fiat-Shamir challenge r from the verification key and signature.
 *
 * Matches hints.rs:875-912 random_oracle().
 * Serializes 14 inputs in ArkWorks compressed format, then hashes.
 */
export function computeFiatShamirChallenge(
  vk: HintsVerificationKey,
  sig: HintsThresholdSignature,
): bigint {
  // Build transcript: 14 compressed-serialized elements in exact order from random_oracle()
  const parts: Uint8Array[] = [
    compressG2(vk.skOfTauCom),        // SK commitment (G2, 96 bytes)
    compressG2(vk.h1),                 // τ commitment (G2, 96 bytes)
    compressG1(sig.aggPk),             // aggregate PK (G1, 48 bytes)
    compressFr(sig.aggWeight),         // aggregate weight (Fr, 32 bytes)
    compressG1(vk.wOfTauCom),          // W commitment (G1, 48 bytes)
    compressG1(sig.bOfTauCom),         // B commitment (G1, 48 bytes)
    compressG1(sig.parsumOfTauCom),    // ParSum commitment (G1, 48 bytes)
    compressG1(sig.qxOfTauCom),        // Qx commitment (G1, 48 bytes)
    compressG1(sig.qzOfTauCom),        // Qz commitment (G1, 48 bytes)
    compressG1(sig.qxOfTauMulTauCom),  // Qx·τ commitment (G1, 48 bytes)
    compressG1(sig.q1OfTauCom),        // Q1 commitment (G1, 48 bytes)
    compressG1(sig.q2OfTauCom),        // Q2 commitment (G1, 48 bytes)
    compressG1(sig.q3OfTauCom),        // Q3 commitment (G1, 48 bytes)
    compressG1(sig.q4OfTauCom),        // Q4 commitment (G1, 48 bytes)
  ];

  // Concatenate all parts
  const totalLen = parts.reduce((sum, p) => sum + p.length, 0);
  const transcript = new Uint8Array(totalLen);
  let offset = 0;
  for (const part of parts) {
    transcript.set(part, offset);
    offset += part.length;
  }

  return hashToFr(transcript, FIAT_SHAMIR_DST);
}

/**
 * Compute the nth root of unity in BLS12-381 Fr.
 * This is the generator ω of the multiplicative subgroup of size n,
 * where n must be a power of 2 and divide (Fr.ORDER - 1).
 *
 * ω = g^((r-1)/n) where g is a generator of the full multiplicative group.
 *
 * BLS12-381 Fr has 2-adicity 32, so the largest power-of-2 subgroup is 2^32.
 *
 * Matches ArkWorks Radix2EvaluationDomain::new(n).group_gen.
 */
export function nthRootOfUnity(n: number): bigint {
  if (n < 2 || (n & (n - 1)) !== 0) {
    throw new Error(`n must be a power of 2 >= 2, got ${n}`);
  }

  // BLS12-381 Fr: r - 1 = 2^32 × s (where s is odd)
  // The primitive 2^32-th root of unity is g^(s) where g is a generator.
  // ArkWorks uses TWO_ADIC_ROOT_OF_UNITY for the 2^32-th root.
  // For BLS12-381 Fr, confirmed from ark_bls12_381::FrConfig::TWO_ADIC_ROOT_OF_UNITY:
  //   LE bytes: [43,13,159,67,31,151,41,56,185,128,34,140,80,131,54,182,
  //              180,19,200,34,25,104,155,208,32,31,232,223,158,161,162,22]
  const TWO_ADIC_ROOT = 0x16a2a19edfe81f20d09b681922c813b4b63683508c2280b93829971f439f0d2bn;
  // (confirmed via hedera-cryptography forPiotr Rust test output)

  // The 2-adicity is 32
  const TWO_ADICITY = 32;

  // n = 2^k, find k
  let k = 0;
  let tmp = n;
  while (tmp > 1) { tmp >>= 1; k++; }

  if (k > TWO_ADICITY) {
    throw new Error(`n=${n} exceeds 2-adicity ${TWO_ADICITY} of BLS12-381 Fr`);
  }

  // ω_n = TWO_ADIC_ROOT ^ (2^(TWO_ADICITY - k))
  // This gives us the primitive n-th root of unity
  const exponent = 1n << BigInt(TWO_ADICITY - k);
  return Fr.pow(TWO_ADIC_ROOT, exponent);
}
