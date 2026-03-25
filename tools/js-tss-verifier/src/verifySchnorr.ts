/**
 * Schnorr aggregate signature verification for genesis/pre-settled blocks.
 *
 * The 192-byte Schnorr suffix from the tssSignature is:
 *   [0..128):   bitvector — 1 byte per bool, 128 entries (MAX_AB_SIZE)
 *   [128..160): prover_response — JubJub Fr scalar (32 bytes LE)
 *   [160..192): verifier_challenge — JubJub Fr scalar (32 bytes LE)
 *
 * Each 192-byte node historyProofKey in the address book is:
 *   [0..32):    public key x (JubJub Fq = BN254 Fr, 32 bytes LE)
 *   [32..64):   public key y (32 bytes LE)
 *   [64..96):   PoK commitment x (32 bytes LE)
 *   [96..128):  PoK commitment y (32 bytes LE)
 *   [128..160): PoK challenge (JubJub Fr, 32 bytes LE)
 *   [160..192): PoK response (JubJub Fr, 32 bytes LE)
 *
 * Schnorr verification (from hedera-cryptography Schnorr::verify):
 *   1. claimed_commitment = prover_response * G + verifier_challenge * aggPK
 *   2. hash_input = serialize(aggPK) || serialize(claimed_commitment) || message
 *   3. e' = Blake2s(hash_input), then from_le_bytes_mod_order(e'[0..31])
 *   4. Verify: e' == verifier_challenge
 *
 * The rotation message (from TSS.java) is:
 *   ledgerId (32 bytes) || Poseidon(hinTS_VK) (32 bytes)
 *
 * serialize() for JubJub affine points uses ArkWorks serialize_uncompressed:
 *   x (32 bytes LE) || y (32 bytes LE) = 64 bytes
 */

import { blake2s } from "@noble/hashes/blake2.js";
import { edwards } from "@noble/curves/abstract/edwards.js";
import { Field } from "@noble/curves/abstract/modular.js";
import { poseidonSponge, grainGenConstants } from "@noble/curves/abstract/poseidon.js";
import type {
  BootstrapPublicationSummary,
  ParsedSchnorrSuffix,
  ParsedNodeKey,
  SchnorrVerificationResult,
  ProofLayout,
} from "./types.js";

// ─── BabyJubjub (ark_ed_on_bn254) curve parameters ─────────────────────────
// Twisted Edwards curve: a*x² + y² = 1 + d*x²*y²
// BabyJubjub over BN254's scalar field.
// Parameters match Circom/iden3 BabyJubjub AND ArkWorks ark_ed_on_bn254.
// Note: ArkWorks uses the same curve constants but a different generator
// that lies in the prime-order subgroup.

const BN254_FR_ORDER = 0x30644e72e131a029b85045b68181585d2833e84879b9709143e1f593f0000001n;

// ArkWorks ark_ed_on_bn254 uses the rescaled twisted Edwards form:
//   a*x² + y² = 1 + d*x²*y²  with a=1, d=168696/168700 mod p
// https://github.com/arkworks-rs/algebra/blob/master/curves/ed_on_bn254/src/curves/mod.rs
const BABYJUBJUB_L = 2736030358979909402780800718157159386076813972158567259200215660948447373041n;
const BABYJUBJUB_D = 9706598848417545097372247223557719406784115219466060233080913168975159366771n;
const ARK_GX = 19698561148652590122159747500897617769866003486955115824547446575314762165298n;
const ARK_GY = 19298250018296453272277890825869354524455968081175474282777126169995084727839n;

const JubJubFp = Field(BN254_FR_ORDER);
const JubJubFn = Field(BABYJUBJUB_L);

const JubJub = edwards(
  {
    p: BN254_FR_ORDER,
    n: BABYJUBJUB_L,
    h: 8n,
    a: 1n,
    d: BABYJUBJUB_D,
    Gx: ARK_GX,
    Gy: ARK_GY,
  },
  { Fp: JubJubFp, Fn: JubJubFn },
);

// ─── Poseidon hash over BN254 Fr ────────────────────────────────────────────
// Config: t=5, fullRounds=8, partialRounds=60, alpha=5, rate=4, capacity=1
// This matches poseidon_canonical_config::<Fr>() from the Rust code.
// Verified against Circom's Poseidon(4).

const poseidonFp = Field(BN254_FR_ORDER);
const poseidonConstants = grainGenConstants({
  Fp: poseidonFp,
  t: 5,
  roundsFull: 8,
  roundsPartial: 60,
  sboxPower: 5,
});

const createPoseidonSponge = poseidonSponge({
  ...poseidonConstants,
  Fp: poseidonFp,
  rate: 4,
  capacity: 1,
  roundsFull: 8,
  roundsPartial: 60,
  sboxPower: 5,
});

/**
 * Computes Poseidon hash of an array of BN254 Fr elements.
 * Matches PoseidonCRH::evaluate from ArkWorks (absorb all, squeeze one).
 */
function poseidonHash(inputs: bigint[]): bigint {
  const sponge = createPoseidonSponge();
  for (const elem of inputs) {
    sponge.absorb([elem]);
  }
  return sponge.squeeze(1)[0];
}

/**
 * Hashes the hinTS verification key bytes into a BN254 Fr element.
 * Matches hash_hints_vk() from the Rust WRAPS code:
 *   - Chop the VK bytes into 32-byte chunks
 *   - Convert each chunk to Fr via from_le_bytes_mod_order
 *   - Poseidon hash the resulting Fr elements
 */
function hashHintsVk(vkBytes: Buffer): bigint {
  const elements: bigint[] = [];
  for (let i = 0; i < vkBytes.length; i += 32) {
    const end = Math.min(i + 32, vkBytes.length);
    const chunk = vkBytes.subarray(i, end);
    let val = 0n;
    for (let j = chunk.length - 1; j >= 0; j--) {
      val = (val << 8n) | BigInt(chunk[j]);
    }
    // mod order to match from_le_bytes_mod_order
    elements.push(val % BN254_FR_ORDER);
  }
  return poseidonHash(elements);
}

// ─── Parsing ────────────────────────────────────────────────────────────────

function readU256LE(buf: Buffer, offset: number): bigint {
  let value = 0n;
  for (let i = 31; i >= 0; i--) {
    value = (value << 8n) | BigInt(buf[offset + i]);
  }
  return value;
}

export function parseSchnorrSuffix(suffixBytes: Buffer): ParsedSchnorrSuffix {
  if (suffixBytes.length !== 192) {
    throw new Error(`Expected 192-byte Schnorr suffix, got ${suffixBytes.length}`);
  }

  const bitvector: boolean[] = [];
  for (let i = 0; i < 128; i++) {
    bitvector.push(suffixBytes[i] !== 0);
  }

  const proverResponse = readU256LE(suffixBytes, 128);
  const verifierChallenge = readU256LE(suffixBytes, 160);

  return { bitvector, proverResponse, verifierChallenge };
}

export function parseNodeKey(keyHex: string): ParsedNodeKey {
  const buf = Buffer.from(keyHex, "hex");
  if (buf.length !== 192) {
    throw new Error(`Expected 192-byte node key, got ${buf.length}`);
  }
  return {
    pubkeyX: readU256LE(buf, 0),
    pubkeyY: readU256LE(buf, 32),
    pokCommitmentX: readU256LE(buf, 64),
    pokCommitmentY: readU256LE(buf, 96),
    pokChallenge: readU256LE(buf, 128),
    pokResponse: readU256LE(buf, 160),
  };
}

/**
 * Serialize a JubJub affine point to 64 bytes (uncompressed) matching ArkWorks.
 * Format: x (32 bytes LE) || y (32 bytes LE)
 */
function serializePointUncompressed(x: bigint, y: bigint): Uint8Array {
  const buf = new Uint8Array(64);
  let xv = x;
  let yv = y;
  for (let i = 0; i < 32; i++) {
    buf[i] = Number(xv & 0xffn);
    xv >>= 8n;
    buf[32 + i] = Number(yv & 0xffn);
    yv >>= 8n;
  }
  return buf;
}

/**
 * Convert a bigint to 32 bytes LE.
 */
function bigintToBytes32LE(val: bigint): Uint8Array {
  const buf = new Uint8Array(32);
  let v = val;
  for (let i = 0; i < 32; i++) {
    buf[i] = Number(v & 0xffn);
    v >>= 8n;
  }
  return buf;
}

/**
 * Build the rotation message: ledgerId (32B) || Poseidon(hinTS_VK) (32B)
 * ledgerId comes directly from the bootstrap publication.
 * hinTS_VK hash is computed via Poseidon over BN254 Fr.
 */
function buildRotationMessage(
  ledgerIdHex: string,
  hintsVkHex: string,
): Uint8Array {
  const ledgerIdBytes = Buffer.from(ledgerIdHex, "hex");
  const hintsVkBytes = Buffer.from(hintsVkHex, "hex");
  const hintsVkHash = hashHintsVk(hintsVkBytes);

  const msg = new Uint8Array(64);
  msg.set(ledgerIdBytes.subarray(0, 32), 0);
  msg.set(bigintToBytes32LE(hintsVkHash), 32);
  return msg;
}

export function verifySchnorrSignature(
  proofLayout: ProofLayout,
  bootstrap: BootstrapPublicationSummary | null,
): SchnorrVerificationResult {
  if (proofLayout.suffixKind !== "aggregate-schnorr") {
    return { attempted: false, status: "skipped", reason: "Not a Schnorr-path proof." };
  }

  if (!bootstrap) {
    return {
      attempted: false,
      status: "skipped",
      reason: "No bootstrap publication available to extract address book and ledger ID.",
    };
  }

  if (bootstrap.nodeContributions.length === 0) {
    return {
      attempted: false,
      status: "skipped",
      reason: "Bootstrap has no node contributions.",
    };
  }

  try {
    // 1. Parse the 192-byte Schnorr suffix
    const sig = parseSchnorrSuffix(proofLayout.suffixBytes);
    const totalNodes = bootstrap.nodeContributions.length;
    const signerCount = sig.bitvector.filter((b, i) => i < totalNodes && b).length;

    // 2. Parse node public keys and compute aggregate PK
    let aggPK = JubJub.ZERO;
    for (let i = 0; i < totalNodes; i++) {
      if (!sig.bitvector[i]) continue;
      const nodeKey = parseNodeKey(bootstrap.nodeContributions[i].historyProofKeyHex);
      const point = JubJub.fromAffine({ x: nodeKey.pubkeyX, y: nodeKey.pubkeyY });
      aggPK = aggPK.add(point);
    }

    // 3. Build the rotation message: ledgerId || Poseidon(hinTS_VK)
    const hintsVkHex =
      proofLayout.hintsVerificationKeyBytes.length > 0
        ? proofLayout.hintsVerificationKeyBytes.toString("hex")
        : bootstrap.historyProofVerificationKeyHex;
    const rotationMessage = buildRotationMessage(bootstrap.ledgerIdHex, hintsVkHex);

    // 4. Schnorr verification:
    //    claimed_commitment = prover_response * G + verifier_challenge * aggPK
    const G = JubJub.BASE;
    const claimedCommitment = G.multiply(sig.proverResponse).add(
      aggPK.multiply(sig.verifierChallenge),
    );

    // 5. Recompute challenge:
    //    hash_input = serialize(aggPK) || serialize(claimed_commitment) || message
    //    (Parameters has salt: None, so no salt prefix)
    const aggAff = aggPK.toAffine();
    const commitAff = claimedCommitment.toAffine();

    const hashInput = new Uint8Array(64 + 64 + rotationMessage.length);
    hashInput.set(serializePointUncompressed(aggAff.x, aggAff.y), 0);
    hashInput.set(serializePointUncompressed(commitAff.x, commitAff.y), 64);
    hashInput.set(rotationMessage, 128);

    // Blake2s hash, then take first 31 bytes as LE scalar (from_le_bytes_mod_order)
    const digest = blake2s(hashInput);
    let recomputedChallenge = 0n;
    for (let i = 30; i >= 0; i--) {
      recomputedChallenge = (recomputedChallenge << 8n) | BigInt(digest[i]);
    }
    recomputedChallenge = recomputedChallenge % BABYJUBJUB_L;

    // 6. Check
    const challengeMatch = recomputedChallenge === sig.verifierChallenge;

    if (challengeMatch) {
      return {
        attempted: true,
        status: "verified",
        reason: `Schnorr aggregate signature verified successfully (${signerCount}/${totalNodes} signers).`,
        signerCount,
        totalNodes,
      };
    } else {
      return {
        attempted: true,
        status: "failed",
        reason:
          `Challenge mismatch: recomputed=0x${recomputedChallenge.toString(16).slice(0, 16)}... ` +
          `vs expected=0x${sig.verifierChallenge.toString(16).slice(0, 16)}... ` +
          `(${signerCount}/${totalNodes} signers)`,
        signerCount,
        totalNodes,
      };
    }
  } catch (err) {
    return {
      attempted: true,
      status: "error",
      reason: err instanceof Error ? err.message : String(err),
    };
  }
}
