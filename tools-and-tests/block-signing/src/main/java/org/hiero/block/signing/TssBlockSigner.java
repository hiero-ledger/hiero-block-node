// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.signing;

import com.hedera.cryptography.hints.AggregationAndVerificationKeys;
import com.hedera.cryptography.hints.HintsLibraryBridge;
import com.hedera.cryptography.tss.TSS;
import com.hedera.cryptography.wraps.Proof;
import com.hedera.cryptography.wraps.SchnorrKeys;
import com.hedera.cryptography.wraps.WRAPSLibraryBridge;
import com.hedera.cryptography.wraps.WRAPSLibraryBridge.SigningProtocolPhase;
import com.hedera.cryptography.wraps.WRAPSVerificationKey;
import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.hapi.block.stream.TssSignedBlockProof;
import com.hedera.hapi.node.transaction.SignedTransaction;
import com.hedera.hapi.node.transaction.TransactionBody;
import com.hedera.hapi.node.tss.LedgerIdNodeContribution;
import com.hedera.hapi.node.tss.LedgerIdPublicationTransactionBody;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.List;
import org.hiero.block.api.RosterEntry;
import org.hiero.block.api.TssData;
import org.hiero.block.api.TssRoster;

/// Produces TSS/hinTS threshold [BlockProof]s that the block-verification `TSSVerifier` accepts, in
/// either of the two address-book-proof forms the verifier understands:
///
///   - **Genesis Schnorr-aggregate** (192-byte `abProof`) — the pre-settlement path a genesis
///     consensus node emits. Requires no WRAPS SNARK proving artifacts; built live in [#create] /
///     [#createDeterministic].
///   - **Settled WRAPS proof** (704-byte compressed `abProof`) — the post-settlement path. The proof
///     is a SNARK that takes ~13 minutes to construct and needs the ~2 GB proving artifacts, so it is
///     pre-computed once by [#generateGenesisWrapsProof] and committed as a resource; [#createDeterministicSettled]
///     loads it. Verifying it needs no artifacts.
///
/// The signer uses a single-node roster (hinTS universe `n = 2`, the smallest valid power of two,
/// since the maximum number of parties is `n - 1`). All per-era key material — the hinTS CRS, BLS
/// secret, aggregation/verification keys, and the ledger id — is generated once at construction.
/// [#signBlockProof] then only BLS-signs the block root hash and aggregates it, which is cheap; the
/// `abProof` (192 or 704) is fixed for the signer's lifetime.
///
/// The paired [#verificationMaterial] is a [TssData] whose roster, ledger id, and WRAPS verification
/// key match what the verifier needs.
public final class TssBlockSigner implements BlockSigner {

    /// hinTS universe size (power of two). Max parties = N - 1, so N = 2 supports the single node.
    private static final int N = 2;

    private static final long NODE_ID = 0L;
    private static final long WEIGHT = 1L;

    /// Fixed seed for the deterministic factories; any constant works since these are test-only keys.
    private static final byte[] DETERMINISTIC_SEED = "hiero-block-signing-test-seed".getBytes();
    private static final int[] PARTIES = {0};
    private static final long[] WEIGHTS = {WEIGHT};
    private static final long[] NODE_IDS = {NODE_ID};

    /// Length of a compressed WRAPS proof (the settled-path `abProof`).
    private static final int COMPRESSED_WRAPS_PROOF_LENGTH = 704;

    /// Classpath resource holding the pre-computed genesis WRAPS proof for the deterministic roster.
    private static final String WRAPS_PROOF_RESOURCE = "/org/hiero/block/signing/genesis-wraps-proof.bin";

    private static final HintsLibraryBridge HINTS = HintsLibraryBridge.getInstance();
    private static final WRAPSLibraryBridge WRAPS = WRAPSLibraryBridge.getInstance();

    private final byte[] crs;
    private final byte[] hintsVerificationKey;
    private final byte[] aggregationKey;
    private final byte[] blsSecretKey;
    private final byte[] addressBookProof;
    private final byte[] ledgerId;
    private final byte[] schnorrPublicKey;
    private final VerificationMaterial verificationMaterial;

    private TssBlockSigner(
            final byte[] crs,
            final byte[] hintsVerificationKey,
            final byte[] aggregationKey,
            final byte[] blsSecretKey,
            final byte[] addressBookProof,
            final byte[] ledgerId,
            final byte[] schnorrPublicKey) {
        this.crs = crs;
        this.hintsVerificationKey = hintsVerificationKey;
        this.aggregationKey = aggregationKey;
        this.blsSecretKey = blsSecretKey;
        this.addressBookProof = addressBookProof;
        this.ledgerId = ledgerId;
        this.schnorrPublicKey = schnorrPublicKey;
        this.verificationMaterial = VerificationMaterial.ofTss(TssData.newBuilder()
                .ledgerId(Bytes.wrap(ledgerId))
                .wrapsVerificationKey(Bytes.wrap(WRAPSVerificationKey.getDefaultKey()))
                .currentRoster(TssRoster.newBuilder()
                        .rosterEntries(List.of(RosterEntry.newBuilder()
                                .nodeId(NODE_ID)
                                .weight(WEIGHT)
                                .schnorrPublicKey(Bytes.wrap(schnorrPublicKey))
                                .build()))
                        .build())
                .validFromBlock(0L)
                .build());
    }

    /// Runs the full one-time key ceremony with fresh random key material and returns a ready-to-use
    /// signer.
    ///
    /// The hinTS native library keeps singleton state cached from the most recent ceremony, so the
    /// cache is reset first. As a result only one [TssBlockSigner] should be actively signing at a
    /// time in a given JVM; creating a second signer invalidates the first.
    @NonNull
    public static TssBlockSigner create() {
        return create(new SecureRandom());
    }

    /// Runs the ceremony with a fixed seed, producing the SAME roster and keys every time.
    ///
    /// Use this when several signer instances must agree on one roster within a single JVM — e.g.
    /// multiple simulator publishers streaming to one Block Node: they all sign with the same keys,
    /// the node self-provisions from any one of them, and duplicate blocks are byte-identical (and so
    /// detectable). Because every instance derives the same key material, the shared native cache is
    /// never invalidated between them.
    @NonNull
    public static TssBlockSigner createDeterministic() {
        return create(deterministicRandom());
    }

    /// Like [#createDeterministic] but with the SETTLED-path `abProof`: a pre-computed 704-byte
    /// compressed WRAPS proof loaded from a committed resource. Use this to exercise the post-genesis
    /// WRAPS verification path in tests without the slow SNARK proving or the ~2 GB artifacts at run
    /// time (verifying a WRAPS proof needs no artifacts).
    ///
    /// The committed proof is generated by [#generateGenesisWrapsProof] over the SAME deterministic
    /// roster this factory builds, so its ledger id and hinTS key match.
    @NonNull
    public static TssBlockSigner createDeterministicSettled() {
        final Era era = buildEra(deterministicRandom());
        return fromEra(era, loadCommittedWrapsProof());
    }

    private static TssBlockSigner create(final SecureRandom random) {
        final Era era = buildEra(random);
        // Genesis address-book proof: a 192-byte Schnorr aggregate over the rotation message
        // `ledgerId || hashArray(hintsVerificationKey)`, which is exactly what TSS.verifyTSS rebuilds.
        final byte[] rotationMessage =
                concat(era.ledgerId(), require("hashArray", WRAPS.hashArray(era.hintsVerificationKey())));
        final byte[] addressBookProof = aggregateSchnorr(
                era.schnorr(), new byte[][] {era.schnorr().publicKey()}, rotationMessage, random32(random));
        return fromEra(era, addressBookProof);
    }

    /// Constructs the 704-byte compressed genesis WRAPS proof for the deterministic roster.
    ///
    /// This is the slow, artifact-dependent step: it loads the ~2 GB proving key and runs a SNARK
    /// (~13 minutes). It is meant to be run offline, once, to (re)generate the committed resource —
    /// not at test run time. Requires `TSS_LIB_WRAPS_ARTIFACTS_PATH` to point at the WRAPS proving
    /// artifacts (`isProofSupported()`).
    @NonNull
    public static byte[] generateGenesisWrapsProof() {
        if (!WRAPSLibraryBridge.isProofSupported()) {
            throw new IllegalStateException(
                    "WRAPS proving artifacts unavailable; set TSS_LIB_WRAPS_ARTIFACTS_PATH to a directory containing"
                            + " decider_pp.bin/decider_vp.bin/nova_pp.bin/nova_vp.bin");
        }
        final SecureRandom random = deterministicRandom();
        final Era era = buildEra(random);
        final byte[][] schnorrPublicKeys = {era.schnorr().publicKey()};
        // The WRAPS proof's aggregate signature is over formatRotationMessage(roster, hintsVK).
        final byte[] wrapsMessage = require(
                "formatRotationMessage",
                WRAPS.formatRotationMessage(schnorrPublicKeys, WEIGHTS, NODE_IDS, era.hintsVerificationKey()));
        final byte[] aggregateSignature =
                aggregateSchnorr(era.schnorr(), schnorrPublicKeys, wrapsMessage, random32(random));
        // Genesis proof: prev == next == this roster, real hinTS VK, no previous proof.
        final Proof proof = require(
                "constructWrapsProof",
                WRAPS.constructWrapsProof(
                        era.ledgerId(),
                        schnorrPublicKeys,
                        WEIGHTS,
                        NODE_IDS,
                        schnorrPublicKeys,
                        WEIGHTS,
                        NODE_IDS,
                        null,
                        era.hintsVerificationKey(),
                        aggregateSignature));
        final byte[] compressed = proof.compressed();
        if (compressed.length != COMPRESSED_WRAPS_PROOF_LENGTH) {
            throw new IllegalStateException("unexpected compressed WRAPS proof length " + compressed.length);
        }
        return compressed;
    }

    @NonNull
    private static SecureRandom deterministicRandom() {
        try {
            final SecureRandom random = SecureRandom.getInstance("SHA1PRNG");
            random.setSeed(DETERMINISTIC_SEED);
            return random;
        } catch (final NoSuchAlgorithmException fatal) {
            throw new IllegalStateException("SHA1PRNG unavailable", fatal);
        }
    }

    /// Runs the one-time key ceremony (roster, hinTS CRS, keys) shared by every factory.
    private static Era buildEra(final SecureRandom random) {
        HINTS.resetCache();

        // Schnorr roster + ledger id (Poseidon hash of the address book).
        final SchnorrKeys schnorr = require("generateSchnorrKeys", WRAPS.generateSchnorrKeys(random32(random)));
        final byte[][] schnorrPublicKeys = {schnorr.publicKey()};
        final byte[] ledgerId = require("hashAddressBook", WRAPS.hashAddressBook(schnorrPublicKeys, WEIGHTS, NODE_IDS));

        // hinTS CRS ceremony: init -> at least one contribution -> prune to the universe size.
        byte[] crs = require("initCRS", HINTS.initCRS((short) N));
        crs = require("updateCRS", HINTS.updateCRS(crs, random32(random)));
        crs = require("pruneCRS", HINTS.pruneCRS(crs, (short) N));

        // hinTS per-party keys + preprocessing.
        final byte[] blsSecretKey = require("generateSecretKey", HINTS.generateSecretKey(random32(random)));
        final byte[] hintsPublicKey = require("computeHints", HINTS.computeHints(crs, blsSecretKey, 0, N));
        final byte[][] hintsPublicKeys = {hintsPublicKey};
        final AggregationAndVerificationKeys keys =
                require("preprocess", HINTS.preprocess(crs, PARTIES, hintsPublicKeys, WEIGHTS, N));
        return new Era(crs, keys.verificationKey(), keys.aggregationKey(), blsSecretKey, ledgerId, schnorr);
    }

    private static TssBlockSigner fromEra(final Era era, final byte[] addressBookProof) {
        return new TssBlockSigner(
                era.crs(),
                era.hintsVerificationKey(),
                era.aggregationKey(),
                era.blsSecretKey(),
                addressBookProof,
                era.ledgerId(),
                era.schnorr().publicKey());
    }

    private static byte[] loadCommittedWrapsProof() {
        try (final InputStream in = TssBlockSigner.class.getResourceAsStream(WRAPS_PROOF_RESOURCE)) {
            if (in == null) {
                throw new IllegalStateException("missing committed WRAPS proof resource " + WRAPS_PROOF_RESOURCE
                        + "; regenerate it with generateGenesisWrapsProof()");
            }
            final byte[] proof = in.readAllBytes();
            if (proof.length != COMPRESSED_WRAPS_PROOF_LENGTH) {
                throw new IllegalStateException("committed WRAPS proof has length " + proof.length + ", expected "
                        + COMPRESSED_WRAPS_PROOF_LENGTH);
            }
            return proof;
        } catch (final IOException fatal) {
            throw new UncheckedIOException("failed to read committed WRAPS proof", fatal);
        }
    }

    /// One-time deterministic key material shared by the genesis and settled factories.
    private record Era(
            byte[] crs,
            byte[] hintsVerificationKey,
            byte[] aggregationKey,
            byte[] blsSecretKey,
            byte[] ledgerId,
            SchnorrKeys schnorr) {}

    @Override
    @NonNull
    public BlockProof signBlockProof(final long blockNumber, @NonNull final Bytes blockRootHash) {
        final byte[] message = blockRootHash.toByteArray();
        final byte[] partialSignature = require("signBls", HINTS.signBls(message, blsSecretKey));
        final byte[] hintsSignature = require(
                "aggregateSignatures",
                HINTS.aggregateSignatures(
                        crs, aggregationKey, hintsVerificationKey, PARTIES, new byte[][] {partialSignature}));
        final byte[] tssSignature = TSS.composeSignature(hintsVerificationKey, hintsSignature, addressBookProof);
        return BlockProof.newBuilder()
                .block(blockNumber)
                .signedBlockProof(TssSignedBlockProof.newBuilder()
                        .blockSignature(Bytes.wrap(tssSignature))
                        .build())
                .build();
    }

    @Override
    @NonNull
    public VerificationMaterial verificationMaterial() {
        return verificationMaterial;
    }

    /// Builds the serialized `SignedTransaction` bytes carrying this signer's roster as a
    /// `LedgerIdPublicationTransactionBody`.
    ///
    /// Placing this as a signed-transaction item inside block 0 lets a verifier self-provision its
    /// `TssData` from the block stream itself (see `BlockHasher.findLedgerIdPublication`), so no
    /// out-of-band bootstrap file is needed for a producer that streams from genesis. The returned
    /// bytes are the value of the block item's signed-transaction field, portable across PBJ and
    /// protoc consumers.
    @NonNull
    public Bytes genesisLedgerIdSignedTransaction() {
        final LedgerIdPublicationTransactionBody publication = LedgerIdPublicationTransactionBody.newBuilder()
                .ledgerId(Bytes.wrap(ledgerId))
                .historyProofVerificationKey(Bytes.wrap(WRAPSVerificationKey.getDefaultKey()))
                .nodeContributions(List.of(LedgerIdNodeContribution.newBuilder()
                        .nodeId(NODE_ID)
                        .weight(WEIGHT)
                        .historyProofKey(Bytes.wrap(schnorrPublicKey))
                        .build()))
                .build();
        final TransactionBody body =
                TransactionBody.newBuilder().ledgerIdPublication(publication).build();
        final SignedTransaction signedTransaction = SignedTransaction.newBuilder()
                .bodyBytes(TransactionBody.PROTOBUF.toBytes(body))
                .build();
        return SignedTransaction.PROTOBUF.toBytes(signedTransaction);
    }

    /// Runs the 4-phase threshold-Schnorr protocol for the single signer and returns the 192-byte
    /// aggregate signature over `message`. R1 takes an empty roster; R2/R3/Aggregate take the full
    /// roster plus the prior rounds' messages.
    private static byte[] aggregateSchnorr(
            final SchnorrKeys schnorr, final byte[][] schnorrPublicKeys, final byte[] message, final byte[] entropy) {
        final boolean[] signers = {true};
        final byte[] privateKey = schnorr.privateKey();
        final byte[][] round1 = {
            require(
                    "R1",
                    WRAPS.runSigningProtocolPhase(
                            SigningProtocolPhase.R1,
                            entropy,
                            message,
                            privateKey,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null))
        };
        final byte[][] round2 = {
            require(
                    "R2",
                    WRAPS.runSigningProtocolPhase(
                            SigningProtocolPhase.R2,
                            entropy,
                            message,
                            privateKey,
                            schnorrPublicKeys,
                            WEIGHTS,
                            NODE_IDS,
                            signers,
                            round1,
                            null,
                            null))
        };
        final byte[][] round3 = {
            require(
                    "R3",
                    WRAPS.runSigningProtocolPhase(
                            SigningProtocolPhase.R3,
                            entropy,
                            message,
                            privateKey,
                            schnorrPublicKeys,
                            WEIGHTS,
                            NODE_IDS,
                            signers,
                            round1,
                            round2,
                            null))
        };
        return require(
                "Aggregate",
                WRAPS.runSigningProtocolPhase(
                        SigningProtocolPhase.Aggregate,
                        null,
                        message,
                        null,
                        schnorrPublicKeys,
                        WEIGHTS,
                        NODE_IDS,
                        signers,
                        round1,
                        round2,
                        round3));
    }

    private static byte[] random32(final SecureRandom random) {
        final byte[] bytes = new byte[WRAPSLibraryBridge.ENTROPY_SIZE];
        random.nextBytes(bytes);
        return bytes;
    }

    private static byte[] concat(final byte[] left, final byte[] right) {
        final byte[] result = new byte[left.length + right.length];
        System.arraycopy(left, 0, result, 0, left.length);
        System.arraycopy(right, 0, result, left.length, right.length);
        return result;
    }

    private static <T> T require(final String operation, final T result) {
        if (result == null) {
            throw new IllegalStateException("TSS native call '" + operation + "' returned null (invalid input)");
        }
        return result;
    }
}
