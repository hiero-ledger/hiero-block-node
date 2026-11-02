// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.signing;

import com.hedera.cryptography.hints.AggregationAndVerificationKeys;
import com.hedera.cryptography.hints.HintsLibraryBridge;
import com.hedera.cryptography.tss.TSS;
import com.hedera.cryptography.wraps.Proof;
import com.hedera.cryptography.wraps.SchnorrKeys;
import com.hedera.cryptography.wraps.WRAPSLibraryBridge;
import com.hedera.cryptography.wraps.WRAPSLibraryBridge.SigningProtocolPhase;
import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.hapi.block.stream.TssSignedBlockProof;
import com.hedera.hapi.node.transaction.SignedTransaction;
import com.hedera.hapi.node.transaction.TransactionBody;
import com.hedera.hapi.node.tss.LedgerIdNodeContribution;
import com.hedera.hapi.node.tss.LedgerIdPublicationTransactionBody;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import org.hiero.block.api.RosterEntry;
import org.hiero.block.api.TssData;
import org.hiero.block.api.TssRoster;

/// Produces TSS/hinTS threshold [BlockProof]s that the block-verification `TSSVerifier` accepts.
///
/// A block signature is composed by `TSS.composeSignature` from the hinTS verification key, the hinTS
/// aggregate signature over the block root hash, and the WRAPS that places the hinTS verification key
/// in the chain of trust of the ledger id. The WRAPS is the same for every block the signer produces.
/// Constructing it runs a SNARK prover, so it is computed once, offline, for a fixed roster by
/// [#generateGenesisWrapsProof] and committed as a resource that [#create] loads. Construction needs
/// the full TSS library jar; verifying, and everything else this signer does, works with the verifier
/// variant the build resolves by default.
///
/// The signer uses a single-node roster (hinTS universe `n = 2`, the smallest valid power of two,
/// since the maximum number of parties is `n - 1`). All per-era key material, the hinTS CRS, BLS
/// secret, aggregation and verification keys, Schnorr keys and the ledger id, derives from a fixed
/// seed, so every instance in a JVM shares one roster: several simulator publishers sign
/// identically, a node can self-provision from any of them, and duplicate blocks are byte-identical
/// (and so detectable).
///
/// The ledger id is `WRAPSLibraryBridge.computeNetworkID(roster, hintsVerificationKey)` over the
/// genesis roster, which is what a consensus node publishes in
/// `LedgerIdPublicationTransactionBody.ledger_id`. The paired [#verificationMaterial] is a [TssData]
/// carrying that ledger id and the roster.
public final class TssBlockSigner implements BlockSigner {

    /// hinTS universe size (power of two). Max parties = N - 1, so N = 2 supports the single node.
    private static final int N = 2;

    private static final long NODE_ID = 0L;
    private static final long WEIGHT = 1L;

    /// Fixed seed for the deterministic key ceremony; any constant works since these are test-only keys.
    private static final byte[] DETERMINISTIC_SEED = "hiero-block-signing-test-seed".getBytes();
    private static final int[] PARTIES = {0};
    private static final long[] WEIGHTS = {WEIGHT};
    private static final long[] NODE_IDS = {NODE_ID};

    /// Classpath resource holding the pre-computed genesis WRAPS for the deterministic roster.
    private static final String WRAPS_PROOF_RESOURCE = "/org/hiero/block/signing/genesis-wraps-proof.bin";

    /// How to regenerate the committed genesis WRAPS resource, appended to every failure about it.
    private static final String REGENERATE_WRAPS_HINT = "regenerate it with"
            + " `GENERATE_WRAPS_PROOF=true ./gradlew :block-signing:test --tests '*WrapsProofGeneratorTest*'`,"
            + " which rewrites tools-and-tests/block-signing/src/main/resources" + WRAPS_PROOF_RESOURCE
            + ", and commit the result";

    /// Failure message of [#create] when the committed genesis WRAPS does not verify.
    static final String COMMITTED_WRAPS_DOES_NOT_VERIFY = "The committed genesis WRAPS does not verify for the"
            + " signer's roster with the TSS library on the class path, so no signature of TssBlockSigner would"
            + " verify. This usually follows a TSS library upgrade that changed the WRAPS format; "
            + REGENERATE_WRAPS_HINT + ".";

    private static final HintsLibraryBridge HINTS = HintsLibraryBridge.getInstance();
    private static final WRAPSLibraryBridge WRAPS = WRAPSLibraryBridge.getInstance();

    /// Serializes the signing side of the JVM-wide hinTS/WRAPS native singletons above. The hinTS library
    /// caches the signing context of the most recent ceremony natively until `resetCache()`, so the key
    /// ceremony (which resets that cache) and signing must never overlap across instances in one JVM; the
    /// multi-publisher suites run several signers on separate threads in a single JVM. Verification does
    /// not use that cache and needs no lock.
    private static final ReentrantLock NATIVE_LOCK = new ReentrantLock();

    /// Set once [#create] has checked that the committed genesis WRAPS verifies; the resource and the roster
    /// are the same for every instance, so the check runs once per JVM. Read and written only under
    /// [#NATIVE_LOCK].
    private static boolean committedWrapsVerified = false;

    private final byte[] crs;
    private final byte[] hintsVerificationKey;
    private final byte[] aggregationKey;
    private final byte[] blsSecretKey;
    private final byte[] wrapsProof;
    private final byte[] ledgerId;
    private final byte[] schnorrPublicKey;
    private final VerificationMaterial verificationMaterial;

    private TssBlockSigner(final Era era, final byte[] wrapsProof) {
        this.crs = era.crs();
        this.hintsVerificationKey = era.hintsVerificationKey();
        this.aggregationKey = era.aggregationKey();
        this.blsSecretKey = era.blsSecretKey();
        this.wrapsProof = wrapsProof;
        this.ledgerId = era.ledgerId();
        this.schnorrPublicKey = era.schnorr().publicKey();
        this.verificationMaterial = VerificationMaterial.ofTss(TssData.newBuilder()
                .ledgerId(Bytes.wrap(ledgerId))
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

    /// Runs the deterministic key ceremony and returns a signer whose WRAPS is the committed
    /// resource generated by [#generateGenesisWrapsProof] over the same roster.
    ///
    /// The hinTS native library keeps singleton state cached from the most recent ceremony, so the
    /// cache is reset first. Every instance derives the same key material, so creating another
    /// instance never invalidates an existing one.
    ///
    /// The first call in a JVM also checks that the committed WRAPS verifies with the TSS library on the
    /// class path, so a resource left stale by a library upgrade fails here, with the command that
    /// regenerates it, instead of as unexplained verification failures of every signed block.
    ///
    /// @return a signer for the deterministic roster
    /// @throws IllegalStateException if the committed WRAPS is missing, empty, or does not verify
    public static TssBlockSigner create() {
        NATIVE_LOCK.lock();
        try {
            final TssBlockSigner signer =
                    new TssBlockSigner(buildEra(deterministicRandom()), loadCommittedWrapsProof());
            final TssBlockSigner result;
            if (committedWrapsVerified) {
                result = signer;
            } else if (signer.committedWrapsVerifies()) {
                committedWrapsVerified = true;
                result = signer;
            } else {
                throw new IllegalStateException(COMMITTED_WRAPS_DOES_NOT_VERIFY);
            }
            return result;
        } finally {
            NATIVE_LOCK.unlock();
        }
    }

    /// Constructs the compressed genesis WRAPS for the deterministic roster.
    ///
    /// This is the slow step: it runs the SNARK prover over the public parameters that only the full TSS
    /// library jar embeds (about ten seconds on a laptop). It is meant to be run offline, once, to
    /// (re)generate the committed resource, not at test run time; with the verifier variant of the
    /// library on the class path the prover is unavailable and this method fails.
    ///
    /// The WRAPS is a genesis one: previous and next address book are both this roster, there is no
    /// previous proof, and the aggregate Schnorr signature is over the ledger id itself. The result is
    /// verified against that ledger id and the hinTS verification key before it is returned.
    ///
    /// @return the compressed genesis WRAPS
    /// @throws IllegalStateException if the library cannot construct a WRAPS, or the result does not verify
    public static byte[] generateGenesisWrapsProof() {
        NATIVE_LOCK.lock();
        try {
            final SecureRandom random = deterministicRandom();
            final Era era = buildEra(random);
            final byte[][] schnorrPublicKeys = {era.schnorr().publicKey()};
            final byte[] aggregateSignature =
                    aggregateSchnorr(era.schnorr(), schnorrPublicKeys, era.ledgerId(), random32(random));
            final Proof proof = WRAPS.constructWrapsProof(
                    era.addressBookHash(),
                    schnorrPublicKeys,
                    WEIGHTS,
                    NODE_IDS,
                    schnorrPublicKeys,
                    WEIGHTS,
                    NODE_IDS,
                    null,
                    era.hintsVerificationKey(),
                    aggregateSignature);
            final byte[] result;
            if (proof == null || proof.compressed() == null) {
                throw new IllegalStateException("the TSS library could not construct a WRAPS; construction needs the"
                        + " full hedera-cryptography-wraps jar, not its verifier variant");
            } else if (!WRAPS.verifyCompressedProof(proof.compressed(), era.ledgerId(), era.hintsVerificationKey())) {
                throw new IllegalStateException("freshly constructed genesis WRAPS does not verify");
            } else {
                result = proof.compressed();
            }
            return result;
        } finally {
            NATIVE_LOCK.unlock();
        }
    }

    private static SecureRandom deterministicRandom() {
        try {
            final SecureRandom random = SecureRandom.getInstance("SHA1PRNG");
            random.setSeed(DETERMINISTIC_SEED);
            return random;
        } catch (final NoSuchAlgorithmException fatal) {
            throw new IllegalStateException("SHA1PRNG unavailable", fatal);
        }
    }

    /// Runs the one-time key ceremony (roster, hinTS CRS, keys, ledger id). Callers hold [#NATIVE_LOCK];
    /// the `HINTS.resetCache()` here would corrupt an in-flight sign on the shared native singleton
    /// otherwise. The order of the random draws is part of the determinism: [#create] and
    /// [#generateGenesisWrapsProof] must derive the same era.
    private static Era buildEra(final SecureRandom random) {
        HINTS.resetCache();

        // Schnorr roster and its address book hash (the genesis address book the WRAPS starts from).
        final SchnorrKeys schnorr = require("generateSchnorrKeys", WRAPS.generateSchnorrKeys(random32(random)));
        final byte[][] schnorrPublicKeys = {schnorr.publicKey()};
        final byte[] addressBookHash =
                require("hashAddressBook", WRAPS.hashAddressBook(schnorrPublicKeys, WEIGHTS, NODE_IDS));

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

        // The ledger id of a network with this genesis roster and hinTS verification key.
        final byte[] ledgerId = require(
                "computeNetworkID",
                WRAPS.computeNetworkID(schnorrPublicKeys, WEIGHTS, NODE_IDS, keys.verificationKey()));
        return new Era(
                crs, keys.verificationKey(), keys.aggregationKey(), blsSecretKey, addressBookHash, ledgerId, schnorr);
    }

    private static byte[] loadCommittedWrapsProof() {
        try (final InputStream in = TssBlockSigner.class.getResourceAsStream(WRAPS_PROOF_RESOURCE)) {
            final byte[] result;
            if (in == null) {
                throw new IllegalStateException(
                        "missing committed WRAPS resource " + WRAPS_PROOF_RESOURCE + "; " + REGENERATE_WRAPS_HINT);
            } else {
                final byte[] proof = in.readAllBytes();
                if (proof.length == 0) {
                    throw new IllegalStateException(
                            "committed WRAPS resource " + WRAPS_PROOF_RESOURCE + " is empty; " + REGENERATE_WRAPS_HINT);
                } else {
                    result = proof;
                }
            }
            return result;
        } catch (final IOException fatal) {
            throw new UncheckedIOException("failed to read the committed WRAPS", fatal);
        }
    }

    /// One-time deterministic key material of the roster.
    private record Era(
            byte[] crs,
            byte[] hintsVerificationKey,
            byte[] aggregationKey,
            byte[] blsSecretKey,
            byte[] addressBookHash,
            byte[] ledgerId,
            SchnorrKeys schnorr) {}

    @Override
    public BlockProof signBlockProof(final long blockNumber, final Bytes blockRootHash) {
        final byte[] message = blockRootHash.toByteArray();
        final byte[] tssSignature;
        NATIVE_LOCK.lock();
        try {
            final byte[] partialSignature = require("signBls", HINTS.signBls(message, blsSecretKey));
            final byte[] hintsSignature = require(
                    "aggregateSignatures",
                    HINTS.aggregateSignatures(
                            crs, aggregationKey, hintsVerificationKey, PARTIES, new byte[][] {partialSignature}));
            tssSignature = TSS.composeSignature(hintsVerificationKey, hintsSignature, wrapsProof);
        } finally {
            NATIVE_LOCK.unlock();
        }
        return BlockProof.newBuilder()
                .block(blockNumber)
                .signedBlockProof(TssSignedBlockProof.newBuilder()
                        .blockSignature(Bytes.wrap(tssSignature))
                        .build())
                .build();
    }

    @Override
    public VerificationMaterial verificationMaterial() {
        return verificationMaterial;
    }

    /// Reports whether the committed genesis WRAPS, which this signer puts into every signature, verifies
    /// for the signer's ledger id and hinTS verification key with the TSS library on the class path. A
    /// WRAPS that does not verify makes every signature of this signer fail verification; [#create]
    /// checks this once per JVM, the signer's own tests on every run.
    ///
    /// @return `true` if the committed WRAPS verifies, `false` otherwise
    boolean committedWrapsVerifies() {
        return WRAPS.verifyCompressedProof(wrapsProof, ledgerId, hintsVerificationKey);
    }

    /// Builds the serialized `SignedTransaction` bytes carrying this signer's ledger id and roster as a
    /// `LedgerIdPublicationTransactionBody`.
    ///
    /// Placing this as a signed-transaction item inside block 0 lets a verifier self-provision its
    /// `TssData` from the block stream itself (see `BlockHasher.findLedgerIdPublication`), so no
    /// out-of-band bootstrap file is needed for a producer that streams from genesis. Like a consensus
    /// node, the publication carries no history proof verification key: the TSS library embeds what it
    /// verifies a WRAPS with. The returned bytes are the value of the block item's
    /// signed-transaction field, portable across PBJ and protoc consumers.
    public Bytes genesisLedgerIdSignedTransaction() {
        final LedgerIdPublicationTransactionBody publication = LedgerIdPublicationTransactionBody.newBuilder()
                .ledgerId(Bytes.wrap(ledgerId))
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

    /// Runs the 4-phase threshold-Schnorr protocol for the single signer and returns the aggregate
    /// signature over `message`. R1 takes an empty roster; R2/R3/Aggregate take the full roster plus
    /// the prior rounds' messages. Only the genesis proof construction needs it.
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

    private static <T> T require(final String operation, final T result) {
        if (result == null) {
            throw new IllegalStateException("TSS native call '" + operation + "' returned null (invalid input)");
        } else {
            return result;
        }
    }
}
