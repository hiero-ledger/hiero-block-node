// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.signing;

import com.hedera.cryptography.hints.AggregationAndVerificationKeys;
import com.hedera.cryptography.hints.HintsLibraryBridge;
import com.hedera.cryptography.tss.TSS;
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
import java.security.SecureRandom;
import java.util.List;
import org.hiero.block.api.RosterEntry;
import org.hiero.block.api.TssData;
import org.hiero.block.api.TssRoster;

/// Produces TSS/hinTS threshold [BlockProof]s that the block-verification `TSSVerifier` accepts via
/// the genesis Schnorr-aggregate path — no WRAPS SNARK proving artifacts required.
///
/// The signer uses a single-node roster (hinTS universe `n = 2`, the smallest valid power of two,
/// since the maximum number of parties is `n - 1`). All per-era key material — the hinTS CRS, BLS
/// secret, aggregation/verification keys, the 192-byte Schnorr address-book proof, and the ledger id
/// — is generated once at construction. [#signBlockProof] then only BLS-signs the block root hash and
/// aggregates it, which is cheap.
///
/// The paired [#verificationMaterial] is a [TssData] whose roster, ledger id, and (unused-on-this-path
/// but length-validated) WRAPS verification key exactly match what the verifier needs: it pushes the
/// roster via `TSS.setAddressBook(...)` and then calls `TSS.verifyTSS(ledgerId, signature, rootHash)`.
public final class TssBlockSigner implements BlockSigner {

    /// hinTS universe size (power of two). Max parties = N - 1, so N = 2 supports the single node.
    private static final int N = 2;

    private static final long NODE_ID = 0L;
    private static final long WEIGHT = 1L;
    private static final int[] PARTIES = {0};
    private static final long[] WEIGHTS = {WEIGHT};
    private static final long[] NODE_IDS = {NODE_ID};

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

    /// Runs the full one-time key ceremony and returns a ready-to-use signer.
    ///
    /// The hinTS native library keeps singleton state cached from the most recent ceremony, so the
    /// cache is reset first. As a result only one [TssBlockSigner] should be actively signing at a
    /// time in a given JVM; creating a second signer invalidates the first.
    @NonNull
    public static TssBlockSigner create() {
        HINTS.resetCache();
        final SecureRandom random = new SecureRandom();

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
        final byte[] hintsVerificationKey = keys.verificationKey();
        final byte[] aggregationKey = keys.aggregationKey();

        // Genesis address-book proof: a 192-byte Schnorr aggregate over the rotation message
        // `ledgerId || hashArray(hintsVerificationKey)`, which is exactly what TSS.verifyTSS rebuilds.
        final byte[] hintsKeyHash = require("hashArray", WRAPS.hashArray(hintsVerificationKey));
        final byte[] rotationMessage = concat(ledgerId, hintsKeyHash);
        final byte[] addressBookProof = aggregateSchnorr(schnorr, schnorrPublicKeys, rotationMessage, random32(random));

        return new TssBlockSigner(
                crs,
                hintsVerificationKey,
                aggregationKey,
                blsSecretKey,
                addressBookProof,
                ledgerId,
                schnorr.publicKey());
    }

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
