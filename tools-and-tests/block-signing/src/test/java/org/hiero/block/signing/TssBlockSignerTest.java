// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.signing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import com.hedera.cryptography.tss.TSS;
import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.hapi.node.transaction.SignedTransaction;
import com.hedera.hapi.node.transaction.TransactionBody;
import com.hedera.hapi.node.tss.LedgerIdPublicationTransactionBody;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.hiero.block.api.TssData;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/// Verifies that [TssBlockSigner] produces signatures the block-verification `TSSVerifier` accepts.
///
/// The tests drive the exact library call the verifier performs, `TSS.verifyTSS(ledgerId, signature,
/// rootHash)` with the ledger id taken from the signer's [TssData], so a passing test means the real
/// verifier accepts the proof. No other library state is needed: the library embeds what it verifies
/// against. The signature and the ledger id are treated as opaque bytes, as the verifier does.
class TssBlockSignerTest {

    /// Tests of the block signatures the signer produces.
    @Nested
    @DisplayName("Block signatures")
    class BlockSignatureTests {
        /// This test aims to assert that a signed block proof carries a TSS signature and the requested
        /// block number, and that `TSS.verifyTSS` accepts the signature for the signer's ledger id and the
        /// signed block root hash.
        @Test
        @DisplayName("signBlockProof() produces a signature accepted by TSS.verifyTSS()")
        void signatureIsAcceptedByVerifyTss() {
            final TssBlockSigner signer = TssBlockSigner.create();
            final Bytes blockRootHash = sha384("block-root-hash");

            final BlockProof proof = signer.signBlockProof(7L, blockRootHash);

            assertThat(proof.block()).isEqualTo(7L);
            assertThat(proof.hasSignedBlockProof()).isTrue();
            assertThat(verifies(signer, proof.signedBlockProof().blockSignature(), blockRootHash))
                    .isTrue();
        }

        /// This test aims to assert that a signature over one block root hash is rejected by
        /// `TSS.verifyTSS` when checked against a different hash, so the signature really binds the
        /// signed hash.
        @Test
        @DisplayName("a signature does not verify for a different block root hash")
        void signatureDoesNotVerifyForADifferentBlockRootHash() {
            final TssBlockSigner signer = TssBlockSigner.create();
            final BlockProof proof = signer.signBlockProof(1L, sha384("the-signed-root"));
            final Bytes signature = proof.signedBlockProof().blockSignature();

            assertThat(verifies(signer, signature, sha384("a-different-root"))).isFalse();
        }

        /// This test aims to assert that flipping a single byte anywhere in a signature, at its start
        /// (where the hinTS verification key is), in its middle and at its end (where the WRAPS is), makes
        /// `TSS.verifyTSS` reject it, so every part of the signature is checked on every verification.
        ///
        /// @param position where to flip a byte, as a fraction of the signature length in percent
        @ParameterizedTest(name = "flipped byte at {0} percent")
        @ValueSource(ints = {0, 50, 100})
        @DisplayName("a tampered signature does not verify")
        void tamperedSignatureDoesNotVerify(final int position) {
            final TssBlockSigner signer = TssBlockSigner.create();
            final Bytes blockRootHash = sha384("block-root-hash");
            final byte[] signature = signer.signBlockProof(1L, blockRootHash)
                    .signedBlockProof()
                    .blockSignature()
                    .toByteArray();
            final int index = Math.min(signature.length - 1, signature.length * position / 100);
            signature[index] ^= 0x01;

            assertThat(verifies(signer, Bytes.wrap(signature), blockRootHash)).isFalse();
        }
    }

    /// Tests of the verification material paired with the signatures.
    @Nested
    @DisplayName("Verification material")
    class VerificationMaterialTests {
        /// This test aims to assert that the signer's [TssData] carries a ledger id, a single-entry
        /// informational roster, no WRAPS verification key (the TSS library no longer uses one), and is
        /// valid from block 0.
        @Test
        @DisplayName("TssData carries the ledger id and roster, no WRAPS verification key")
        void tssDataCarriesLedgerIdAndRoster() {
            final TssData tssData =
                    TssBlockSigner.create().verificationMaterial().tssData();

            assertThat(tssData.ledgerId().length()).isPositive();
            assertThat(tssData.currentRoster().rosterEntries()).hasSize(1);
            assertThat(tssData.wrapsVerificationKey().length()).isZero();
            assertThat(tssData.validFromBlock()).isZero();
        }

        /// This test aims to assert that every signer created in a JVM derives the same roster and
        /// ledger id, so several publishers in one test share one chain of trust and a verifier
        /// provisioned from any of them accepts blocks from all of them.
        @Test
        @DisplayName("signers are deterministic")
        void signersAreDeterministic() {
            final TssBlockSigner first = TssBlockSigner.create();
            final TssBlockSigner second = TssBlockSigner.create();
            final Bytes blockRootHash = sha384("root");

            assertThat(second.verificationMaterial().tssData())
                    .isEqualTo(first.verificationMaterial().tssData());
            assertThat(verifies(
                            first,
                            second.signBlockProof(3L, blockRootHash)
                                    .signedBlockProof()
                                    .blockSignature(),
                            blockRootHash))
                    .isTrue();
        }

        /// This test aims to assert that the genesis ledger id publication the signer builds for
        /// block 0 parses as a signed transaction whose body carries the signer's ledger id and its
        /// roster entry as a node contribution, and no history proof verification key, matching what
        /// a consensus node publishes.
        @Test
        @DisplayName("the genesis publication carries the ledger id and roster, no verification key")
        void genesisPublicationCarriesLedgerIdAndRoster() throws ParseException {
            final TssBlockSigner signer = TssBlockSigner.create();
            final TssData tssData = signer.verificationMaterial().tssData();

            final SignedTransaction signedTransaction =
                    SignedTransaction.PROTOBUF.parse(signer.genesisLedgerIdSignedTransaction());
            final TransactionBody body = TransactionBody.PROTOBUF.parse(signedTransaction.bodyBytes());
            final LedgerIdPublicationTransactionBody publication = body.ledgerIdPublicationOrThrow();

            assertThat(publication.ledgerId()).isEqualTo(tssData.ledgerId());
            assertThat(publication.historyProofVerificationKey().length()).isZero();
            assertThat(publication.nodeContributions()).hasSize(1);
            assertThat(publication.nodeContributions().getFirst().historyProofKey())
                    .isEqualTo(
                            tssData.currentRoster().rosterEntries().getFirst().schnorrPublicKey());
        }
    }

    /// Tests of the committed genesis WRAPS resource.
    @Nested
    @DisplayName("Committed genesis WRAPS")
    class CommittedWrapsTests {
        /// This test aims to assert that the committed genesis WRAPS resource verifies, with the TSS
        /// library on the class path, for the ledger id and hinTS verification key of the signer's roster.
        /// Every signature the signer produces carries that WRAPS, so when it stops verifying, typically
        /// after a TSS library upgrade that changes the WRAPS format, every TSS signed test block in the
        /// build fails verification. `create()` already refuses such a resource, once per JVM, with the
        /// command that regenerates it; this test runs the check again regardless of that, so the cause is
        /// named by a test of its own, with the same message.
        @Test
        @DisplayName("the committed genesis WRAPS verifies with the TSS library on the class path")
        void committedGenesisWrapsVerifies() {
            final TssBlockSigner signer = TssBlockSigner.create();

            assertThat(signer.committedWrapsVerifies())
                    .withFailMessage(TssBlockSigner.COMMITTED_WRAPS_DOES_NOT_VERIFY)
                    .isTrue();
        }
    }

    /// Tests of the offline WRAPS construction.
    @Nested
    @DisplayName("Genesis WRAPS construction")
    class GenesisWrapsConstructionTests {
        /// This test aims to assert that, with the verifier variant of the TSS library the build uses by
        /// default, constructing a genesis WRAPS fails with an explanation that the full library jar is
        /// needed, rather than returning nothing or producing an unusable resource. It is skipped when
        /// `GENERATE_WRAPS_PROOF=true` puts the full jar on the test class path.
        @Test
        @DisplayName("construction needs the full TSS library jar")
        void constructionNeedsTheFullLibraryJar() {
            assumeFalse("true".equals(System.getenv("GENERATE_WRAPS_PROOF")), "the full library jar is in use");

            assertThatThrownBy(TssBlockSigner::generateGenesisWrapsProof)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("full hedera-cryptography-wraps jar");
        }
    }

    /// Runs the verifier's library call for a signer's ledger id.
    private static boolean verifies(final TssBlockSigner signer, final Bytes signature, final Bytes blockRootHash) {
        final byte[] ledgerId =
                signer.verificationMaterial().tssData().ledgerId().toByteArray();
        return TSS.verifyTSS(ledgerId, signature.toByteArray(), blockRootHash.toByteArray());
    }

    private static Bytes sha384(final String text) {
        try {
            return Bytes.wrap(MessageDigest.getInstance("SHA-384").digest(text.getBytes()));
        } catch (final NoSuchAlgorithmException fatal) {
            throw new IllegalStateException(fatal);
        }
    }
}
