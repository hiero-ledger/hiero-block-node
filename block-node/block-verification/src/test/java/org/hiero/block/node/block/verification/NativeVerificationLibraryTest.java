// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.hiero.block.signing.TssBlockSigner;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;
import org.junit.jupiter.api.Timeout;

/// Tests for [NativeVerificationLibrary], the gateway to the TSS native library.
///
/// The library loading group runs first so that, when this class runs alone in a fresh JVM, the warm-up
/// is the very first use of the native library: that is the order in which the plugin uses it, and the
/// order in which the library's lazy loading must not fail. Run it alone with
/// `./gradlew :block-verification:test --tests '*NativeVerificationLibraryTest*'` after touching the
/// gateway. Signatures and ledger ids come from the test signer and are treated as opaque bytes.
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
@Timeout(unit = SECONDS, value = 120)
@DisplayName("NativeVerificationLibrary Tests")
class NativeVerificationLibraryTest {

    /// Tests of loading the native library and its embedded verification key.
    @Nested
    @Order(1)
    @DisplayName("Library loading")
    class LibraryLoadingTests {
        /// This test aims to assert that the warm-up loads the native library and its verification key
        /// and reports that TSS block proofs can be verified, also when it is the first use of the
        /// library in the JVM, and that asking again gives the same answer.
        @Test
        @DisplayName("warmUp() reports that TSS block proofs can be verified")
        void warmUpReportsVerificationSupported() {
            assertThat(NativeVerificationLibrary.warmUp()).isTrue();
            assertThat(NativeVerificationLibrary.warmUp()).isTrue();
        }

        /// This test aims to assert that the gateway reports the version of the TSS library module, which
        /// the plugin logs at start and in failures.
        @Test
        @DisplayName("libraryVersion() reports the TSS library version")
        void libraryVersionIsReported() {
            assertThat(NativeVerificationLibrary.libraryVersion()).isNotBlank();
        }
    }

    /// Tests of verifying single signatures.
    @Nested
    @Order(2)
    @DisplayName("Verification")
    class VerificationTests {
        private static TssBlockSigner signer;
        private static byte[] ledgerId;
        private static byte[] hash;
        private static byte[] signature;

        @BeforeAll
        static void sign() {
            signer = TssBlockSigner.create();
            ledgerId = signer.verificationMaterial().tssData().ledgerId().toByteArray();
            hash = sha384("verification");
            signature = signatureOver(signer, hash);
        }

        /// This test aims to assert that a signature produced by the test signer verifies against the
        /// signer's ledger id and the signed hash.
        @Test
        @DisplayName("a valid signature verifies")
        void validSignatureVerifies() {
            assertThat(NativeVerificationLibrary.verifyTss(ledgerId, signature, hash))
                    .isTrue();
        }

        /// This test aims to assert that flipping the first byte of the ledger id makes a valid
        /// signature fail verification, so the whole ledger id is bound by the verification.
        @Test
        @DisplayName("a signature does not verify for a ledger id with its first byte changed")
        void changedFirstLedgerIdByteFails() {
            final byte[] otherLedgerId = ledgerId.clone();
            otherLedgerId[0] ^= 0x01;

            assertThat(NativeVerificationLibrary.verifyTss(otherLedgerId, signature, hash))
                    .isFalse();
        }

        /// This test aims to assert that flipping the last byte of the ledger id makes a valid
        /// signature fail verification, so the whole ledger id is bound by the verification, not only
        /// a part of it.
        @Test
        @DisplayName("a signature does not verify for a ledger id with its last byte changed")
        void changedLastLedgerIdByteFails() {
            final byte[] otherLedgerId = ledgerId.clone();
            otherLedgerId[otherLedgerId.length - 1] ^= 0x01;

            assertThat(NativeVerificationLibrary.verifyTss(otherLedgerId, signature, hash))
                    .isFalse();
        }

        /// This test aims to assert that a ledger id the library considers malformed, such as a truncated
        /// one, makes the library throw an [IllegalArgumentException] rather than return a result; the
        /// TSS verifier reports that as a bad block proof.
        @Test
        @DisplayName("a truncated ledger id makes the library throw IllegalArgumentException")
        void truncatedLedgerIdThrows() {
            final byte[] truncatedLedgerId = new byte[ledgerId.length / 2];
            System.arraycopy(ledgerId, 0, truncatedLedgerId, 0, truncatedLedgerId.length);

            assertThatThrownBy(() -> NativeVerificationLibrary.verifyTss(truncatedLedgerId, signature, hash))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    /// Tests of verifying signatures requested by concurrent verification sessions.
    @Nested
    @Order(3)
    @DisplayName("Concurrent verification")
    class ConcurrentVerificationTests {
        /// Number of verification tasks started at the same time.
        private static final int TASKS = 8;

        /// This test aims to assert that verifications requested at the same time from several threads, as
        /// the plugin's concurrent verification sessions request them, each return the result they return
        /// on their own: valid signatures verify, while a tampered signature and a signature checked against
        /// another hash do not, in every task.
        @Test
        @DisplayName("concurrent verifications return the same results as sequential ones")
        void concurrentVerificationsAreIndependent() throws InterruptedException, ExecutionException {
            final TssBlockSigner signer = TssBlockSigner.create();
            final byte[] ledgerId =
                    signer.verificationMaterial().tssData().ledgerId().toByteArray();
            final CountDownLatch start = new CountDownLatch(1);
            final List<Future<List<Boolean>>> results = new ArrayList<>(TASKS);
            try (final ExecutorService executor = Executors.newFixedThreadPool(TASKS)) {
                for (int task = 0; task < TASKS; task++) {
                    final byte[] hash = sha384("concurrent-" + task);
                    final byte[] valid = signatureOver(signer, hash);
                    final byte[] tampered = valid.clone();
                    tampered[tampered.length / 2] ^= 0x01;
                    final Callable<List<Boolean>> verifications = () -> {
                        start.await();
                        return List.of(
                                NativeVerificationLibrary.verifyTss(ledgerId, valid, hash),
                                NativeVerificationLibrary.verifyTss(ledgerId, tampered, hash),
                                NativeVerificationLibrary.verifyTss(ledgerId, valid, sha384("another hash")));
                    };
                    results.add(executor.submit(verifications));
                }
                start.countDown();
                for (final Future<List<Boolean>> result : results) {
                    assertThat(result.get()).containsExactly(true, false, false);
                }
            }
        }
    }

    private static byte[] signatureOver(final TssBlockSigner signer, final byte[] hash) {
        return signer.signBlockProof(1L, Bytes.wrap(hash))
                .signedBlockProof()
                .blockSignature()
                .toByteArray();
    }

    private static byte[] sha384(final String text) {
        try {
            return MessageDigest.getInstance("SHA-384").digest(text.getBytes());
        } catch (final NoSuchAlgorithmException fatal) {
            throw new IllegalStateException(fatal);
        }
    }
}
