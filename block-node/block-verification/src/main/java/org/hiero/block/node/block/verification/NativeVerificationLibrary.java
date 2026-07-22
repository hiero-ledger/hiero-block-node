// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification;

import com.hedera.cryptography.tss.TSS;
import com.hedera.cryptography.wraps.WRAPSVerificationKey;
import java.util.concurrent.locks.ReentrantLock;

/// Serializing gateway to the hinTS/WRAPS native cryptography library (`com.hedera.cryptography`).
///
/// The native library is a JVM-wide singleton and is **not** thread-safe: the `TSS.setAddressBook`
/// javadoc states the client code is fully responsible for thread-safety of that method with respect
/// to calling `TSS.verifyTSS()`. Verification sessions run concurrently — one per block, on a
/// virtual-thread executor — and empirically even two concurrent `verifyTSS` calls (with no
/// intervening key update) corrupt the library's shared native state, producing sporadic,
/// load-dependent `BAD_BLOCK_PROOF` failures for signatures that are individually valid.
///
/// Every native call in this module therefore runs under a single process-wide [ReentrantLock] owned
/// here, so callers never manage locking themselves. This serializes verification throughput, an
/// acceptable trade-off given the library constraint: correctness takes precedence over throughput.
/// The lock cost is negligible next to the native verify it guards.
public final class NativeVerificationLibrary {
    /// The single lock guarding every hinTS/WRAPS native call in this module.
    private static final ReentrantLock LOCK = new ReentrantLock();

    private NativeVerificationLibrary() {
        // no instances - this class only exposes the guarded native calls
    }

    /// Verifies a TSS block signature, serialized against every other native call.
    ///
    /// Handles both the genesis (Schnorr aggregate) and post-genesis (WRAPS) proof paths;
    /// signatures without a recognized proof suffix are rejected by the library.
    public static boolean verifyTss(final byte[] ledgerId, final byte[] signature, final byte[] hashToVerify) {
        LOCK.lock();
        try {
            return TSS.verifyTSS(ledgerId, signature, hashToVerify);
        } finally {
            LOCK.unlock();
        }
    }

    /// Installs the address book and WRAPS verification key, serialized against every other native
    /// call. Both mutate the shared native state that [#verifyTss] reads, so they are applied together
    /// under the lock.
    public static void updateVerificationKeys(
            final byte[][] publicKeys, final long[] weights, final long[] nodeIds, final byte[] wrapsVerificationKey) {
        LOCK.lock();
        try {
            TSS.setAddressBook(publicKeys, weights, nodeIds);
            WRAPSVerificationKey.setCurrentKey(wrapsVerificationKey);
        } finally {
            LOCK.unlock();
        }
    }
}
