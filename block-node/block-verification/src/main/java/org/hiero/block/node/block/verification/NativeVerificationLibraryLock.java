// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification;

/// Process-wide lock that serializes every access to the hinTS/WRAPS native
/// cryptography library (`com.hedera.cryptography`).
///
/// The native library is a JVM-wide singleton and is **not** thread-safe: the
/// `TSS.setAddressBook` javadoc states the client code is fully responsible for
/// thread-safety of that method with respect to calling `TSS.verifyTSS()`.
/// Verification sessions run concurrently — one per block, on a virtual-thread
/// executor — so without serialization a verify can race another verify or an
/// address-book / key update, corrupting the library's shared native state and
/// producing sporadic, load-dependent `BAD_BLOCK_PROOF` failures for signatures
/// that are individually valid.
///
/// Every native call in this module (`TSS.verifyTSS`, `TSS.setAddressBook`,
/// `WRAPSVerificationKey.setCurrentKey`) must hold this single lock. This
/// serializes verification throughput, an acceptable trade-off given the library
/// constraint: correctness takes precedence over throughput here.
public final class NativeVerificationLibraryLock {
    /// The shared monitor guarding every hinTS/WRAPS native call in this module.
    public static final Object LOCK = new Object();

    private NativeVerificationLibraryLock() {
        // no instances - this class only holds the shared lock
    }
}
