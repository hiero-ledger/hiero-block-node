// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification;

import static java.lang.System.Logger.Level.WARNING;

import com.hedera.cryptography.hints.HintsLibraryBridge;
import com.hedera.cryptography.tss.TSS;
import com.hedera.cryptography.wraps.WRAPSLibraryBridge;
import java.lang.module.ModuleDescriptor;
import java.util.concurrent.locks.ReentrantLock;

/// Gateway to the TSS native cryptography library (`com.hedera.cryptography`).
///
/// A TSS block proof is verified with `TSS.verifyTSS`, given the ledger id of the network, the block
/// signature and the signed block root hash. The ledger id and the signature are opaque to the Block
/// Node: the library checks their format itself and rejects malformed input with an
/// `IllegalArgumentException`.
///
/// The library embeds the key it verifies the WRAPS part of a signature with. [#warmUp] loads the native
/// code and that key once, at plugin start, so the first block does not pay for it and a library that
/// cannot verify in this environment is noticed immediately instead of failing every TSS block proof.
///
/// Every call into the library runs under a single process-wide [ReentrantLock] owned here, so TSS
/// verifications complete one at a time, in the order the verification sessions reach the library.
/// The library itself keeps no mutable state on its verification path, but the plugin's result ordering
/// relies on that completion order when a node starts without any verified block: the first session to
/// complete establishes where strict ordering begins.
public final class NativeVerificationLibrary {
    /// Logger for the gateway.
    private static final System.Logger LOGGER = System.getLogger(NativeVerificationLibrary.class.getName());

    /// Reported when the library module carries no version.
    private static final String UNKNOWN_VERSION = "unknown";

    /// The single lock guarding every call into the TSS library in this module.
    private static final ReentrantLock LOCK = new ReentrantLock();

    private NativeVerificationLibrary() {
        // no instances - this class only exposes the guarded native calls
    }

    /// Loads the native code and the embedded verification key of the TSS library, if they are not loaded
    /// yet, and reports whether TSS block proofs can be verified in this process. Later calls are cheap and
    /// return the same answer.
    ///
    /// The library loads its native code lazily, in `getInstance()`, while
    /// `WRAPSLibraryBridge.isVerificationSupported()` is static and loads the key through a native call:
    /// asking for the key before both instances exist fails with an `UnsatisfiedLinkError` that leaves the
    /// library unusable for the rest of the process. Both instances are therefore obtained first. The native
    /// code is extracted into the directory named by the `java.io.tmpdir` system property, so a failure here
    /// usually means that directory cannot be written to or mapped for execution. A failure is reported here
    /// once, with the stack trace; a library that reports the key as not loaded logs its own reason.
    ///
    /// @return `true` if TSS block proofs can be verified, `false` otherwise
    public static boolean warmUp() {
        boolean verificationSupported = false;
        LOCK.lock();
        try {
            HintsLibraryBridge.getInstance();
            WRAPSLibraryBridge.getInstance();
            verificationSupported = WRAPSLibraryBridge.isVerificationSupported();
        } catch (final RuntimeException | LinkageError e) {
            LOGGER.log(
                    WARNING,
                    "The TSS library %s could not be loaded (temp directory %s)"
                            .formatted(libraryVersion(), System.getProperty("java.io.tmpdir")),
                    e);
        } finally {
            LOCK.unlock();
        }
        return verificationSupported;
    }

    /// Verifies a TSS block signature over the given hash against the given ledger id, serialized
    /// against every other call into the library.
    ///
    /// @param ledgerId the ledger id of the network that signed the block
    /// @param signature the block signature from the block proof
    /// @param hashToVerify the block root hash the signature is over
    /// @return `true` if the signature verifies, `false` otherwise
    /// @throws IllegalArgumentException if the library considers any of the inputs malformed
    public static boolean verifyTss(final byte[] ledgerId, final byte[] signature, final byte[] hashToVerify) {
        LOCK.lock();
        try {
            return TSS.verifyTSS(ledgerId, signature, hashToVerify);
        } finally {
            LOCK.unlock();
        }
    }

    /// Returns the version of the TSS library module, for log messages.
    ///
    /// @return the module version, or `unknown` if the module carries none
    public static String libraryVersion() {
        final ModuleDescriptor descriptor = WRAPSLibraryBridge.class.getModule().getDescriptor();
        final String version;
        if (descriptor == null) {
            version = UNKNOWN_VERSION;
        } else {
            version = descriptor.rawVersion().orElse(UNKNOWN_VERSION);
        }
        return version;
    }
}
