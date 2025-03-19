// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.verification.signature;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * An interface for verifying signatures.
 */
public interface SignatureVerifier {

    /**
     * Verifies the signature of a hash.
     *
     * @param hash the hash to verify
     * @param signature the signature to verify
     * @return true if the signature is valid, false otherwise
     */
    Boolean verifySignature(@NonNull Bytes hash, @NonNull Bytes signature);
}
