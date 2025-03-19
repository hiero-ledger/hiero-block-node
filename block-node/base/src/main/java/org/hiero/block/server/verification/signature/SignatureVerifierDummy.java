// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.verification.signature;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import javax.inject.Inject;
import org.hiero.block.common.hasher.HashingUtilities;

/**
 * A dummy implementation of {@link SignatureVerifier} that always returns true.
 */
public class SignatureVerifierDummy implements SignatureVerifier {

    /**
     * Constructs the dummy verifier.
     */
    @Inject
    // on actual impl we would need to provide the public key (aka LedgerID)
    public SignatureVerifierDummy() {}

    /**
     * Verifies the signature of a hash, for the dummy implementation this always returns true.
     *
     * @param hash the hash to verify
     * @param signature the signature to verify
     * @return true if the signature is valid, false otherwise
     */
    @Override
    public Boolean verifySignature(@NonNull Bytes hash, @NonNull Bytes signature) {
        // Dummy implementation
        // signature = is Hash384( BlockHash )
        return signature.equals(HashingUtilities.noThrowSha384HashOf(hash));
    }
}
