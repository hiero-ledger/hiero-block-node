// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification.verifier;

import org.hiero.block.node.verification.session.SessionFailureType;

/// This interface defines a verifier for a [com.hedera.hapi.block.stream.BlockProof]
public interface ProofVerifier {
    /// Verify a [com.hedera.hapi.block.stream.BlockProof].
    /// @return `null` if verification is successful or a [SessionFailureType] otherwise
    SessionFailureType verify();
}
