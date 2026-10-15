// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.verifier;

import java.security.NoSuchAlgorithmException;
import org.hiero.block.node.block.verification.session.SessionFailureType;

/// This interface defines a verifier for a [com.hedera.hapi.block.stream.BlockProof]
public interface ProofVerifier {
    /// Verify a [com.hedera.hapi.block.stream.BlockProof].
    /// @return `null` if verification is successful or a [SessionFailureType] otherwise
    /// @throws NoSuchAlgorithmException if the Java runtime provides no implementation of an algorithm the
    ///     verification needs
    SessionFailureType verify() throws NoSuchAlgorithmException;
}
