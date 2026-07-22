// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.signing;

import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;

/// Produces real, verifiable [BlockProof]s for tests, plus the [VerificationMaterial] a matching
/// verifier must be provisioned with.
///
/// Implementations are immutable and safe to reuse across many blocks: all per-era key material is
/// generated once at construction, and [#signBlockProof] performs only the per-block signing work.
public interface BlockSigner {

    /// Produces a signed [BlockProof] for `blockNumber` over `signedContent`.
    ///
    /// The meaning of `signedContent` is scheme-specific:
    ///   - TSS ([TssBlockSigner]): the block root hash, exactly as
    ///     `HashingUtilities.computeFinalBlockHash(...)` produces it.
    ///   - RSA ([RsaBlockSigner]): the verbatim `record_file_contents` bytes; the signer applies the
    ///     version 6 payload transform internally.
    ///
    /// @param blockNumber the block number to embed in the proof
    /// @param signedContent the scheme-specific content to sign
    /// @return a [BlockProof] the paired verifier will accept once provisioned with [#verificationMaterial]
    @NonNull
    BlockProof signBlockProof(long blockNumber, @NonNull Bytes signedContent);

    /// The material a verifier must hold to accept proofs from this signer.
    @NonNull
    VerificationMaterial verificationMaterial();
}
