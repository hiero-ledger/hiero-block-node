// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.verification;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A record representing the result of a block verification.
 *
 * @param blockNumber the block number
 * @param blockHash the block hash
 * @param status the verification status
 */
public record VerificationResult(long blockNumber, @NonNull Bytes blockHash, @NonNull BlockVerificationStatus status) {}
