// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.common.hasher;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.ByteBuffer;

/**
 * Holds the input and output hashes of a list of block items.
 *
 * @param inputHashes the input hashes
 * @param outputHashes the output hashes
 */
public record Hashes(@NonNull ByteBuffer inputHashes, @NonNull ByteBuffer outputHashes) {}
