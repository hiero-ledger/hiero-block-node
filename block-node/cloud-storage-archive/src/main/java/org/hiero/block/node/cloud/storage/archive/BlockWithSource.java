// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.archive;

import static java.util.Objects.requireNonNull;

import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.spi.blockmessaging.BlockSource;

/// Pairs a [BlockUnparsed] with its [BlockSource] for transfer through the upload queue shared
/// between [CloudStorageArchivePlugin] and [BlockUploadTask].
///
/// A `null` source is normalised to [BlockSource#UNKNOWN] at construction time so consumers never
/// need to guard against `null`.
record BlockWithSource(BlockUnparsed block, BlockSource source) {
    BlockWithSource {
        requireNonNull(block);
        source = source != null ? source : BlockSource.UNKNOWN;
    }
}
