// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.model;

import com.hedera.hapi.block.stream.RecordFileSignature;
import com.hedera.hapi.node.base.NodeAddressBook;
import java.util.List;
import org.hiero.block.tools.records.model.parsed.ParsedRecordBlock;

/**
 * Parsed record block with RSA signatures already verified and pre-converted to
 * {@link RecordFileSignature} protobuf objects, ready for conversion to block stream format
 * without further cryptographic work.
 *
 * <p>Instances are produced by Stage 1 of the four-stage pipeline in
 * {@code ToWrappedBlocksCommand} (parse + RSA-verify thread pool) and consumed by
 * Stage 2 (convert thread), which calls
 * {@link org.hiero.block.tools.records.model.parsed.RecordBlockConverter#toBlock(PreVerifiedBlock,
 * long, byte[], byte[], org.hiero.block.tools.blocks.AmendmentProvider)}.
 *
 * @param recordBlock the fully parsed record block
 * @param addressBook the node address book active at the block's consensus time
 * @param verifiedSignatures RSA-verified signatures already mapped to the wire format,
 *        ready to be embedded directly in the {@link com.hedera.hapi.block.stream.BlockProof}
 */
public record PreVerifiedBlock(
        ParsedRecordBlock recordBlock, NodeAddressBook addressBook, List<RecordFileSignature> verifiedSignatures) {}
