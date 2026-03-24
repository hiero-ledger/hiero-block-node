// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.validation;

import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.tools.days.model.AddressBookRegistry;
import org.hiero.block.tools.records.SigFileUtils;
import org.hiero.block.tools.records.model.parsed.ValidationException;

/**
 * Validates block signatures by verifying RSA signatures (for SignedRecordFileProof) or
 * checking non-empty TSS signatures (for SignedBlockProof).
 *
 * <p>For SignedRecordFileProof, at least 1/3 + 1 of address book nodes must have valid
 * RSA-SHA384 signatures. The signed hash is reconstructed from the block's RecordFileItem
 * and BlockHeader.
 *
 * <p>This is a stateless validation — no cross-block state is maintained.
 */
public final class SignatureValidation implements BlockValidation {

    /** The address book registry providing public keys for signature verification. */
    private final AddressBookRegistry addressBookRegistry;

    /**
     * Creates a new signature validation.
     *
     * @param addressBookRegistry the address book registry for public key lookups
     */
    public SignatureValidation(final AddressBookRegistry addressBookRegistry) {
        this.addressBookRegistry = addressBookRegistry;
    }

    @Override
    public String name() {
        return "Signatures";
    }

    @Override
    public String description() {
        return "Verifies RSA signatures (SignedRecordFileProof) or non-empty TSS signatures (SignedBlockProof)";
    }

    @Override
    public boolean requiresGenesisStart() {
        return false;
    }

    @Override
    public void validate(final BlockUnparsed block, final long blockNumber) throws ValidationException {
        // Find and selectively parse the block proof
        BlockProof blockProof = null;
        try {
            for (final BlockItemUnparsed item : block.blockItems()) {
                if (item.hasBlockProof()) {
                    blockProof = BlockProof.PROTOBUF.parse(item.blockProofOrThrow());
                    break;
                }
            }
        } catch (ParseException e) {
            throw new ValidationException("Block: " + blockNumber + " - Failed to parse BlockProof: " + e.getMessage());
        }
        if (blockProof == null) {
            throw new ValidationException("Block: " + blockNumber + " - No BlockProof found for signature validation");
        }

        if (blockProof.hasSignedRecordFileProof()) {
            validateSignedRecordFileProof(blockNumber, block, blockProof);
        } else if (blockProof.hasSignedBlockProof()) {
            // TSS verification not yet implemented — verify non-empty
            final Bytes blockSig = blockProof.signedBlockProofOrThrow().blockSignature();
            if (blockSig.length() == 0) {
                throw new ValidationException("Block: " + blockNumber + " - Empty TSS block signature");
            }
        } else {
            throw new ValidationException("Block: " + blockNumber + " - Unknown proof type: "
                    + blockProof.proof().kind());
        }
    }

    /**
     * Validates a SignedRecordFileProof by reconstructing the record file hash and verifying
     * RSA signatures from consensus nodes against the address book.
     *
     * @param blockNumber the block number
     * @param block the full block (unparsed)
     * @param blockProof the block proof containing the SignedRecordFileProof
     * @throws ValidationException if signature validation fails
     */
    private void validateSignedRecordFileProof(
            final long blockNumber, final BlockUnparsed block, final BlockProof blockProof) throws ValidationException {
        final var signedRecordFileProof = blockProof.signedRecordFileProofOrThrow();
        final var signatures = signedRecordFileProof.recordFileSignatures();

        if (signatures.isEmpty()) {
            throw new ValidationException("Block: " + blockNumber + " - No signatures in SignedRecordFileProof");
        }

        // Selectively extract signature data and parse BlockHeader from the block
        Bytes recordFileBytes = null;
        BlockHeader blockHeader = null;
        try {
            for (final BlockItemUnparsed item : block.blockItems()) {
                if (item.hasRecordFile()) {
                    recordFileBytes = item.recordFileOrThrow();
                }
                if (item.hasBlockHeader()) {
                    blockHeader = BlockHeader.PROTOBUF.parse(item.blockHeaderOrThrow());
                }
            }
        } catch (ParseException e) {
            throw new ValidationException(
                    "Block: " + blockNumber + " - Failed to parse BlockHeader: " + e.getMessage());
        }
        if (recordFileBytes == null || blockHeader == null) {
            throw new ValidationException(
                    "Block: " + blockNumber + " - Missing RecordFileItem or BlockHeader for signature verification");
        }

        // Extract signature data via wire-format navigation (no Transaction/TransactionRecord parsing)
        final int version = signedRecordFileProof.version();
        final SignatureDataExtractor.SignatureData sigData;
        final byte[] signedHash;
        try {
            sigData = SignatureDataExtractor.extract(recordFileBytes, version);
            signedHash = SignatureDataExtractor.computeSignedHash(version, blockHeader.hapiProtoVersion(), sigData);
        } catch (ParseException e) {
            throw new ValidationException(
                    "Block: " + blockNumber + " - Failed to extract signature data: " + e.getMessage());
        } catch (IOException e) {
            throw new ValidationException(
                    "Block: " + blockNumber + " - Failed to compute signed hash: " + e.getMessage());
        }
        final Instant blockTime = Instant.ofEpochSecond(sigData.creationTimeSeconds(), sigData.creationTimeNanos());

        // Get the address book for this block's timestamp
        final NodeAddressBook addressBook = addressBookRegistry.getAddressBookForBlock(blockTime);
        final int totalNodes = addressBook.nodeAddress().size();
        final int threshold = (totalNodes / 3) + 1;

        // Verify each signature, tracking unique nodes to avoid counting duplicates.
        // Early-exit once threshold is met — no need to verify remaining signatures.
        final Set<Long> validatedNodes = new HashSet<>();
        final List<String> diagnostics = new ArrayList<>();
        for (final var sig : signatures) {
            final long accountNum = AddressBookRegistry.accountIdForNode(sig.nodeId());
            try {
                final String pubKeyHex = AddressBookRegistry.publicKeyForNode(addressBook, 0, 0, accountNum);
                final String keySnippet =
                        pubKeyHex.length() > 16 ? pubKeyHex.substring(pubKeyHex.length() - 16) : pubKeyHex;
                if (SigFileUtils.verifyRsaSha384(
                        pubKeyHex, signedHash, sig.signaturesBytes().toByteArray())) {
                    validatedNodes.add(accountNum);
                    diagnostics.add(
                            "  node " + accountNum + " (id=" + sig.nodeId() + "): VERIFIED key=..." + keySnippet);
                    if (validatedNodes.size() >= threshold) break;
                } else {
                    diagnostics.add("  node " + accountNum + " (id=" + sig.nodeId() + "): FAILED key=..." + keySnippet);
                }
            } catch (Exception e) {
                diagnostics.add("  node " + accountNum + " (id=" + sig.nodeId() + "): ERROR "
                        + e.getClass().getSimpleName());
            }
        }

        if (validatedNodes.size() < threshold) {
            final StringBuilder detail = new StringBuilder();
            detail.append("Block: ").append(blockNumber);
            detail.append(" - Insufficient valid signatures: ");
            detail.append(validatedNodes.size()).append(" unique nodes/").append(totalNodes);
            detail.append(" verified, need ").append(threshold).append("/").append(totalNodes);
            detail.append("\n  blockTime=").append(blockTime);
            detail.append(", signatures=").append(signatures.size());
            detail.append("\n  Per-signature results:\n");
            diagnostics.forEach(d -> detail.append(d).append("\n"));
            throw new ValidationException(detail.toString());
        }
    }
}
