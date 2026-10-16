// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.validation;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.OneofDescriptor;
import com.google.protobuf.WireFormat;
import com.hedera.hapi.block.stream.protoc.Block;
import java.io.IOException;
import org.hiero.block.tools.records.model.parsed.ValidationException;

/**
 * Checks the raw protobuf encoding of a block for fields that occur more than once where the schema allows only
 * one: a non-repeated message field set twice, or more than one member of the same oneof.
 *
 * <p>PBJ keeps only the last occurrence of such a field and silently drops the others, so a duplicate means the
 * block parses differently than it was encoded. This must run on the decompressed bytes, before any PBJ parse,
 * because {@link org.hiero.block.internal.BlockUnparsed} has already dropped duplicates at the block item level.
 *
 * <p>Repeated fields, scalar and {@code bytes} fields outside a oneof, unknown fields and messages nested inside
 * {@code bytes} fields are not checked.
 */
public final class ProtobufDuplicateFieldValidator {

    /** Name reported as the failed validation. */
    public static final String NAME = "Duplicate Fields";

    private static final Descriptor BLOCK = Block.getDescriptor();

    private ProtobufDuplicateFieldValidator() {}

    /**
     * Walks the given block bytes and fails on the first field that occurs more than once where only one is allowed.
     *
     * @param blockBytes the decompressed protobuf encoding of a {@code Block}
     * @param blockNumber the block number, used in error messages
     * @throws ValidationException if a duplicate is found or the bytes are not valid protobuf
     */
    public static void validate(final byte[] blockBytes, final long blockNumber) throws ValidationException {
        try {
            checkMessage(CodedInputStream.newInstance(blockBytes), BLOCK, "Block", 0, blockNumber);
        } catch (final IOException e) {
            throw new ValidationException("Block: " + blockNumber + " - Malformed protobuf: " + e.getMessage());
        }
    }

    private static void checkMessage(
            final CodedInputStream in,
            final Descriptor type,
            final String path,
            final int depth,
            final long blockNumber)
            throws IOException, ValidationException {
        if (depth > ProtobufParsingConstants.MAX_DEPTH) {
            throw new ValidationException("Block: " + blockNumber + " - Nesting deeper than "
                    + ProtobufParsingConstants.MAX_DEPTH + " at " + path);
        }
        // Allocated on first use and indexed by descriptor position, so leaf messages allocate nothing.
        int[] counts = null; // field index -> occurrences of that message field so far
        FieldDescriptor[] oneofMembers = null; // oneof index -> member seen so far
        int tag = in.readTag();
        while (tag != 0) {
            final FieldDescriptor field = type.findFieldByNumber(WireFormat.getTagFieldNumber(tag));
            boolean descended = false;
            if (field != null) {
                // Any oneof member counts, whatever its type: a second member always replaces the first.
                final OneofDescriptor oneof = field.getRealContainingOneof();
                if (oneof != null) {
                    if (oneofMembers == null) {
                        oneofMembers = new FieldDescriptor[type.getOneofCount()];
                    }
                    final FieldDescriptor previous = oneofMembers[oneof.getIndex()];
                    if (previous != null) {
                        throw new ValidationException("Block: " + blockNumber + " - oneof '" + oneof.getName()
                                + "' set more than once (" + previous.getName() + ", " + field.getName() + ") at "
                                + path);
                    }
                    oneofMembers[oneof.getIndex()] = field;
                }
                if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE
                        && WireFormat.getTagWireType(tag) == WireFormat.WIRETYPE_LENGTH_DELIMITED) {
                    if (counts == null) {
                        counts = new int[type.getFields().size()];
                    }
                    final int count = ++counts[field.getIndex()];
                    final String childPath = field.isRepeated()
                            ? path + "." + field.getName() + "[" + (count - 1) + "]"
                            : path + "." + field.getName();
                    if (!field.isRepeated() && count > 1) {
                        throw new ValidationException(
                                "Block: " + blockNumber + " - Non-repeated field set more than once: " + childPath);
                    }
                    final int oldLimit = in.pushLimit(in.readRawVarint32());
                    checkMessage(in, field.getMessageType(), childPath, depth + 1, blockNumber);
                    in.popLimit(oldLimit);
                    descended = true;
                }
            }
            if (!descended) {
                in.skipField(tag);
            }
            tag = in.readTag();
        }
    }
}
