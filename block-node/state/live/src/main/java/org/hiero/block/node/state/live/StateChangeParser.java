// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.live;

import static com.hedera.pbj.runtime.ProtoParserTools.readUint32;
import static com.swirlds.state.merkle.StateKeyUtils.kvKey;
import static com.swirlds.state.merkle.StateKeyUtils.queueKey;
import static com.swirlds.state.merkle.StateUtils.getStateKeyForSingleton;

import com.hedera.hapi.platform.state.QueueState;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.ProtoConstants;
import com.hedera.pbj.runtime.ProtoParserTools;
import com.hedera.pbj.runtime.ProtoWriterTools;
import com.hedera.pbj.runtime.io.ReadableSequentialData;
import com.hedera.pbj.runtime.io.buffer.BufferedData;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.virtualmap.VirtualMap;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * This class does the minimal protobuf parsing of state changes to get just the data it needs in a way that is forwards
 * and backwards compatible. So will handle state types being added or removed. It treats the state contents as binary
 * as they are the same protobuf binary format in state changes as they are stored in the state database.
 */
@SuppressWarnings({"unused", "RedundantLabeledSwitchRuleCodeBlock"})
public final class StateChangeParser {
    public static void applyStateChanges(@NonNull VirtualMap virtualMap, @NonNull Bytes stateChangesBytes) {
        ReadableSequentialData input = stateChangesBytes.toReadableSequentialData();
        // -- PARSE LOOP StateChanges ---------------------------------------------
        while (input.hasRemaining()) {
            // Given the wire type and the field type, parse the field
            switch (input.readVarInt(false)) {
                case 10 /* type=2 [MESSAGE] field=1 [consensus_timestamp] */ -> {
                    final var messageLength = input.readVarInt(false);
                    // we do not care about the timestamp field, so skip
                    input.skip(messageLength);
                }
                case 18 /* type=2 [MESSAGE] field=2 [state_changes] */ -> {
                    // this is "repeated" field so we will get more than one call
                    final var messageLength = input.readVarInt(false);
                    if (messageLength > 0) {
                        final long endPosition = input.position() + messageLength;
                        processStateChange(virtualMap, input, endPosition);
                    }
                }
            }
        }
    }

    private static void processStateChange(
            @NonNull VirtualMap virtualMap, @NonNull ReadableSequentialData input, long endPosition) {
        int stateId = -1;
        while (input.position() < endPosition) {
            switch (input.readVarInt(false)) {
                case 8 /* type=0 [UINT32] field=1 [state_id] */ -> {
                    stateId = readUint32(input);
                }
                case 18 /* type=2 [MESSAGE] field=2 [state_add] */ -> {
                    // we do not care about the state_add field, so skip
                    final var messageLength = input.readVarInt(false);
                    input.skip(messageLength);
                }
                case 26 /* type=2 [MESSAGE] field=3 [state_remove] */ -> {
                    // we do not care about the state_remove field, so skip
                    final var messageLength = input.readVarInt(false);
                    input.skip(messageLength);
                }
                case 34 /* type=2 [MESSAGE] field=4 [singleton_update] */ -> {
                    final var messageLength = input.readVarInt(false);
                    if (messageLength > 0) {
                        final long msgEndPosition = input.position() + messageLength;
                        processSingletonUpdateChange(virtualMap, stateId, input, msgEndPosition);
                    }
                }
                case 42 /* type=2 [MESSAGE] field=5 [map_update] */ -> {
                    final var messageLength = input.readVarInt(false);
                    if (messageLength > 0) {
                        final long msgEndPosition = input.position() + messageLength;
                        processMapUpdateChange(virtualMap, stateId, input, msgEndPosition);
                    }
                }
                case 50 /* type=2 [MESSAGE] field=6 [map_delete] */ -> {
                    final var messageLength = input.readVarInt(false);
                    if (messageLength > 0) {
                        final long msgEndPosition = input.position() + messageLength;
                        processMapDeleteChange(virtualMap, stateId, input, msgEndPosition);
                    }
                }
                case 58 /* type=2 [MESSAGE] field=7 [queue_push] */ -> {
                    final var messageLength = input.readVarInt(false);
                    if (messageLength > 0) {
                        final long msgEndPosition = input.position() + messageLength;
                        processQueuePushChange(virtualMap, stateId, input, msgEndPosition);
                    }
                }
                case 66 /* type=2 [MESSAGE] field=8 [queue_pop] */ -> {
                    final var messageLength = input.readVarInt(false);
                    // QueuePopChange has no fields, so messageLength will be 0, but we still need to process it
                    processQueuePopChange(virtualMap, stateId);
                }
            }
        }
    }

    /**
     * Process a singleton update change from the input and apply it to the given virtual map.
     *
     * @param virtualMap the virtual map to apply the change to
     * @param stateId the state ID of the singleton
     * @param input the input positioned at the beginning of the singleton update change message
     * @param endPosition the position in input at which this message ends
     */
    private static void processSingletonUpdateChange(
            @NonNull VirtualMap virtualMap, int stateId, @NonNull ReadableSequentialData input, long endPosition) {
        // SingletonUpdateChange has 1 single oneof, so we do not need a loop
        final int tag = input.readVarInt(false);
        // we also do not care which field it is as they are all wire type length encoded.
        final var messageLength = input.readVarInt(false);
        // get the singleton state key
        Bytes key = getStateKeyForSingleton(stateId);
        // create value bytes
        Bytes value = stateValueWrap(stateId, messageLength, input);
        // put into the virtual merkle map
        virtualMap.putBytes(key, value);
    }

    /**
     * Process a map update change from the input and apply it to the given virtual map.
     *
     * @param virtualMap the virtual map to apply the change to
     * @param stateId the state ID of the map
     * @param input the input positioned at the beginning of the map update change message
     * @param endPosition the position in input at which this message ends
     */
    private static void processMapUpdateChange(
            @NonNull VirtualMap virtualMap, int stateId, @NonNull ReadableSequentialData input, long endPosition) {
        // read map key and value contents as Bytes
        Bytes mapKeyAsStateKey = null;
        Bytes mapValueAsStateValue = null;
        while (input.position() < endPosition) {
            final int tag = input.readVarInt(false);
            switch (tag) {
                case 10 /* type=2 [MESSAGE] field=1 [key] */ -> {
                    final var messageLength = input.readVarInt(false);
                    if (messageLength > 0) {
                        // MapChangeKey has a single oneof field; extract the field number to determine type
                        final int mapChangeKeyFieldTag = input.readVarInt(false);
                        final var mapChangeKeyFieldMessageLength = input.readVarInt(false);
                        // Field number from tag (lower 3 bits are wire type)
                        int fieldNumber = mapChangeKeyFieldTag >>> 3;

                        // proto_bytes_key (field 6) and proto_string_key (field 7) are wrapper types
                        // that contain an inner message with the actual bytes/string at field 1
                        if (fieldNumber == 6 || fieldNumber == 7) {
                            // Wrapper type - read through inner tag and length to get actual bytes
                            final int innerTag = input.readVarInt(false);
                            final int innerLength = input.readVarInt(false);
                            mapKeyAsStateKey = kvKey(stateId, input.readBytes(innerLength));
                        } else {
                            // Other key types (account_id_key at field 2, etc.) - read as-is
                            mapKeyAsStateKey = kvKey(stateId, input.readBytes(mapChangeKeyFieldMessageLength));
                        }
                    }
                }
                case 18 /* type=2 [MESSAGE] field=2 [value] */ -> {
                    final var messageLength = input.readVarInt(false);
                    if (messageLength > 0) {
                        // MapChangeValue has a single oneof field; we do not care what field id it is
                        final int mapChangeValueFieldTag = input.readVarInt(false);
                        final var mapChangeValueFieldMessageLength = input.readVarInt(false);
                        mapValueAsStateValue = stateValueWrap(stateId, mapChangeValueFieldMessageLength, input);
                    }
                }
                case 24 /* type=0 [BOOL] field=3 [identical] */ -> input.readVarInt(false); // not needed
            }
        }
        if (mapKeyAsStateKey == null || mapValueAsStateValue == null) {
            throw new RuntimeException("MapChangeKey or MapChangeValue missing");
        }
        // put into the virtual merkle map
        virtualMap.putBytes(mapKeyAsStateKey, mapValueAsStateValue);
    }

    /**
     * Process a map-delete-change from the input and apply it to the given virtual map.
     *
     * @param virtualMap the virtual map to apply the change to
     * @param stateId the state ID of the map
     * @param input the input positioned at the beginning of the map delete change message
     * @param endPosition the position in input at which this message ends
     */
    private static void processMapDeleteChange(
            @NonNull VirtualMap virtualMap, int stateId, @NonNull ReadableSequentialData input, long endPosition) {
        Bytes mapKeyAsStateKey = null;
        while (input.position() < endPosition) {
            final int tag = input.readVarInt(false);
            if (tag == 10 /* type=2 [MESSAGE] field=1 [key] */) {
                final var messageLength = input.readVarInt(false);
                if (messageLength > 0) {
                    // MapChangeKey has a single oneof field; extract the field number to determine type
                    final int mapChangeKeyFieldTag = input.readVarInt(false);
                    final var mapChangeKeyFieldMessageLength = input.readVarInt(false);
                    // Field number from tag (lower 3 bits are wire type)
                    int fieldNumber = mapChangeKeyFieldTag >>> 3;

                    // proto_bytes_key (field 6) and proto_string_key (field 7) are wrapper types
                    if (fieldNumber == 6 || fieldNumber == 7) {
                        // Wrapper type - read through inner tag and length to get actual bytes
                        final int innerTag = input.readVarInt(false);
                        final int innerLength = input.readVarInt(false);
                        mapKeyAsStateKey = kvKey(stateId, input.readBytes(innerLength));
                    } else {
                        // Other key types (account_id_key at field 2, etc.) - read as-is
                        mapKeyAsStateKey = kvKey(stateId, input.readBytes(mapChangeKeyFieldMessageLength));
                    }
                }
            }
        }
        if (mapKeyAsStateKey == null) {
            throw new RuntimeException("MapChangeKey missing in MapDeleteChange");
        }
        // remove from the virtual merkle map
        virtualMap.remove(mapKeyAsStateKey);
    }

    /**
     * Process a queue, push change from the input and apply it to the given virtual map.
     * This reads the current queue state (head/tail), adds the new element at the tail position,
     * and updates the queue state with the incremented tail.
     *
     * @param virtualMap the virtual map to apply the change to
     * @param stateId the state ID of the queue
     * @param input the input positioned at the beginning of the queue push change message
     * @param endPosition the position in input at which this message ends
     */
    private static void processQueuePushChange(
            @NonNull VirtualMap virtualMap, int stateId, @NonNull ReadableSequentialData input, long endPosition) {
        try {
            // Read the current queue state
            Bytes queueStateKey = getStateKeyForSingleton(stateId);
            Bytes existingQueueStateBytes = virtualMap.getBytes(queueStateKey);
            QueueState queueState = existingQueueStateBytes != null
                    ? QueueState.PROTOBUF.parse(existingQueueStateBytes)
                    : new QueueState(1, 1);

            // Parse the QueuePushChange to get the element value
            // QueuePushChange has a value oneof with options:
            //   field 1: proto_bytes_element (wrapper message with bytes field 1)
            //   field 2: proto_string_element (wrapper message with string field 1)
            //   field 3: transaction_receipt_entries_element (direct message)
            Bytes elementValue = null;
            while (input.position() < endPosition) {
                final int tag = input.readVarInt(false);
                final var messageLength = input.readVarInt(false);
                if (messageLength > 0) {
                    switch (tag) {
                        case 10 /* proto_bytes_element */, 18 /* proto_string_element */ -> {
                            // These are wrapper messages with field 1 containing the actual value
                            final int innerTag = input.readVarInt(false);
                            final int innerLength = input.readVarInt(false);
                            elementValue = stateValueWrap(stateId, innerLength, input);
                        }
                        case 26 /* transaction_receipt_entries_element */ -> {
                            // This is a direct message - the whole message is the value
                            elementValue = stateValueWrap(stateId, messageLength, input);
                        }
                        default -> input.skip(messageLength);
                    }
                }
            }
            if (elementValue == null) {
                throw new RuntimeException("No value found in QueuePushChange");
            }
            // Create a queue element key at the current tail position
            Bytes queueElementKey = queueKey(stateId, queueState.tail());
            // Put element into virtual map
            virtualMap.putBytes(queueElementKey, elementValue);
            // Update queue state (increment tail)
            Bytes newQueueStateBytes =
                    QueueState.PROTOBUF.toBytes(new QueueState(queueState.head(), queueState.tail() + 1));
            virtualMap.putBytes(queueStateKey, newQueueStateBytes);
        } catch (ParseException e) {
            throw new RuntimeException("Failed to parse QueueState.", e);
        }
    }

    /**
     * Process a queue pop change and apply it to the given virtual map.
     * This reads the current queue state (head/tail), removes the element at the head position,
     * and updates the queue state with the incremented head.
     *
     * @param virtualMap the virtual map to apply the change to
     * @param stateId the state ID of the queue
     */
    private static void processQueuePopChange(@NonNull VirtualMap virtualMap, int stateId) {
        // QueuePopChange has no fields, so we don't need any input
        try {
            // Read the current queue state
            Bytes queueStateKey = getStateKeyForSingleton(stateId);
            Bytes existingQueueStateBytes = virtualMap.getBytes(queueStateKey);
            if (existingQueueStateBytes == null) {
                throw new RuntimeException("Cannot pop from queue - queue state not found for stateId: " + stateId);
            }
            QueueState queueState = QueueState.PROTOBUF.parse(existingQueueStateBytes);
            // Check if the queue is empty
            if (queueState.head() >= queueState.tail()) {
                throw new RuntimeException("Cannot pop from empty queue for stateId: " + stateId);
            }
            // Remove element at head position
            Bytes queueElementKey = queueKey(stateId, queueState.head());
            virtualMap.remove(queueElementKey);
            // Update queue state (increment head)
            Bytes newQueueStateBytes =
                    QueueState.PROTOBUF.toBytes(new QueueState(queueState.head() + 1, queueState.tail()));
            virtualMap.putBytes(queueStateKey, newQueueStateBytes);
        } catch (ParseException e) {
            throw new RuntimeException("Failed to parse QueueState.", e);
        }
    }

    /**
     * Take an input positioned at the beginning of a state value. Read the value and wrap it to be binary
     * compatible with the protobuf object com.hedera.hapi.platform.state.StateValue.
     *
     * @param stateId the state ID of the value
     * @param valueLength the length of the value in protobuf binary format
     * @param input the input positioned at the beginning of the value, ready to read
     * @return Bytes representing StateValue in protobuf binary format, containing the given value for state id
     */
    private static Bytes stateValueWrap(int stateId, int valueLength, ReadableSequentialData input) {
        // compute field tag
        final int tag = (stateId << ProtoParserTools.TAG_FIELD_OFFSET) | ProtoConstants.WIRE_TYPE_DELIMITED.ordinal();
        final int tagSize = ProtoWriterTools.sizeOfVarInt32(tag);
        // compute value length encoded size
        final int valueSize = ProtoWriterTools.sizeOfVarInt32(valueLength);
        // compute total size and allocate a buffer
        final int totalSize = tagSize + valueSize + valueLength;
        final byte[] buffer = new byte[totalSize];
        final BufferedData out = BufferedData.wrap(buffer);
        // write
        out.writeVarInt(tag, false);
        out.writeVarInt(valueLength, false);
        input.readBytes(out); // limit is set to the correct size already
        // return buffer as Bytes
        return Bytes.wrap(buffer);
    }
}
