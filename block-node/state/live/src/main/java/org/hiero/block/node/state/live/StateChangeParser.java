// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.live;

import static com.hedera.pbj.runtime.ProtoParserTools.readBool;
import static com.hedera.pbj.runtime.ProtoParserTools.readUint32;
import static com.swirlds.state.merkle.StateKeyUtils.kvKey;
import static com.swirlds.state.merkle.StateUtils.getStateKeyForSingleton;

import com.hedera.hapi.block.stream.output.MapChangeKey;
import com.hedera.hapi.block.stream.output.MapChangeValue;
import com.hedera.pbj.runtime.ProtoConstants;
import com.hedera.pbj.runtime.ProtoParserTools;
import com.hedera.pbj.runtime.ProtoWriterTools;
import com.hedera.pbj.runtime.io.ReadableSequentialData;
import com.hedera.pbj.runtime.io.buffer.BufferedData;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.virtualmap.VirtualMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * This class does the minimal protobuf parsing of state changes to get just the data it needs in a way that is forwards
 * and backwards compatible. So will handle state types being added or removed. It treats the state contents as binary
 * as they are the same protobuf binary format in state changes as they are stored in the state database.
 */
@SuppressWarnings("unused")
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
                        processStateChange(virtualMap, input);
                    }
                }
            }
        }
    }

    private static void processStateChange(@NonNull VirtualMap virtualMap, @NonNull ReadableSequentialData input) {
        int stateId = -1;
        while (input.hasRemaining()) {
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
                        processSingletonUpdateChange(virtualMap, stateId, input);
                    }
                }
                case 42 /* type=2 [MESSAGE] field=5 [map_update] */ -> {
                    final var messageLength = input.readVarInt(false);
                    if (messageLength > 0) {
                        processMapUpdateChange(virtualMap, stateId, input);
                    }
                }
                case 50 /* type=2 [MESSAGE] field=6 [map_delete] */ -> {
                    final var messageLength = input.readVarInt(false);
                    if (messageLength > 0) {
                        processMapDeleteChange(virtualMap, stateId, input);
                    }
                }
                case 58 /* type=2 [MESSAGE] field=7 [queue_push] */ -> {
                    final var messageLength = input.readVarInt(false);
                    if (messageLength > 0) {
                        processQueuePushChange(virtualMap, stateId, input);
                    }
                }
                case 66 /* type=2 [MESSAGE] field=8 [queue_pop] */ -> {
                    final var messageLength = input.readVarInt(false);
                    if (messageLength > 0) {
                        processQueuePopChange(virtualMap, stateId, input);
                    }
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
     */
    private static void processSingletonUpdateChange(@NonNull VirtualMap virtualMap, int stateId,
            @NonNull ReadableSequentialData input) {
        // SingletonUpdateChange has 1 single oneof, so we do not need a loop
        final int tag = input.readVarInt(false);
        // we also do not care which field it is as they are all wire type length encoded.
        final var messageLength = input.readVarInt(false);
        // get the singleton state key
        Bytes key = getStateKeyForSingleton(stateId);
        // create value bytes
        Bytes value = stateValueWrap(stateId, messageLength, input);
        // put into the virtual merkle map
        virtualMap.putBytes(key,value);
    }

    /**
     * Process a map update change from the input and apply it to the given virtual map.
     *
     * @param virtualMap the virtual map to apply the change to
     * @param stateId the state ID of the map
     * @param input the input positioned at the beginning of the map update change message
     */
    private static void processMapUpdateChange(@NonNull VirtualMap virtualMap, int stateId,
        @NonNull ReadableSequentialData input) {
        try {
            // read map key and value contents as Bytes
            Bytes mapKeyAsStateKey = null;
            Bytes mapValueAsStateValue = null;
            while (input.hasRemaining()) {
                final int tag = input.readVarInt(false);
                switch (tag) {
                    case 10 /* type=2 [MESSAGE] field=1 [key] */ -> {
                        final var messageLength = input.readVarInt(false);
                        final MapChangeKey value;
                        if (messageLength > 0) {
                            // MapChangeKey has a single oneof field; we do not care what field id it is
                            final int mapChangeKeyFieldTag = input.readVarInt(false);
                            final var mapChangeKeyFieldMessageLength = input.readVarInt(false);
                            mapKeyAsStateKey = kvKey(stateId, input.readBytes(mapChangeKeyFieldMessageLength));
                        }
                    }
                    case 18 /* type=2 [MESSAGE] field=2 [value] */ -> {
                        final var messageLength = input.readVarInt(false);
                        final MapChangeValue value;
                        if (messageLength > 0) {
                            // MapChangeValue has a single oneof field; we do not care what field id it is
                            final int mapChangeValueFieldTag = input.readVarInt(false);
                            final var mapChangeValueFieldMessageLength = input.readVarInt(false);
                            mapValueAsStateValue = stateValueWrap(stateId, mapChangeValueFieldMessageLength, input);
                        }
                    }
                    case 24 /* type=0 [BOOL] field=3 [identical] */ -> {
                        final var isIdentical = readBool(input); // not needed
                    }
                }
            }
            if (mapKeyAsStateKey == null || mapValueAsStateValue == null) {
                throw new RuntimeException("MapChangeKey or MapChangeValue missing");
            }
            // put into the virtual merkle map
            virtualMap.putBytes(mapKeyAsStateKey, mapValueAsStateValue);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void processMapDeleteChange(@NonNull VirtualMap virtualMap, int stateId,
        @NonNull ReadableSequentialData input) {

    }

    private static void processQueuePushChange(@NonNull VirtualMap virtualMap, int stateId,
        @NonNull ReadableSequentialData input) {

    }

    private static void processQueuePopChange(@NonNull VirtualMap virtualMap, int stateId,
        @NonNull ReadableSequentialData input) {

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
