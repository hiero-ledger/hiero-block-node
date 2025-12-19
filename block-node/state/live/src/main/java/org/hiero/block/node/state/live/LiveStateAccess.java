package org.hiero.block.node.state.live;

import com.hedera.hapi.block.stream.output.StateIdentifier;
import com.hedera.hapi.platform.state.QueueState;
import com.hedera.hapi.platform.state.SingletonType;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;

/**
 * API to access the latest state. Implements read-only access to the latest state data. Implementors of this interface
 * should be thread safe. There are no transaction semantics, the data returned is simply the latest readable state at
 * the time of the call. Which means that data may change between calls. So if you get queue state and then peek at
 * head/tail/index, the queue may have changed in between those calls.
 */
@SuppressWarnings("unused")
public interface LiveStateAccess {

    /**
     * Returns the block number of the latest readable state.
     *
     * @return the current block number of the state
     */
    long blockNumber();

    /**
     * Get a map value from the latest state version
     *
     * @param stateID the state ID, from {@link StateIdentifier} enum, eg. {@link StateIdentifier#STATE_ID_ENTITY_ID}
     * @param key the binary protobuf encoded key
     * @return the binary protobuf encoded value, null if not found
     * @throws IllegalArgumentException if the stateID is not valid
     * @see StateIdentifier
     */
    Bytes mapValue(final int stateID, @NonNull final Bytes key);

    /**
     * Get a singleton value from the latest state version
     *
     * @param singletonID the singleton ID, from SingletonType enum, eg. SingletonType.ENTITYIDSERVICE_I_ENTITY_ID
     * @return the binary protobuf encoded value
     * @throws IllegalArgumentException if the singletonID is not valid
     * @see SingletonType
     */
    Bytes singleton(final int singletonID);

    /**
     * Get a queue state from the latest state version
     *
     * @param stateID the state ID, from {@link StateIdentifier} enum, eg. {@link StateIdentifier#STATE_ID_TRANSACTION_RECEIPTS}
     * @return the queue state, which has the indexes of head and tail
     * @throws IllegalArgumentException if the stateID is not valid or not a queue type
     * @see StateIdentifier
     */
    QueueState queueState(final int stateID);

    /**
     * Peek at head element in a queue from the latest state version
     *
     * @param stateID the state ID, from {@link StateIdentifier} enum, eg. {@link StateIdentifier#STATE_ID_TRANSACTION_RECEIPTS}
     * @return the binary protobuf encoded value at head
     * @throws IllegalArgumentException if the stateID is not valid or not a queue type
     * @see StateIdentifier
     */
    Bytes queuePeekHead(final int stateID);

    /**
     * Peek at tail element in a queue from the latest state version
     *
     * @param stateID the state ID, from {@link StateIdentifier} enum, eg. {@link StateIdentifier#STATE_ID_TRANSACTION_RECEIPTS}
     * @return the binary protobuf encoded value at tail
     * @throws IllegalArgumentException if the stateID is not valid or not a queue type
     * @see StateIdentifier
     */
    Bytes queuePeekTail(final int stateID);

    /**
     * Peek at element at index in a queue from the latest state version. Index has to be between the head and the tail
     * inclusive. To find the head and tail indexes use {@link #queueState(int)}
     *
     * @param stateID the state ID, from {@link StateIdentifier} enum, eg. {@link StateIdentifier#STATE_ID_TRANSACTION_RECEIPTS}
     * @param index the index to peek at
     * @return the binary protobuf encoded value at index
     * @throws IllegalArgumentException if the stateID is not valid or not a queue type
     * @see StateIdentifier
     */
    Bytes queuePeek(final int stateID, final int index);

    /**
     * Get all elements in a queue from the latest state version as a list. The list will be ordered from head to tail.
     *
     * @param stateID the state ID, from {@link StateIdentifier} enum, eg. {@link StateIdentifier#STATE_ID_TRANSACTION_RECEIPTS}
     * @return the list of binary protobuf encoded values in the queue
     * @throws IllegalArgumentException if the stateID is not valid or not a queue type
     * @see StateIdentifier
     */
    List<Bytes> queueAsList(final int stateID);
}
