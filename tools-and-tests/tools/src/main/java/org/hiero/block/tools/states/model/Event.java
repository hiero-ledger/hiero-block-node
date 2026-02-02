// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import java.io.DataInputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import org.hiero.block.tools.states.utils.Utils;

/**
 * A serializable hashgraph event containing creator info, parent links, consensus data, and a hash.
 *
 * @param creatorId the ID of the node that created this event
 * @param creatorSeq the sequence number assigned by the creator
 * @param otherId the ID of the other-parent event's creator
 * @param otherSeq the sequence number of the other-parent event
 * @param timeCreated the instant this event was created
 * @param signature the creator's signature over this event
 * @param selfParent the self-parent event, or {@code null} if none
 * @param otherParent the other-parent event, or {@code null} if none
 * @param hash the hash of this event
 * @param timeReceived the instant this event was received
 * @param generation the generation number of this event
 * @param roundCreated the round in which this event was created
 * @param isWitness whether this event is a witness
 * @param isFameDecided whether the fame of this witness has been decided
 * @param isFamous whether this witness is famous
 * @param isConsensus whether this event has reached consensus
 * @param consensusTimestamp the consensus timestamp, or {@code null} if not yet consensus
 * @param roundReceived the round in which this event was received
 * @param consensusOrder the consensus order of this event
 * @param lastInRoundReceived whether this is the last event received in its round
 * @param abbreviated whether this event is in abbreviated form
 */
public record Event(
        long creatorId,
        long creatorSeq,
        long otherId,
        long otherSeq,
        Instant timeCreated,
        byte[] signature,
        Event selfParent,
        Event otherParent,
        Hash hash,
        Instant timeReceived,
        long generation,
        long roundCreated,
        boolean isWitness,
        boolean isFameDecided,
        boolean isFamous,
        boolean isConsensus,
        Instant consensusTimestamp,
        long roundReceived,
        long consensusOrder,
        boolean lastInRoundReceived,
        boolean abbreviated) {

    /**
     * Deserializes an event from the given stream.
     *
     * @param dis the stream to read from
     * @param eventsByCreatorSeq map of existing events by creator and sequence for resolving parent references
     * @return the deserialized event
     * @throws IOException if an I/O error occurs
     */
    public static Event readFrom(DataInputStream dis, Map<CreatorSeqPair, Event> eventsByCreatorSeq)
            throws IOException {
        int[] byteCount = new int[1];

        // info sent during a normal sync
        long creatorId = dis.readLong();
        long creatorSeq = dis.readLong();
        long otherId = dis.readLong();
        long otherSeq = dis.readLong();

        // event.transactions = SyncUtils.readByteArray2d(dis, byteCount);
        // event.sysTransaction = SyncUtils.readBooleanArray(dis, byteCount);

        Instant timeCreated = Utils.readInstant(dis, byteCount);
        byte[] signature = Utils.readByteArray(dis, byteCount);

        // find the parents if they exist
        Event selfParent = eventsByCreatorSeq.get(new CreatorSeqPair(creatorId, creatorSeq - 1));
        Event otherParent = eventsByCreatorSeq.get(new CreatorSeqPair(otherId, otherSeq));

        // other info
        Hash hash = Hash.readHash(dis);
        // event.selfHash = Hash.readHash(dis);
        // event.otherHash = Hash.readHash(dis);

        // event.creator = platform.getAddress(event.creatorId);
        creatorSeq = dis.readLong();
        Instant timeReceived = Utils.readInstant(dis, byteCount);
        long generation = dis.readLong();
        long roundCreated = dis.readLong();

        boolean isWitness = dis.readBoolean();
        boolean isFameDecided = dis.readBoolean();
        boolean isFamous = dis.readBoolean();
        boolean isConsensus = dis.readBoolean();

        Instant consensusTimestamp = Utils.readInstant(dis, byteCount);

        long roundReceived = dis.readLong();
        long consensusOrder = dis.readLong();
        boolean lastInRoundReceived = dis.readBoolean();

        boolean abbreviated = true;

        return new Event(
                creatorId,
                creatorSeq,
                otherId,
                otherSeq,
                timeCreated,
                signature,
                selfParent,
                otherParent,
                hash,
                timeReceived,
                generation,
                roundCreated,
                isWitness,
                isFameDecided,
                isFamous,
                isConsensus,
                consensusTimestamp,
                roundReceived,
                consensusOrder,
                lastInRoundReceived,
                abbreviated);
    }

    /**
     * Returns the raw hash bytes of this event.
     *
     * @return the hash bytes
     */
    public byte[] getHash() {
        return hash.hash(); // TODO for now we will just return the read hash, but we might want to calculate it later
    }
}
