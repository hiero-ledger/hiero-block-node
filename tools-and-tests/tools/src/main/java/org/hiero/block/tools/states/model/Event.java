package org.hiero.block.tools.states.model;

import org.hiero.block.tools.states.FCDataInputStream;
import org.hiero.block.tools.states.SyncUtils;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;

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
        boolean abbreviated
) {

    public static Event readFrom(FCDataInputStream dis,  Map<CreatorSeqPair, Event> eventsByCreatorSeq) throws IOException {
        int[] byteCount = new int[1];

        // info sent during a normal sync
        long creatorId = dis.readLong();
        long creatorSeq = dis.readLong();
        long otherId = dis.readLong();
        long otherSeq = dis.readLong();

        // event.transactions = SyncUtils.readByteArray2d(dis, byteCount);
        // event.sysTransaction = SyncUtils.readBooleanArray(dis, byteCount);

        Instant timeCreated = SyncUtils.readInstant(dis, byteCount);
        byte[] signature = SyncUtils.readByteArray(dis, byteCount);

        // find the parents if they exist
        Event selfParent = eventsByCreatorSeq
                .get(new CreatorSeqPair(creatorId, creatorSeq - 1));
        Event otherParent = eventsByCreatorSeq
                .get(new CreatorSeqPair(otherId, otherSeq));

        // other info
        Hash hash = Hash.readHash(dis);
        // event.selfHash = Hash.readHash(dis);
        // event.otherHash = Hash.readHash(dis);

        // event.creator = platform.getAddress(event.creatorId);
        creatorSeq = dis.readLong();
        Instant timeReceived = SyncUtils.readInstant(dis, byteCount);
        long generation = dis.readLong();
        long roundCreated = dis.readLong();

        boolean isWitness = dis.readBoolean();
        boolean isFameDecided = dis.readBoolean();
        boolean isFamous = dis.readBoolean();
        boolean isConsensus = dis.readBoolean();

        Instant consensusTimestamp = SyncUtils.readInstant(dis, byteCount);

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
                abbreviated
        );
    }

    public byte[] getHash() {
        return hash.hash(); // TODO for now we will just return the read hash, but we might want to calculate it later
    }
}
