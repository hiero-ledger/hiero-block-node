// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.LinkedList;
import java.util.List;
import java.util.zip.GZIPInputStream;
import org.hiero.block.tools.states.utils.HashingOutputStream;
import org.hiero.block.tools.states.utils.Utils;

/** A complete signed state snapshot including the application state, events, address book, and signatures. */
@SuppressWarnings({"CallToPrintStackTrace", "unused"})
public final class SignedState {
    /** This version number should be used to handle compatibility issues that may arise from any future changes */
    private static final long CLASS_VERSION = 2;
    /** number of rounds considered stale for minimum generation calculation */
    private static final long ROUNDS_STALE = 25;

    /** the version number read from the serialized state */
    private long instanceVersion;
    /** the hash read from the serialized state */
    private byte[] readHash;
    /** the application state containing accounts and storage */
    private final HGCAppState state = new HGCAppState();
    /** the consensus round number */
    private long round;
    /** the number of consensus events */
    private long numEventsCons = 0;
    /** the network address book at this state */
    private final AddressBook addressBook = new AddressBook();
    /** the hash of consensus events */
    private byte[] hashEventsCons;
    /** the consensus timestamp */
    private Instant consensusTimestamp;
    /** whether this state should be saved to disk */
    private boolean shouldSaveToDisk;
    /** the array of events */
    private Event[] events;
    /** the minimum generation info per round */
    private List<Pair<Long, Long>> minGenInfo;
    /** the signature set for this state */
    private SigSet sigSet;

    /**
     * Load a SignedState from a URL, can be compressed (.gz) or uncompressed
     *
     * @param stateFileUrl the URL to the signed state file
     * @return the loaded SignedState
     * @throws IOException if an I/O error occurs
     */
    public static SignedState load(URL stateFileUrl) throws IOException {
        SignedState signedState = new SignedState();
        try (DataInputStream fin = new DataInputStream(
                stateFileUrl.toString().endsWith(".gz")
                        ? new GZIPInputStream(new BufferedInputStream(stateFileUrl.openStream(), 1024 * 1024))
                        : new BufferedInputStream(stateFileUrl.openStream(), 1024 * 1024))) {
            signedState.copyFrom(fin);
            signedState.copyFromExtra(fin);
        }
        return signedState;
    }

    /**
     * Load a SignedState from a file, can be compressed (.gz) or uncompressed
     *
     * @param stateFile the path to the signed state file
     * @return the loaded SignedState
     * @throws IOException if an I/O error occurs
     */
    public static SignedState load(Path stateFile) throws IOException {
        SignedState signedState = new SignedState();
        try (DataInputStream fin = new DataInputStream(
                stateFile.getFileName().toString().endsWith(".gz")
                        ? new GZIPInputStream(new BufferedInputStream(Files.newInputStream(stateFile), 1024 * 1024))
                        : new BufferedInputStream(Files.newInputStream(stateFile), 1024 * 1024))) {
            signedState.copyFrom(fin);
            signedState.copyFromExtra(fin);
        }
        return signedState;
    }

    /**
     * Gets the hash read from the serialized state.
     *
     * @return the hash read from the serialized state
     */
    public byte[] readHash() {
        return readHash;
    }

    /**
     * Gets the version number read from the serialized state.
     *
     * @return the version number read from the serialized state
     */
    public long instanceVersion() {
        return instanceVersion;
    }

    /**
     * Gets the consensus round number.
     *
     * @return the consensus round number
     */
    public long round() {
        return round;
    }

    /**
     * Gets the number of consensus events.
     *
     * @return the number of consensus events
     */
    public long numEventsCons() {
        return numEventsCons;
    }

    /**
     * Gets the network address book at this state.
     *
     * @return the network address book at this state
     */
    public AddressBook addressBook() {
        return addressBook;
    }

    /**
     * Gets whether this state should be saved to disk.
     *
     * @return whether this state should be saved to disk
     */
    public boolean shouldSaveToDisk() {
        return shouldSaveToDisk;
    }

    /**
     * Gets the application state containing accounts and storage.
     *
     * @return the application state containing accounts and storage
     */
    public HGCAppState state() {
        return state;
    }

    /**
     * Gets the consensus timestamp.
     *
     * @return the consensus timestamp
     */
    public Instant consensusTimestamp() {
        return consensusTimestamp;
    }

    /**
     * Gets the signature set for this state.
     *
     * @return the signature set for this state
     */
    public SigSet sigSet() {
        return sigSet;
    }

    /**
     * Deserializes the SignedState header from the given stream.
     *
     * @param inStream the stream to read from
     * @throws IOException if an I/O error occurs or the version is incompatible
     */
    public void copyFrom(DataInputStream inStream) throws IOException {
        instanceVersion = inStream.readLong(); // classVersion
        if (instanceVersion != CLASS_VERSION) {
            throw new IOException(
                    "SignedState version mismatch: expected " + CLASS_VERSION + ", got " + instanceVersion);
        }
        readHash = Utils.readByteArray(inStream);
        state.copyFrom(inStream);
        round = inStream.readLong();
        numEventsCons = inStream.readLong();
        addressBook.copyFrom(inStream);
        hashEventsCons = Utils.readByteArray(inStream);
        consensusTimestamp = Utils.readInstant(inStream);
        shouldSaveToDisk = inStream.readBoolean();
        int numEvents = inStream.readInt();
        events = new Event[numEvents];
        HashMap<CreatorSeqPair, Event> eventsByCreatorSeq = new HashMap<>();
        for (int i = 0; i < events.length; i++) {
            events[i] = Event.readFrom(inStream, eventsByCreatorSeq);
            eventsByCreatorSeq.put(new CreatorSeqPair(events[i].creatorId(), events[i].creatorSeq()), events[i]);
        }

        // from version 2 we store the minGen info
        if (instanceVersion >= 2) {
            minGenInfo = Utils.readList(inStream, LinkedList::new, (stream) -> {
                long key = stream.readLong();
                long value = stream.readLong();
                return new Pair<>(key, value);
            });
        } else {
            // if we don't have min gen info, we need to produce it from the events
            minGenInfo = produceMinGenFromEvents();
        }

        sigSet = new SigSet(addressBook);
        sigSet.copyFrom(inStream);
    }

    /**
     * Deserializes the SignedState contents from the given stream.
     *
     * @param inStream the stream to read from
     * @throws IOException if an I/O error occurs
     */
    public void copyFromExtra(DataInputStream inStream) throws IOException {
        state.copyFromExtra(inStream);
    }

    private List<Pair<Long, Long>> produceMinGenFromEvents() {
        long minGen = events[0].generation();
        List<Pair<Long, Long>> list = new LinkedList<>();
        for (long i = round - ROUNDS_STALE; i <= round; i++) {
            list.add(new Pair<>(i, minGen));
        }
        return list;
    }

    /**
     * Generates a hash of the full SignedState, matching the original platform's
     * {@code SignedState.generateSigendStateHash()} method.
     *
     * @param digest the object to do the hashing
     * @return the hash generated, or null if an error occurred
     */
    public byte[] generateSignedStateHash(MessageDigest digest) {
        byte[] swirldStateHash = generateSwirldStateHash(digest);
        if (swirldStateHash == null) return null;
        digest.reset();
        Hash.update(digest, round);
        Hash.update(digest, consensusTimestamp);
        Hash.update(digest, numEventsCons);
        digest.update(hashEventsCons);
        for (Event event : events) {
            digest.update(event.getHash());
        }
        digest.update(swirldStateHash);
        digest.update(addressBook.getHash());
        return digest.digest();
    }

    /**
     * Generates a hash of the SwirldState
     *
     * @param digest
     * 		the object to do the hashing
     * @return the hash generated, or null if an error occurred
     */
    public byte[] generateSwirldStateHash(MessageDigest digest) {
        // the streams are closed automatically
        try (HashingOutputStream hashOut = new HashingOutputStream(digest);
                BufferedOutputStream bufOut = new BufferedOutputStream(hashOut);
                DataOutputStream fcOut = new DataOutputStream(bufOut)) {
            state.copyTo(fcOut);
            fcOut.flush();
            return digest.digest();
        } catch (IOException e) {
            e.printStackTrace();
        }
        digest.reset();
        return null;
    }

    @Override
    public String toString() {
        return "SignedState{" + "instanceVersion="
                + instanceVersion + ",\n     readHash="
                + HexFormat.of().formatHex(readHash) + ",\n     state="
                + state + ",\n     round="
                + round + ",\n     numEventsCons="
                + numEventsCons + ",\n     addressBook="
                + addressBook + ",\n     hashEventsCons="
                + HexFormat.of().formatHex(hashEventsCons) + ",\n     consensusTimestamp="
                + consensusTimestamp + ",\n     shouldSaveToDisk="
                + shouldSaveToDisk + ",\n     events Count="
                + events.length + ",\n     minGenInfo="
                + minGenInfo + ",\n     sigSet="
                + sigSet + "\n}";
    }
}
