// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.junit.jupiter.api.Test;

/** Tests for simple record types in the model package. */
class SimpleRecordsTest {

    // ==================== SequenceNumber ====================

    @Test
    void sequenceNumberCopyFromAndCopyToRoundTrip() throws IOException {
        SequenceNumber original = new SequenceNumber(42L);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        original.copyTo(dos);
        dos.flush();

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
        SequenceNumber read = SequenceNumber.copyFrom(dis);
        assertEquals(original.sequenceNum(), read.sequenceNum());
    }

    @Test
    void sequenceNumberZero() throws IOException {
        SequenceNumber original = new SequenceNumber(0L);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        original.copyTo(dos);
        dos.flush();

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
        SequenceNumber read = SequenceNumber.copyFrom(dis);
        assertEquals(0L, read.sequenceNum());
    }

    // ==================== MapKey ====================

    @Test
    void mapKeyCopyFromAndCopyToRoundTrip() throws IOException {
        MapKey original = new MapKey(0, 0, 1001);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        original.copyTo(dos);
        dos.flush();

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
        MapKey read = MapKey.copyFrom(dis);
        assertEquals(original.realmId(), read.realmId());
        assertEquals(original.shardId(), read.shardId());
        assertEquals(original.accountId(), read.accountId());
    }

    @Test
    void mapKeyBadVersionThrows() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeLong(99); // bad version
        dos.writeLong(15486487); // correct object ID
        dos.writeLong(0);
        dos.writeLong(0);
        dos.writeLong(0);
        dos.flush();

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
        assertThrows(IOException.class, () -> MapKey.copyFrom(dis));
    }

    // ==================== JAccountID ====================

    @Test
    void jAccountIdCopyFromAndCopyToRoundTrip() throws IOException {
        JAccountID original = new JAccountID(0, 0, 1001);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        original.copyTo(dos);
        dos.flush();

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
        JAccountID read = JAccountID.copyFrom(dis);
        assertEquals(original.shardNum(), read.shardNum());
        assertEquals(original.realmNum(), read.realmNum());
        assertEquals(original.accountNum(), read.accountNum());
    }

    // ==================== Pair ====================

    @Test
    void pairAccessors() {
        Pair<String, Integer> pair = new Pair<>("hello", 42);
        assertEquals("hello", pair.left());
        assertEquals(42, pair.right());
    }

    // ==================== CreatorSeqPair ====================

    @Test
    void creatorSeqPairAccessors() {
        CreatorSeqPair csp = new CreatorSeqPair(1L, 2L);
        assertEquals(1L, csp.creatorId());
        assertEquals(2L, csp.seq());
    }

    // ==================== ApplicationConstants ====================

    @Test
    void applicationConstantsValues() {
        assertEquals('p', ApplicationConstants.P);
        assertEquals('n', ApplicationConstants.N);
    }
}
