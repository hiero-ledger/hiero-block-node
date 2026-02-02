// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.junit.jupiter.api.Test;

/** Tests for {@link FCLinkedList}. */
class FCLinkedListTest {

    // ==================== copyTo with hash ====================

    @Test
    void copyToWritesVersionObjectIdAndHash() throws IOException {
        // Create an FCLinkedList with a known hash via copyFrom of an empty list
        ByteArrayOutputStream setupBaos = new ByteArrayOutputStream();
        DataOutputStream setupDos = new DataOutputStream(setupBaos);
        setupDos.writeLong(FCLinkedList.VERSION); // version
        setupDos.writeLong(FCLinkedList.OBJECT_ID); // object ID
        byte[] testHash = new byte[48];
        testHash[0] = 42;
        setupDos.write(testHash); // hash
        // copyFromExtra: BEGIN_LIST, VERSION, OBJECT_ID, size=0, END_LIST
        setupDos.writeInt(275624369); // BEGIN_LIST_MARKER
        setupDos.writeLong(FCLinkedList.VERSION);
        setupDos.writeLong(FCLinkedList.OBJECT_ID);
        setupDos.writeInt(0); // empty list
        setupDos.writeInt(275654143); // END_LIST_MARKER
        setupDos.flush();

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(setupBaos.toByteArray()));
        FCLinkedList<String> list = FCLinkedList.copyFrom(dis, in -> "dummy");
        assertEquals(0, list.size());

        // Now test copyTo
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        list.copyTo(dos);
        dos.flush();

        // Read back: version(8) + objectId(8) + hash(48) = 64 bytes
        assertEquals(64, baos.size());
        DataInputStream readDis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
        assertEquals(FCLinkedList.VERSION, readDis.readLong());
        assertEquals(FCLinkedList.OBJECT_ID, readDis.readLong());
        byte[] readHash = new byte[48];
        readDis.readFully(readHash);
        assertEquals(42, readHash[0]);
    }

    // ==================== copyTo without hash (null) ====================

    @Test
    void copyToWithNullHashWritesZeros() throws IOException {
        FCLinkedList<String> list = new FCLinkedList<>();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        list.copyTo(dos);
        dos.flush();

        assertEquals(64, baos.size());
        DataInputStream readDis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
        readDis.readLong(); // version
        readDis.readLong(); // objectId
        byte[] readHash = new byte[48];
        readDis.readFully(readHash);
        for (byte b : readHash) {
            assertEquals(0, b);
        }
    }

    // ==================== copyFrom bad version ====================

    @Test
    void copyFromBadVersionThrows() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeLong(999L); // bad version
        dos.writeLong(FCLinkedList.OBJECT_ID);
        dos.flush();

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
        assertThrows(IOException.class, () -> FCLinkedList.copyFrom(dis, in -> "dummy"));
    }

    // ==================== copyFrom bad object ID ====================

    @Test
    void copyFromBadObjectIdThrows() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeLong(FCLinkedList.VERSION);
        dos.writeLong(9999999L); // bad object ID
        dos.flush();

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
        assertThrows(IOException.class, () -> FCLinkedList.copyFrom(dis, in -> "dummy"));
    }

    // ==================== copyFrom with elements ====================

    @Test
    void copyFromWithElements() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeLong(FCLinkedList.VERSION);
        dos.writeLong(FCLinkedList.OBJECT_ID);
        dos.write(new byte[48]); // hash
        dos.writeInt(275624369); // BEGIN_LIST
        dos.writeLong(FCLinkedList.VERSION);
        dos.writeLong(FCLinkedList.OBJECT_ID);
        dos.writeInt(2); // 2 elements

        // Element 1
        dos.writeInt(282113441); // BEGIN_ELEMENT
        dos.writeBoolean(false); // element present (inverted: !readBoolean())
        dos.writeUTF("hello");
        dos.writeInt(282124951); // END_ELEMENT

        // Element 2 (null)
        dos.writeInt(282113441); // BEGIN_ELEMENT
        dos.writeBoolean(true); // null element (inverted: !readBoolean() = false)

        dos.writeInt(275654143); // END_LIST
        dos.flush();

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
        FCLinkedList<String> list = FCLinkedList.copyFrom(dis, in -> {
            try {
                return in.readUTF();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        assertEquals(2, list.size());
        assertNotNull(list.get(0));
        assertEquals("hello", list.get(0));
        assertEquals(null, list.get(1));
    }
}
