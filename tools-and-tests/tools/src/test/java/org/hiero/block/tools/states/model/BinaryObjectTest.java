// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.junit.jupiter.api.Test;

/** Tests for {@link BinaryObject}. */
class BinaryObjectTest {

    // ==================== copyFrom / copyTo roundtrip ====================

    @Test
    void copyFromAndCopyToRoundTrip() throws IOException {
        byte[] hashBytes = new byte[48];
        hashBytes[0] = (byte) 0xAB;
        hashBytes[47] = (byte) 0xCD;

        // Write a BinaryObject
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeLong(1L); // version
        dos.writeLong(1231553L); // object ID
        dos.write(hashBytes);
        dos.flush();

        // Read it back
        BinaryObject obj = new BinaryObject();
        obj.copyFrom(new DataInputStream(new ByteArrayInputStream(baos.toByteArray())));
        assertNotNull(obj.hash());
        assertEquals(48, obj.hash().hash().length);

        // Write it out again
        ByteArrayOutputStream baos2 = new ByteArrayOutputStream();
        DataOutputStream dos2 = new DataOutputStream(baos2);
        obj.copyTo(dos2);
        dos2.flush();

        // Read again and compare
        BinaryObject obj2 = new BinaryObject();
        obj2.copyFrom(new DataInputStream(new ByteArrayInputStream(baos2.toByteArray())));
        assertEquals(obj.hash().hex(), obj2.hash().hex());
    }

    // ==================== copyFrom bad version ====================

    @Test
    void copyFromBadVersionThrows() {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        try {
            dos.writeLong(999L); // bad version
            dos.writeLong(1231553L);
            dos.write(new byte[48]);
            dos.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        BinaryObject obj = new BinaryObject();
        assertThrows(
                IOException.class,
                () -> obj.copyFrom(new DataInputStream(new ByteArrayInputStream(baos.toByteArray()))));
    }

    // ==================== copyFrom bad object ID ====================

    @Test
    void copyFromBadObjectIdThrows() {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        try {
            dos.writeLong(1L); // version
            dos.writeLong(9999999L); // bad object ID
            dos.write(new byte[48]);
            dos.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        BinaryObject obj = new BinaryObject();
        assertThrows(
                IOException.class,
                () -> obj.copyFrom(new DataInputStream(new ByteArrayInputStream(baos.toByteArray()))));
    }

    // ==================== constructor with data ====================

    @Test
    void constructorWithData() {
        byte[] data = {1, 2, 3};
        BinaryObject obj = new BinaryObject(data);
        assertEquals(data, obj.data());
        assertNull(obj.hash());
    }

    // ==================== empty constructor ====================

    @Test
    void emptyConstructor() {
        BinaryObject obj = new BinaryObject();
        assertNull(obj.data());
        assertNull(obj.hash());
        assertEquals(0L, obj.id());
    }

    // ==================== toString ====================

    @Test
    void toStringWithData() {
        BinaryObject obj = new BinaryObject(new byte[] {0x0A, 0x0B});
        String str = obj.toString();
        assertTrue(str.contains("0a0b"));
    }

    @Test
    void toStringWithNullData() {
        BinaryObject obj = new BinaryObject();
        String str = obj.toString();
        assertTrue(str.contains("null"));
    }
}
