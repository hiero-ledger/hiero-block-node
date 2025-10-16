package org.hiero.block.tools.records;

import static org.hiero.block.tools.records.SerializationV5Utils.HASH_OBJECT_SIZE_BYTES;
import static org.hiero.block.tools.utils.Sha384.SHA_384_HASH_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.hedera.pbj.runtime.io.buffer.BufferedData;
import java.io.IOException;
import java.util.HexFormat;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link SerializationV5Utils}.
 */
public class SerializationV5UtilsTest {
    private static final int OFFSET_TO_START_HASH = Integer.BYTES * 5;
    private static final String EXPECTED_END_HASH_HEX =
        "0d7773874647eddc3039fedf1d9a47aac58b7f4f4c47e77a8599456b800472cd0b55954837f03e002a217095615430b8";
    private static final String EXPECTED_START_HASH_HEX =
        "13ab802d147ef5a5a32c4bd386225e354aaa03c91122e48fea9777ad31e010de840dab29d42074fb264b5cacd1f69702";

    @SuppressWarnings({"DataFlowIssue", "resource"})
    public static byte[] readTestV5File() throws IOException {
        return SerializationV5UtilsTest.class.getResourceAsStream(
            "/record-files/example-v5/2022-01-01T00_00_00.252365821Z/2022-01-01T00_00_00.252365821Z.rcd")
            .readAllBytes();
    }

    @Test
    public void testReadV5HashObject() throws IOException {
        final var data = readTestV5File();
        // check end hash in file with both methods
        final var hash1 = SerializationV5Utils.readV5HashObject(new java.io.DataInputStream(
            new java.io.ByteArrayInputStream(data, data.length - HASH_OBJECT_SIZE_BYTES, HASH_OBJECT_SIZE_BYTES)));
        assertEquals(SHA_384_HASH_SIZE, hash1.length);
        assertEquals(EXPECTED_END_HASH_HEX, HexFormat.of().formatHex(hash1));
        final var hash2 = SerializationV5Utils.readV5HashObject(
            BufferedData.wrap(data,data.length - HASH_OBJECT_SIZE_BYTES, HASH_OBJECT_SIZE_BYTES));
        assertEquals(SHA_384_HASH_SIZE, hash2.length);
        assertEquals(EXPECTED_END_HASH_HEX, HexFormat.of().formatHex(hash2));
        // check start hash in file with both methods
        final var hash3 = SerializationV5Utils.readV5HashObject(new java.io.DataInputStream(
            new java.io.ByteArrayInputStream(data, OFFSET_TO_START_HASH, HASH_OBJECT_SIZE_BYTES)));
        assertEquals(SHA_384_HASH_SIZE, hash3.length);
        assertEquals(EXPECTED_START_HASH_HEX, HexFormat.of().formatHex(hash3));
        final var hash4 = SerializationV5Utils.readV5HashObject(
            BufferedData.wrap(data,OFFSET_TO_START_HASH, HASH_OBJECT_SIZE_BYTES));
        assertEquals(SHA_384_HASH_SIZE, hash4.length);
        assertEquals(EXPECTED_START_HASH_HEX, HexFormat.of().formatHex(hash4));
    }
}
