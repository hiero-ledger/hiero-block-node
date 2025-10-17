// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.records;

import static org.hiero.block.tools.records.RecordFileUtils.extractRecordFileTime;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.hapi.node.base.NodeAddressBook;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;
import org.hiero.block.tools.commands.days.model.AddressBookRegistry;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for InMemoryBlock validate() method using example record files from mainnet and testnet.
 * Testing using blocks 0, 26591040 and 82297471 as these are examples of v2,v5 and v6 formats.
 */
public class InMemoryBlockValidateTest {
    public static final byte[] EXAMPLE_V2_PREVIOUS_RECORD_FILE_HASH = new byte[48];
    public static final byte[] EXAMPLE_V5_PREVIOUS_RECORD_FILE_HASH = HexFormat.of()
            .parseHex(
                    "13ab802d147ef5a5a32c4bd386225e354aaa03c91122e48fea9777ad31e010de840dab29d42074fb264b5cacd1f69702");
    public static final byte[] EXAMPLE_V6_PREVIOUS_RECORD_FILE_HASH = HexFormat.of()
            .parseHex(
                    "cbd7a318fb7d0a023632002926857a1511953f6e1a6d162df1fe8b57f97d21389094f7a2bb31edd9d9859cc38c561bdd");
    public static final byte[] EXAMPLE_V2_EXPECTED_END_RUNNING_HASH = HexFormat.of()
            .parseHex(
                    "420fffe68fcd2a1eadcce589fdf9565bcf5a269d02232fe07cdc565b3b6f76ce46a9418ddc1bbe051d4894e04d091f8e");
    public static final byte[] EXAMPLE_V5_EXPECTED_END_RUNNING_HASH = HexFormat.of()
            .parseHex(
                    "0d7773874647eddc3039fedf1d9a47aac58b7f4f4c47e77a8599456b800472cd0b55954837f03e002a217095615430b8");
    public static final byte[] EXAMPLE_V6_EXPECTED_END_RUNNING_HASH = HexFormat.of()
            .parseHex(
                    "f3a71062087f6afb70754c32cca0dcb48d297b0b909a956cd2b6d22c782ed6054742584b0465865e1fb1adcfbda7f65d");

    /**
     * Load a file from a resource path into an InMemoryFile.
     *
     * @param resourcePath the path to the resource file
     * @return the loaded InMemoryFile
     * @throws Exception if there is an error loading the file
     */
    private static InMemoryFile loadResourceFile(String resourcePath) throws Exception {
        final URL url = InMemoryBlockValidateTest.class.getResource(resourcePath);
        if (url == null) {
            throw new IllegalStateException("Resource not found: " + resourcePath);
        }
        final Path path = Path.of(url.toURI());
        final byte[] data = Files.readAllBytes(path);
        return new InMemoryFile(path.getFileName(), data);
    }

    /**
     * Load an address book from a resource file.
     *
     * @param resourcePath the path to the resource file
     * @return the loaded NodeAddressBook
     * @throws Exception if there is an error loading the file
     */
    private static NodeAddressBook loadAddressBookResourceFile(String resourcePath) throws Exception {
        final URL url = InMemoryBlockValidateTest.class.getResource(resourcePath);
        if (url == null) {
            throw new IllegalStateException("Resource not found: " + resourcePath);
        }
        final Path path = Path.of(url.toURI());
        final byte[] data = Files.readAllBytes(path);
        return AddressBookRegistry.readAddressBook(data);
    }

    @Test
    void validateV2RecordFile() throws Exception {
        final NodeAddressBook addressBook = AddressBookRegistry.loadGenesisAddressBook();
        final String base = "/record-files/example-v2/2019-09-13T21_53_51.396440Z/";
        final InMemoryFile record = loadResourceFile(base + "2019-09-13T21_53_51.396440Z.rcd");
        // use RecordFileInfo to check the previous file and end running hashes written in the file, not recomputed
        final RecordFileInfo info = RecordFileInfo.parse(record.data());
        assertArrayEquals(
                EXAMPLE_V2_PREVIOUS_RECORD_FILE_HASH,
                info.previousBlockHash().toByteArray(),
                "Previous file hash mismatch for v2");
        assertArrayEquals(
                EXAMPLE_V2_EXPECTED_END_RUNNING_HASH,
                info.blockHash().toByteArray(),
                "Expected end running hash mismatch for v2 (info)");
        // create the InMemoryBlock
        final InMemoryBlock block = InMemoryBlock.newInMemoryBlock(
                extractRecordFileTime("2019-09-13T21_53_51.396440Z.rcd").toInstant(ZoneOffset.UTC),
                record,
                Collections.emptyList(), // no other record files needed for testing
                List.of(
                        loadResourceFile(base + "node_0.0.3.rcd_sig"),
                        loadResourceFile(base + "node_0.0.4.rcd_sig"),
                        loadResourceFile(base + "node_0.0.5.rcd_sig"),
                        loadResourceFile(base + "node_0.0.6.rcd_sig"),
                        loadResourceFile(base + "node_0.0.7.rcd_sig"),
                        loadResourceFile(base + "node_0.0.8.rcd_sig"),
                        loadResourceFile(base + "node_0.0.9.rcd_sig")),
                Collections.emptyList(),
                Collections.emptyList());
        // now call validate with the genesis address book as this is the first block
        final InMemoryBlock.ValidationResult result =
                block.validate(EXAMPLE_V2_PREVIOUS_RECORD_FILE_HASH, addressBook);
        assertTrue(
                result.warningMessages().isBlank(),
                "Should be no warnings, but got:\n" + result.warningMessages() + "\n");
        assertTrue(result.isValid(), "V2 validation should be valid");
        assertArrayEquals(
                EXAMPLE_V2_EXPECTED_END_RUNNING_HASH, result.endRunningHash(), "End running hash mismatch for v2");
        assertEquals(info.hapiProtoVersion(), result.hapiVersion(), "HAPI version mismatch for v2");
        // now make bad block with not enough signature files and check we get validation failure
        final InMemoryBlock badBlock = InMemoryBlock.newInMemoryBlock(
            extractRecordFileTime("2019-09-13T21_53_51.396440Z.rcd").toInstant(ZoneOffset.UTC),
            record,
            Collections.emptyList(), // no other record files needed for testing
            List.of(
                loadResourceFile(base + "node_0.0.3.rcd_sig"),
                loadResourceFile(base + "node_0.0.4.rcd_sig"),
                loadResourceFile(base + "node_0.0.5.rcd_sig")),
            Collections.emptyList(),
            Collections.emptyList());
        final InMemoryBlock.ValidationResult result2 =
            badBlock.validate(EXAMPLE_V2_PREVIOUS_RECORD_FILE_HASH, addressBook);
        assertTrue(result2.warningMessages().contains("Insufficient valid signatures"), "Should report not enough sigs");
        assertFalse(result2.isValid(), "V2 validation should be invalid");
    }

    @Test
    void validateV5RecordFile() throws Exception {
        final String base = "/record-files/example-v5/2022-01-01T00_00_00.252365821Z/";
        final NodeAddressBook addressBook = loadAddressBookResourceFile(base+"address_book.bin");
        final InMemoryFile record = loadResourceFile(base + "2022-01-01T00_00_00.252365821Z.rcd");
        // use RecordFileInfo to check the previous file and end running hashes written in the file, not recomputed
        final RecordFileInfo info = RecordFileInfo.parse(record.data());
        assertArrayEquals(
                EXAMPLE_V5_PREVIOUS_RECORD_FILE_HASH,
                info.previousBlockHash().toByteArray(),
                "Previous file hash mismatch for v5");
        assertArrayEquals(
                EXAMPLE_V5_EXPECTED_END_RUNNING_HASH,
                info.blockHash().toByteArray(),
                "Expected end running hash mismatch for v5 (info)");
        // create the InMemoryBlock
        final InMemoryBlock block = InMemoryBlock.newInMemoryBlock(
                extractRecordFileTime("2022-01-01T00_00_00.252365821Z.rcd").toInstant(ZoneOffset.UTC),
                record,
                Collections.emptyList(), // no other record files needed for testing
                List.of(
                        loadResourceFile(base + "node_0.0.10.rcd_sig"),
                        loadResourceFile(base + "node_0.0.11.rcd_sig"),
                        loadResourceFile(base + "node_0.0.12.rcd_sig"),
                        loadResourceFile(base + "node_0.0.13.rcd_sig"),
                        loadResourceFile(base + "node_0.0.14.rcd_sig"),
                        loadResourceFile(base + "node_0.0.15.rcd_sig"),
                        loadResourceFile(base + "node_0.0.16.rcd_sig"),
                        loadResourceFile(base + "node_0.0.17.rcd_sig"),
                        loadResourceFile(base + "node_0.0.18.rcd_sig"),
                        loadResourceFile(base + "node_0.0.20.rcd_sig"),
                        loadResourceFile(base + "node_0.0.21.rcd_sig"),
                        loadResourceFile(base + "node_0.0.22.rcd_sig")),
                Collections.emptyList(),
                Collections.emptyList()); // this block has no sidecar files
        // validate the block
        final InMemoryBlock.ValidationResult result =
                block.validate(EXAMPLE_V5_PREVIOUS_RECORD_FILE_HASH, addressBook);
        assertTrue(
                result.warningMessages().isBlank(),
                "Should be no warnings, but got:\n" + result.warningMessages() + "\n");
        assertTrue(result.isValid(), "V5 validation should be valid");
        assertArrayEquals(
                EXAMPLE_V5_EXPECTED_END_RUNNING_HASH, result.endRunningHash(), "End running hash mismatch for v5");
        assertEquals(info.hapiProtoVersion(), result.hapiVersion(), "HAPI version mismatch for v5");
        // now make bad block with not enough signature files and check we get validation failure
        final InMemoryBlock badBlock = InMemoryBlock.newInMemoryBlock(
            extractRecordFileTime("2022-01-01T00_00_00.252365821Z.rcd").toInstant(ZoneOffset.UTC),
            record,
            Collections.emptyList(), // no other record files needed for testing
            List.of(
                loadResourceFile(base + "node_0.0.10.rcd_sig"),
                loadResourceFile(base + "node_0.0.11.rcd_sig"),
                loadResourceFile(base + "node_0.0.12.rcd_sig")),
            Collections.emptyList(),
            Collections.emptyList()); // this block has no sidecar files
        final InMemoryBlock.ValidationResult result2 =
            badBlock.validate(EXAMPLE_V5_PREVIOUS_RECORD_FILE_HASH, addressBook);
        assertTrue(result2.warningMessages().contains("Insufficient valid signatures"), "Should report not enough sigs");
        assertFalse(result2.isValid(), "V5 validation should be invalid");
    }

    @Test
    void validateV6RecordFileWithSidecar() throws Exception {
        final String base = "/record-files/example-v6/2025-07-23T20_37_42.076472454Z/";
        final NodeAddressBook addressBook = loadAddressBookResourceFile(base+"address_book.bin");
        System.out.println("addressBook = " + addressBook);
        final InMemoryFile record = loadResourceFile(base + "2025-07-23T20_37_42.076472454Z.rcd");
        final InMemoryFile sidecar01 = loadResourceFile(base + "2025-07-23T20_37_42.076472454Z_01.rcd");
        // use RecordFileInfo to check the previous file and end running hashes written in the file, not recomputed
        final RecordFileInfo info = RecordFileInfo.parse(record.data());
        assertArrayEquals(
                EXAMPLE_V6_PREVIOUS_RECORD_FILE_HASH,
                info.previousBlockHash().toByteArray(),
                "Previous file hash mismatch for v6");
        assertArrayEquals(
                EXAMPLE_V6_EXPECTED_END_RUNNING_HASH,
                info.blockHash().toByteArray(),
                "Expected end running hash mismatch for v6 (info)");
        // create the InMemoryBlock
        final InMemoryBlock block = InMemoryBlock.newInMemoryBlock(
                extractRecordFileTime("2025-07-23T20_37_42.076472454Z.rcd").toInstant(ZoneOffset.UTC),
                record,
                Collections.emptyList(), // no other record files needed for testing
                List.of(
                        loadResourceFile(base + "node_0.0.3.rcd_sig"),
                        loadResourceFile(base + "node_0.0.21.rcd_sig"),
                        loadResourceFile(base + "node_0.0.25.rcd_sig"),
                        loadResourceFile(base + "node_0.0.28.rcd_sig"),
                        loadResourceFile(base + "node_0.0.29.rcd_sig"),
                        loadResourceFile(base + "node_0.0.30.rcd_sig"),
                        loadResourceFile(base + "node_0.0.31.rcd_sig"),
                        loadResourceFile(base + "node_0.0.32.rcd_sig"),
                        loadResourceFile(base + "node_0.0.33.rcd_sig"),
                        loadResourceFile(base + "node_0.0.34.rcd_sig"),
                        loadResourceFile(base + "node_0.0.35.rcd_sig"),
                        loadResourceFile(base + "node_0.0.37.rcd_sig")),
                List.of(sidecar01),
                Collections.emptyList());

        // validate the block
        final InMemoryBlock.ValidationResult result =
                block.validate(EXAMPLE_V6_PREVIOUS_RECORD_FILE_HASH, addressBook);
        assertTrue(
                result.warningMessages().isBlank(),
                "Should be no warnings, but got:\n" + result.warningMessages() + "\n");
        assertTrue(result.isValid(), "V6 validation should be valid");
        assertArrayEquals(
                EXAMPLE_V6_EXPECTED_END_RUNNING_HASH, result.endRunningHash(), "End running hash mismatch for v6");
        assertEquals(info.hapiProtoVersion(), result.hapiVersion(), "HAPI version mismatch for v6");
        // now make bad block with not enough signature files and check we get validation failure
        final InMemoryBlock badBlock = InMemoryBlock.newInMemoryBlock(
            extractRecordFileTime("2025-07-23T20_37_42.076472454Z.rcd").toInstant(ZoneOffset.UTC),
            record,
            Collections.emptyList(), // no other record files needed for testing
            List.of(
                loadResourceFile(base + "node_0.0.3.rcd_sig"),
                loadResourceFile(base + "node_0.0.21.rcd_sig"),
                loadResourceFile(base + "node_0.0.25.rcd_sig")),
            List.of(sidecar01),
            Collections.emptyList());
        final InMemoryBlock.ValidationResult result2 =
            badBlock.validate(EXAMPLE_V6_PREVIOUS_RECORD_FILE_HASH, addressBook);
        assertTrue(result2.warningMessages().contains("Insufficient valid signatures"), "Should report not enough sigs");
        assertFalse(result2.isValid(), "V6 validation should be invalid");
    }
}
