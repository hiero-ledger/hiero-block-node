// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.utils;

import static org.hiero.block.tools.records.RecordFileDates.instantToBlockTimeLong;

import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.hiero.block.tools.days.model.AddressBookRegistry;
import org.hiero.block.tools.mirrornode.BlockTimeReader;
import org.hiero.block.tools.records.model.parsed.ParsedSignatureFile;
import org.hiero.block.tools.records.model.parsed.ParsedV2RecordFileTest;
import org.hiero.block.tools.records.model.unparsed.InMemoryFile;

/**
 * Test data record files from Hedera main net data. Data from resources and hard coded from mirror node data.
 */
@SuppressWarnings("unused")
public class TestBlocks {
    // ================= V2 Test Block =================================================================================
    /** V2 Test block number */
    public static final long V2_TEST_BLOCK_NUMBER = 0;
    /** V2 Test Block 2019-09-13T21_53_51.396440Z BLOCK 0 hash from mirror node */
    public static final byte[] V2_TEST_BLOCK_HASH = HexFormat.of()
            .parseHex(
                    "420fffe68fcd2a1eadcce589fdf9565bcf5a269d02232fe07cdc565b3b6f76ce46a9418ddc1bbe051d4894e04d091f8e");
    /** V2 Test Block date string */
    public static final String V2_TEST_BLOCK_DATE_STR = "2019-09-13T21_53_51.396440Z";
    /** V2 Test Block record file name */
    public static final String V2_TEST_BLOCK_RECORD_FILE_NAME = V2_TEST_BLOCK_DATE_STR + ".rcd";
    /** V2 Test Block Node Address Book */
    public static final NodeAddressBook V2_TEST_BLOCK_ADDRESS_BOOK = new AddressBookRegistry().getCurrentAddressBook();
    /** V2 Test Block bytes */
    public static final byte[] V2_TEST_BLOCK_BYTES;

    static {
        try {
            V2_TEST_BLOCK_BYTES = Objects.requireNonNull(
                            ParsedV2RecordFileTest.class.getResourceAsStream("/record-files/example-v2/"
                                    + V2_TEST_BLOCK_DATE_STR + "/" + V2_TEST_BLOCK_RECORD_FILE_NAME))
                    .readAllBytes();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // ================= V5 Test Block =================================================================================
    /** V5 Test block number */
    public static final long V5_TEST_BLOCK_NUMBER = 26591040;
    /** V5 Test Block 22022-01-01T00_00_00.252365821Z BLOCK 26591040 hash from mirror node */
    public static final byte[] V5_TEST_BLOCK_HASH = HexFormat.of()
            .parseHex(
                    "0d7773874647eddc3039fedf1d9a47aac58b7f4f4c47e77a8599456b800472cd0b55954837f03e002a217095615430b8");
    /** V5 Test Block date string */
    public static final String V5_TEST_BLOCK_DATE_STR = "2022-01-01T00_00_00.252365821Z";
    /** V5 Test Block record file name */
    public static final String V5_TEST_BLOCK_RECORD_FILE_NAME = V5_TEST_BLOCK_DATE_STR + ".rcd";
    /** V5 Test Block Node Address Book */
    public static final NodeAddressBook V5_TEST_BLOCK_ADDRESS_BOOK;
    /** V5 Test Block bytes */
    public static final byte[] V5_TEST_BLOCK_BYTES;

    static {
        try {
            V5_TEST_BLOCK_ADDRESS_BOOK = NodeAddressBook.PROTOBUF.parse(
                    Bytes.wrap(Objects.requireNonNull(ParsedV2RecordFileTest.class.getResourceAsStream(
                                    "/record-files/example-v5/" + V5_TEST_BLOCK_DATE_STR + "/address_book.bin"))
                            .readAllBytes()));
            V5_TEST_BLOCK_BYTES = Objects.requireNonNull(
                            ParsedV2RecordFileTest.class.getResourceAsStream("/record-files/example-v5/"
                                    + V5_TEST_BLOCK_DATE_STR + "/" + V5_TEST_BLOCK_RECORD_FILE_NAME))
                    .readAllBytes();
        } catch (IOException | ParseException e) {
            throw new RuntimeException(e);
        }
    }

    // ================= V6 Test Block =================================================================================
    /** V6 Test block number */
    public static final long V6_TEST_BLOCK_NUMBER = 82297471;
    /** V6 Test Block 2025-07-23T20_37_42.076472454Z BLOCK 82297471 hash from mirror node */
    public static final byte[] V6_TEST_BLOCK_HASH = HexFormat.of()
            .parseHex(
                    "f3a71062087f6afb70754c32cca0dcb48d297b0b909a956cd2b6d22c782ed6054742584b0465865e1fb1adcfbda7f65d");
    /** V6 Test Block date string */
    public static final String V6_TEST_BLOCK_DATE_STR = "2025-07-23T20_37_42.076472454Z";
    /** V6 Test Block record file name */
    public static final String V6_TEST_BLOCK_RECORD_FILE_NAME = V6_TEST_BLOCK_DATE_STR + ".rcd";
    /** V6 Test Block sidecar file name */
    public static final String V6_TEST_BLOCK_SIDECAR_FILE_NAME = V6_TEST_BLOCK_DATE_STR + "_01.rcd";
    /** V6 Test Block Node Address Book */
    public static final NodeAddressBook V6_TEST_BLOCK_ADDRESS_BOOK;
    /** V6 Test Block bytes */
    public static final byte[] V6_TEST_BLOCK_BYTES;
    /** V6 Test Block sidecar bytes */
    public static final byte[] V6_TEST_BLOCK_SIDECAR_BYTES;

    static {
        try {
            V6_TEST_BLOCK_ADDRESS_BOOK = NodeAddressBook.PROTOBUF.parse(
                    Bytes.wrap(Objects.requireNonNull(ParsedV2RecordFileTest.class.getResourceAsStream(
                                    "/record-files/example-v6/" + V6_TEST_BLOCK_DATE_STR + "/address_book.bin"))
                            .readAllBytes()));
            V6_TEST_BLOCK_BYTES = Objects.requireNonNull(
                            ParsedV2RecordFileTest.class.getResourceAsStream("/record-files/example-v6/"
                                    + V6_TEST_BLOCK_DATE_STR + "/" + V6_TEST_BLOCK_RECORD_FILE_NAME))
                    .readAllBytes();
            V6_TEST_BLOCK_SIDECAR_BYTES = Objects.requireNonNull(
                            ParsedV2RecordFileTest.class.getResourceAsStream("/record-files/example-v6/"
                                    + V6_TEST_BLOCK_DATE_STR + "/" + V6_TEST_BLOCK_SIDECAR_FILE_NAME))
                    .readAllBytes();
        } catch (IOException | ParseException e) {
            throw new RuntimeException(e);
        }
    }

    // ================= Signature Files Loading =======================================================================

    /**
     * Loads V2 test signature files for nodes 3-9.
     *
     * @return list of parsed signature files
     */
    public static List<ParsedSignatureFile> loadV2SignatureFiles() {
        return IntStream.range(3, 10)
                .mapToObj(nodeId -> {
                    try {
                        final byte[] sigBytes = Objects.requireNonNull(
                                        ParsedV2RecordFileTest.class.getResourceAsStream("/record-files/example-v2/"
                                                + V2_TEST_BLOCK_DATE_STR + "/node_0.0." + nodeId + ".rcd_sig"))
                                .readAllBytes();
                        return new ParsedSignatureFile(
                                new InMemoryFile(Path.of("node_0.0." + nodeId + ".rcd_sig"), sigBytes));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .toList();
    }

    /**
     * Loads V5 test signature files for nodes 10-18, 20-22.
     *
     * @return list of parsed signature files
     */
    public static List<ParsedSignatureFile> loadV5SignatureFiles() {
        return Stream.concat(
                        IntStream.rangeClosed(10, 18).boxed(),
                        IntStream.rangeClosed(20, 22).boxed())
                .map(nodeId -> {
                    try {
                        final byte[] sigBytes = Objects.requireNonNull(
                                        ParsedV2RecordFileTest.class.getResourceAsStream("/record-files/example-v5/"
                                                + V5_TEST_BLOCK_DATE_STR + "/node_0.0." + nodeId + ".rcd_sig"))
                                .readAllBytes();
                        return new ParsedSignatureFile(
                                new InMemoryFile(Path.of("node_0.0." + nodeId + ".rcd_sig"), sigBytes));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .toList();
    }

    /**
     * Loads V6 test signature files for nodes 3, 21, 25, 28-35, 37.
     *
     * @return list of parsed signature files
     */
    public static List<ParsedSignatureFile> loadV6SignatureFiles() {
        return Stream.of(Stream.of(3, 21, 25, 37), IntStream.rangeClosed(28, 35).boxed())
                .flatMap(s -> s)
                .map(nodeId -> {
                    try {
                        final byte[] sigBytes = Objects.requireNonNull(
                                        ParsedV2RecordFileTest.class.getResourceAsStream("/record-files/example-v6/"
                                                + V6_TEST_BLOCK_DATE_STR + "/node_0.0." + nodeId + ".rcd_sig"))
                                .readAllBytes();
                        return new ParsedSignatureFile(
                                new InMemoryFile(Path.of("node_0.0." + nodeId + ".rcd_sig"), sigBytes));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .toList();
    }

    /** Utility main method to find block number by time */
    @SuppressWarnings("resource")
    public static void main(String[] args) throws Exception {
        BlockTimeReader blockTimeReader = new BlockTimeReader(Path.of("data/block_times.bin"));
        long blockNum = blockTimeReader.getNearestBlockAfterTime(
                instantToBlockTimeLong(Instant.parse("2025-07-23T20:37:42.076472454Z")));
        System.out.println("blockNum = " + blockNum);
    }
}
