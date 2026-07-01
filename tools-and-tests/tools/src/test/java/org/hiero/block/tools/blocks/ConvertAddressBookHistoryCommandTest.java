// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hedera.hapi.node.base.AccountID;
import com.hedera.hapi.node.base.NodeAddress;
import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.hedera.pbj.runtime.io.stream.WritableStreamingData;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import org.hiero.block.internal.AddressBookHistory;
import org.hiero.block.internal.DatedNodeAddressBook;
import org.hiero.block.tools.mirrornode.BlockTimeReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

/** Unit tests for {@link ConvertAddressBookHistoryCommand}. */
class ConvertAddressBookHistoryCommandTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Path TEST_BLOCK_TIMES_FILE = Path.of("src/test/resources/metadata/block_times.bin");

    @TempDir
    Path tempDir;

    Path inputFile;
    Path outputFile;

    @BeforeEach
    void setUp() {
        inputFile = tempDir.resolve("address-book-history.json");
        outputFile = tempDir.resolve("rsa-bootstrap-roster-history.json");
    }

    @Nested
    @DisplayName("Output shape & range chaining")
    class OutputShape {

        @Test
        @DisplayName("Resolves start blocks and chains end blocks (last = null)")
        void chainsRanges() throws IOException {
            // Pick three real block numbers in the bundled test block_times.bin and build a
            // 3-era history at those exact block-time instants. Expected start blocks are the
            // picked values; end blocks chain to next-1, last is null.
            final long[] picks = new long[] {100, 5000, 12000};
            final List<Instant> instants = blockInstants(picks);

            writeHistory(
                    inputFile,
                    List.of(
                            dated(instants.get(0), addressBook(0, 1, "deadbeef00")),
                            dated(instants.get(1), addressBook(0, 1, "deadbeef01")),
                            dated(instants.get(2), addressBook(0, 1, "deadbeef02"))));

            int exit = execute();
            assertEquals(0, exit);

            JsonNode root = JSON.readTree(outputFile.toFile());
            JsonNode entries = root.get("entries");
            assertNotNull(entries);
            assertEquals(3, entries.size());

            assertEquals(picks[0], entries.get(0).get("startBlock").asLong());
            assertEquals(picks[1] - 1, entries.get(0).get("endBlock").asLong());

            assertEquals(picks[1], entries.get(1).get("startBlock").asLong());
            assertEquals(picks[2] - 1, entries.get(1).get("endBlock").asLong());

            assertEquals(picks[2], entries.get(2).get("startBlock").asLong());
            assertTrue(entries.get(2).get("endBlock").isNull(), "last era's endBlock must be null");
        }

        @Test
        @DisplayName("Preserves blockTimestamp seconds + nanos per era")
        void preservesTimestamp() throws IOException {
            final long[] picks = new long[] {100, 5000};
            final List<Instant> instants = blockInstants(picks);

            writeHistory(
                    inputFile,
                    List.of(
                            dated(instants.get(0), addressBook(0, 1, "aa")),
                            dated(instants.get(1), addressBook(0, 1, "bb"))));

            assertEquals(0, execute());
            JsonNode entries = JSON.readTree(outputFile.toFile()).get("entries");
            assertEquals(
                    instants.get(0).getEpochSecond(),
                    entries.get(0).get("blockTimestamp").get("seconds").asLong());
            assertEquals(
                    instants.get(0).getNano(),
                    entries.get(0).get("blockTimestamp").get("nanos").asInt());
            assertEquals(
                    instants.get(1).getEpochSecond(),
                    entries.get(1).get("blockTimestamp").get("seconds").asLong());
        }
    }

    @Nested
    @DisplayName("AddressBook payload (Option B / Base64)")
    class AddressBookPayload {

        @Test
        @DisplayName("Round-trips through Base64 + NodeAddressBook.PROTOBUF.parse")
        void roundTrips() throws Exception {
            final long[] picks = new long[] {100, 5000};
            final List<Instant> instants = blockInstants(picks);

            NodeAddressBook era0 = addressBook(0, 2, "0000aaaa");
            NodeAddressBook era1 = addressBook(0, 3, "1111bbbb");
            writeHistory(inputFile, List.of(dated(instants.get(0), era0), dated(instants.get(1), era1)));

            assertEquals(0, execute());

            JsonNode entries = JSON.readTree(outputFile.toFile()).get("entries");
            NodeAddressBook decoded0 = NodeAddressBook.PROTOBUF.parse(Bytes.wrap(
                    Base64.getDecoder().decode(entries.get(0).get("addressBook").asText())));
            NodeAddressBook decoded1 = NodeAddressBook.PROTOBUF.parse(Bytes.wrap(
                    Base64.getDecoder().decode(entries.get(1).get("addressBook").asText())));

            assertEquals(era0, decoded0);
            assertEquals(era1, decoded1);
            assertNotEquals(decoded0, decoded1, "distinct per-era books must round-trip distinctly");
        }
    }

    @Nested
    @DisplayName("Input ordering & validation")
    class InputHandling {

        @Test
        @DisplayName("Sorts unordered eras by timestamp ascending before resolving")
        void sortsUnordered() throws IOException {
            final long[] picks = new long[] {100, 5000, 12000};
            final List<Instant> instants = blockInstants(picks);

            // Write in scrambled order: newest, oldest, middle.
            writeHistory(
                    inputFile,
                    List.of(
                            dated(instants.get(2), addressBook(0, 1, "cc")),
                            dated(instants.get(0), addressBook(0, 1, "aa")),
                            dated(instants.get(1), addressBook(0, 1, "bb"))));

            assertEquals(0, execute());

            JsonNode entries = JSON.readTree(outputFile.toFile()).get("entries");
            assertEquals(picks[0], entries.get(0).get("startBlock").asLong());
            assertEquals(picks[1], entries.get(1).get("startBlock").asLong());
            assertEquals(picks[2], entries.get(2).get("startBlock").asLong());
        }

        @Test
        @DisplayName("Fails when input file is missing")
        void missingInput() {
            int exit = new CommandLine(new ConvertAddressBookHistoryCommand())
                    .execute(
                            "--input", tempDir.resolve("nope.json").toString(),
                            "--output", outputFile.toString(),
                            "--block-times-file", TEST_BLOCK_TIMES_FILE.toString());
            assertEquals(1, exit);
        }

        @Test
        @DisplayName("Fails when block_times.bin is missing")
        void missingBlockTimes() throws IOException {
            final long[] picks = new long[] {100};
            final List<Instant> instants = blockInstants(picks);
            writeHistory(inputFile, List.of(dated(instants.get(0), addressBook(0, 1, "aa"))));

            int exit = new CommandLine(new ConvertAddressBookHistoryCommand())
                    .execute(
                            "--input", inputFile.toString(),
                            "--output", outputFile.toString(),
                            "--block-times-file", tempDir.resolve("nope.bin").toString());
            assertEquals(1, exit);
        }

        @Test
        @DisplayName("Fails (exit 1) on a history with zero eras")
        void emptyHistory() throws IOException {
            writeHistory(inputFile, List.of());
            assertEquals(1, execute());
        }

        @Test
        @DisplayName("Malformed input surfaces as a non-zero exit (parse error, not IO error)")
        void malformedInput() throws IOException {
            Files.writeString(inputFile, "{ this is not valid AddressBookHistory JSON ::: ");
            int exit = execute();
            assertNotEquals(0, exit, "malformed input must not exit 0");
        }
    }

    @Nested
    @DisplayName("Edge cases")
    class EdgeCases {

        @Test
        @DisplayName("Single-era history: endBlock is null, one entry")
        void singleEra() throws IOException {
            final long[] picks = new long[] {7};
            final List<Instant> instants = blockInstants(picks);
            writeHistory(inputFile, List.of(dated(instants.get(0), addressBook(0, 1, "01"))));

            assertEquals(0, execute());

            JsonNode entries = JSON.readTree(outputFile.toFile()).get("entries");
            assertEquals(1, entries.size());
            assertEquals(picks[0], entries.get(0).get("startBlock").asLong());
            assertTrue(entries.get(0).get("endBlock").isNull(), "single-era history must emit null endBlock");
        }

        @Test
        @DisplayName("Creates non-existent output directory(s) before writing")
        void createsNestedOutputDir() throws IOException {
            final long[] picks = new long[] {100};
            final List<Instant> instants = blockInstants(picks);
            writeHistory(inputFile, List.of(dated(instants.get(0), addressBook(0, 1, "aa"))));

            Path nestedOut = tempDir.resolve("a/b/c/roster-history.json");
            int exit = new CommandLine(new ConvertAddressBookHistoryCommand())
                    .execute(
                            "--input", inputFile.toString(),
                            "--output", nestedOut.toString(),
                            "--block-times-file", TEST_BLOCK_TIMES_FILE.toString());
            assertEquals(0, exit);
            assertTrue(Files.isRegularFile(nestedOut), "output file must exist in created nested dir");
        }

        @Test
        @DisplayName("Atomic write: no leftover .tmp sibling after a successful run")
        void atomicWriteLeavesNoTmpFile() throws IOException {
            final long[] picks = new long[] {100};
            final List<Instant> instants = blockInstants(picks);
            writeHistory(inputFile, List.of(dated(instants.get(0), addressBook(0, 1, "aa"))));

            assertEquals(0, execute());
            Path tmp = outputFile.resolveSibling(outputFile.getFileName() + ".tmp");
            assertTrue(Files.exists(outputFile), "output must exist after successful run");
            assertTrue(!Files.exists(tmp), "atomic write must not leave a .tmp sibling on success");
        }

        @Test
        @DisplayName("Ties on seconds are broken by nanos (ascending)")
        void tieBreakByNanos() throws IOException {
            // Build two entries at the same epoch second but different nanos; the smaller-nanos
            // entry must sort first. Use real block instants so BlockTimeReader can resolve them,
            // and perturb only nanos to create the tie. Pick two blocks where the natural nanos
            // happen to differ -- we'll just synthesize timestamps with identical seconds.
            final long[] picks = new long[] {100, 200};
            final List<Instant> instants = blockInstants(picks);
            Instant base = instants.get(0);
            // Era A: base, Era B: base with +1 nano (still resolvable: getNearestBlockAfterTime
            // returns the same/next block). Both eras valid, just want to assert sort order.
            Instant a = base;
            Instant b = base.plusNanos(1);

            // Write scrambled: b before a.
            writeHistory(inputFile, List.of(dated(b, addressBook(0, 1, "bb")), dated(a, addressBook(0, 1, "aa"))));

            assertEquals(0, execute());

            JsonNode entries = JSON.readTree(outputFile.toFile()).get("entries");
            // Era A (smaller nanos) must come first.
            assertEquals(
                    a.getNano(),
                    entries.get(0).get("blockTimestamp").get("nanos").asInt());
            assertEquals(
                    b.getNano(),
                    entries.get(1).get("blockTimestamp").get("nanos").asInt());
        }
    }

    @Nested
    @DisplayName("Realistic scale + output format")
    class RealisticScale {

        @Test
        @DisplayName("~10-era history: all ranges chain, last endBlock is null")
        void tenEraHistory() throws IOException {
            final long[] picks = new long[] {0, 50, 500, 1500, 3000, 5000, 8000, 12000, 15000, 18000};
            final List<Instant> instants = blockInstants(picks);
            List<DatedNodeAddressBook> books = new ArrayList<>();
            for (int i = 0; i < picks.length; i++) {
                books.add(dated(instants.get(i), addressBook(0, 3, String.format("e%02d", i))));
            }
            writeHistory(inputFile, books);

            assertEquals(0, execute());

            JsonNode entries = JSON.readTree(outputFile.toFile()).get("entries");
            assertEquals(picks.length, entries.size());
            for (int i = 0; i < picks.length; i++) {
                assertEquals(picks[i], entries.get(i).get("startBlock").asLong(), "era " + i + " startBlock");
                if (i < picks.length - 1) {
                    assertEquals(
                            picks[i + 1] - 1,
                            entries.get(i).get("endBlock").asLong(),
                            "era " + i + " endBlock chains to next.start-1");
                } else {
                    assertTrue(entries.get(i).get("endBlock").isNull(), "final era endBlock must be null");
                }
            }
        }

        @Test
        @DisplayName("Output JSON has only the top-level \"entries\" key and is pretty-printed")
        void outputIsPrettyPrintedAndMinimalRoot() throws IOException {
            final long[] picks = new long[] {100, 5000};
            final List<Instant> instants = blockInstants(picks);
            writeHistory(
                    inputFile,
                    List.of(
                            dated(instants.get(0), addressBook(0, 1, "aa")),
                            dated(instants.get(1), addressBook(0, 1, "bb"))));

            assertEquals(0, execute());

            // Top-level structure: exactly one field, named "entries".
            JsonNode root = JSON.readTree(outputFile.toFile());
            assertEquals(1, root.size(), "root must have a single field");
            assertNotNull(root.get("entries"));

            // Pretty-printed: output spans multiple lines (Jackson INDENT_OUTPUT).
            String raw = Files.readString(outputFile);
            assertTrue(raw.contains("\n"), "pretty-printed output must contain newlines");
            assertTrue(raw.contains("\n  \"entries\""), "pretty-printed output should indent the entries key");
        }

        @Test
        @DisplayName("Default --output points at the BN's application-state directory")
        void defaultOutputUsesBnApplicationStateDir() {
            assertEquals(
                    Path.of("/opt/hiero/block-node/application-state/rsa-bootstrap-roster-history.json"),
                    ConvertAddressBookHistoryCommand.DEFAULT_OUTPUT_PATH,
                    "default output must land where the BN bootstrap reads from");
        }

        @Test
        @DisplayName("--help prints usage with the command name and key options")
        void helpOutput() {
            java.io.StringWriter out = new java.io.StringWriter();
            CommandLine cmd = new CommandLine(new ConvertAddressBookHistoryCommand());
            cmd.setOut(new java.io.PrintWriter(out));
            int exit = cmd.execute("--help");
            assertEquals(0, exit);
            String text = out.toString();
            assertTrue(text.contains("convert-address-book-history"));
            assertTrue(text.contains("--input"));
            assertTrue(text.contains("--output"));
            assertTrue(text.contains("--block-times-file"));
        }
    }

    // ----- helpers -----

    private int execute() {
        return new CommandLine(new ConvertAddressBookHistoryCommand())
                .execute(
                        "--input", inputFile.toString(),
                        "--output", outputFile.toString(),
                        "--block-times-file", TEST_BLOCK_TIMES_FILE.toString());
    }

    private static List<Instant> blockInstants(long[] blockNumbers) throws IOException {
        try (BlockTimeReader reader = new BlockTimeReader(TEST_BLOCK_TIMES_FILE)) {
            return Arrays.stream(blockNumbers).mapToObj(reader::getBlockInstant).toList();
        }
    }

    private static DatedNodeAddressBook dated(Instant instant, NodeAddressBook book) {
        return new DatedNodeAddressBook(new Timestamp(instant.getEpochSecond(), instant.getNano()), book);
    }

    private static NodeAddressBook addressBook(long firstNodeId, int count, String rsaPubKeyHexBase) {
        List<NodeAddress> nodes = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            long nodeId = firstNodeId + i;
            nodes.add(NodeAddress.newBuilder()
                    .nodeId(nodeId)
                    .memo(Bytes.wrap(("0.0." + (nodeId + 3)).getBytes(StandardCharsets.UTF_8)))
                    .rsaPubKey(rsaPubKeyHexBase + String.format("%02x", i))
                    .nodeAccountId(AccountID.newBuilder()
                            .shardNum(0)
                            .realmNum(0)
                            .accountNum(nodeId + 3)
                            .build())
                    .build());
        }
        return new NodeAddressBook(nodes);
    }

    private static void writeHistory(Path file, List<DatedNodeAddressBook> books) throws IOException {
        AddressBookHistory history = new AddressBookHistory(books);
        try (WritableStreamingData out = new WritableStreamingData(Files.newOutputStream(file))) {
            AddressBookHistory.JSON.write(history, out);
        }
    }
}
