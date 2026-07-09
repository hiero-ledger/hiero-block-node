// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.hapi.node.base.AccountID;
import com.hedera.hapi.node.base.NodeAddress;
import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.hedera.pbj.runtime.io.stream.ReadableStreamingData;
import com.hedera.pbj.runtime.io.stream.WritableStreamingData;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.hiero.block.api.RangedAddressBookHistory;
import org.hiero.block.api.RangedNodeAddressBook;
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

    private static final Path TEST_BLOCK_TIMES_FILE = Path.of("src/test/resources/metadata/block_times.bin");
    private static final long OPEN_ENDED = ConvertAddressBookHistoryCommand.OPEN_ENDED_END_BLOCK;

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
        @DisplayName("Resolves start blocks and chains end blocks (last = OPEN_ENDED sentinel)")
        void chainsRanges() throws IOException {
            // Pick three real block numbers in the bundled test block_times.bin and build a
            // 3-era history at those exact block-time instants. Expected start blocks are the
            // picked values; end blocks chain to next-1, last uses the open-ended sentinel.
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

            RangedAddressBookHistory out = readOutput();
            assertEquals(3, out.addressBooks().size());

            assertEquals(picks[0], out.addressBooks().get(0).startBlock());
            assertEquals(picks[1] - 1, out.addressBooks().get(0).endBlock());

            assertEquals(picks[1], out.addressBooks().get(1).startBlock());
            assertEquals(picks[2] - 1, out.addressBooks().get(1).endBlock());

            assertEquals(picks[2], out.addressBooks().get(2).startBlock());
            assertEquals(
                    OPEN_ENDED,
                    out.addressBooks().get(2).endBlock(),
                    "last era's endBlock must be the open-ended sentinel");
        }
    }

    @Nested
    @DisplayName("AddressBook payload (nested NodeAddressBook)")
    class AddressBookPayload {

        @Test
        @DisplayName("Per-era addressBook round-trips (nested, not base64) via NodeAddressBook.equals")
        void roundTrips() throws Exception {
            final long[] picks = new long[] {100, 5000};
            final List<Instant> instants = blockInstants(picks);

            NodeAddressBook era0 = addressBook(0, 2, "0000aaaa");
            NodeAddressBook era1 = addressBook(0, 3, "1111bbbb");
            writeHistory(inputFile, List.of(dated(instants.get(0), era0), dated(instants.get(1), era1)));

            assertEquals(0, execute());

            RangedAddressBookHistory out = readOutput();
            assertEquals(slim(era0), out.addressBooks().get(0).addressBook());
            assertEquals(slim(era1), out.addressBooks().get(1).addressBook());
            assertNotEquals(
                    out.addressBooks().get(0).addressBook(),
                    out.addressBooks().get(1).addressBook(),
                    "distinct per-era books must round-trip distinctly");
        }

        @Test
        @DisplayName("Emitted JSON is the nested NodeAddressBook shape (nodeAddress[]), no base64 or blockTimestamp")
        void nestedShape() throws IOException {
            final long[] picks = new long[] {100};
            final List<Instant> instants = blockInstants(picks);
            writeHistory(inputFile, List.of(dated(instants.get(0), addressBook(0, 1, "aabb"))));

            assertEquals(0, execute());

            String json = Files.readString(outputFile);
            assertTrue(json.contains("\"addressBooks\""), "top-level key must be 'addressBooks'");
            assertTrue(json.contains("\"addressBook\""), "each entry must have a nested 'addressBook' object");
            assertTrue(
                    json.contains("\"nodeAddress\""),
                    "nested addressBook must serialize as NodeAddressBook (nodeAddress[])");
            assertTrue(json.contains("\"RSAPubKey\""), "nested nodeAddress must expose RSAPubKey");
            assertTrue(!json.contains("\"blockTimestamp\""), "blockTimestamp must not be emitted");
            assertTrue(!json.contains("\"entries\""), "legacy top-level 'entries' must not be emitted");
        }

        @Test
        @DisplayName("Emitted nodeAddress carries only nodeId + RSAPubKey; other fields are dropped")
        void slimmedNodeAddressFields() throws IOException {
            // Build an input with fields the command should drop: memo, nodeAccountId,
            // description, serviceEndpoint, stake, nodeCertHash. The emitted JSON must
            // carry only nodeId and RSAPubKey per nodeAddress.
            final long[] picks = new long[] {100};
            final List<Instant> instants = blockInstants(picks);
            writeHistory(inputFile, List.of(dated(instants.get(0), addressBook(0, 2, "cafe"))));

            assertEquals(0, execute());

            String json = Files.readString(outputFile);
            // Positive: nodeId + RSAPubKey are present.
            assertTrue(json.contains("\"nodeId\""), "nodeId must be present");
            assertTrue(json.contains("\"RSAPubKey\""), "RSAPubKey must be present");
            // Negative: fields the test helper populates but the command drops must be absent.
            assertTrue(!json.contains("\"memo\""), "memo must be dropped");
            assertTrue(!json.contains("\"nodeAccountId\""), "nodeAccountId must be dropped");
            assertTrue(!json.contains("\"description\""), "description must be dropped");
            assertTrue(!json.contains("\"serviceEndpoint\""), "serviceEndpoint must be dropped");
            assertTrue(!json.contains("\"stake\""), "stake must be dropped");
            assertTrue(!json.contains("\"nodeCertHash\""), "nodeCertHash must be dropped");
        }

        @Test
        @DisplayName("startBlock and endBlock are always present in raw JSON, even when 0 / -1 defaults")
        void explicitStartAndEndBlockPresent() throws IOException {
            // First-era startBlock resolves to 0 (target == genesis) and last-era endBlock is
            // the OPEN_ENDED_END_BLOCK sentinel (-1). PBJ's JSON codec elides proto3 uint64
            // defaults, so without our post-write fixup both fields would go missing.
            // Operators reading the roster should always see both fields spelled out.
            final long[] picks = new long[] {0, 5000, 12000};
            final List<Instant> instants = blockInstants(picks);
            writeHistory(
                    inputFile,
                    List.of(
                            dated(instants.get(0), addressBook(0, 1, "aa")),
                            dated(instants.get(1), addressBook(0, 1, "bb")),
                            dated(instants.get(2), addressBook(0, 1, "cc"))));

            assertEquals(0, execute());

            final String json = Files.readString(outputFile);
            final long startBlockCount =
                    json.lines().filter(l -> l.contains("\"startBlock\"")).count();
            final long endBlockCount =
                    json.lines().filter(l -> l.contains("\"endBlock\"")).count();
            assertEquals(3L, startBlockCount, "every entry must emit a startBlock line");
            assertEquals(3L, endBlockCount, "every entry must emit an endBlock line");

            // PBJ writes uint64 fields as JSON strings; our post-write fixup follows the
            // same convention so the shape stays uniform for the BN parser.
            assertTrue(
                    json.contains("\"startBlock\" : \"0\""),
                    "first era's startBlock=0 must be written explicitly as a JSON string");
            assertTrue(
                    json.contains("\"endBlock\" : \"-1\""),
                    "last era's endBlock=-1 sentinel must be written explicitly as a JSON string");

            // PBJ parse-side must still reconstruct the same values from the fixed-up JSON.
            final RangedAddressBookHistory out = readOutput();
            assertEquals(0L, out.addressBooks().get(0).startBlock());
            assertEquals(-1L, out.addressBooks().get(2).endBlock());
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

            RangedAddressBookHistory out = readOutput();
            assertEquals(picks[0], out.addressBooks().get(0).startBlock());
            assertEquals(picks[1], out.addressBooks().get(1).startBlock());
            assertEquals(picks[2], out.addressBooks().get(2).startBlock());
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

        @Test
        @DisplayName("Fails loudly when an era timestamp falls before block_times.bin coverage")
        void beforeCoverageFailsLoudly() throws IOException {
            // Instant.EPOCH (1970-01-01) is guaranteed before any real block-time entry, so the
            // reader will clamp to block 0 while the target is earlier than block 0's time.
            writeHistory(inputFile, List.of(dated(Instant.EPOCH, addressBook(0, 1, "aa"))));
            int exit = execute();
            assertEquals(1, exit, "resolution before file coverage must exit 1, not silently produce zeros");
            assertTrue(!Files.exists(outputFile), "output file must not be written on resolution failure");
        }

        @Test
        @DisplayName("Fails loudly when an era timestamp falls after block_times.bin coverage")
        void afterCoverageFailsLoudly() throws IOException {
            // Instant.parse("9999-01-01T00:00:00Z") is guaranteed past any real block-time entry.
            Instant farFuture = Instant.parse("9999-01-01T00:00:00Z");
            writeHistory(inputFile, List.of(dated(farFuture, addressBook(0, 1, "aa"))));
            int exit = execute();
            assertEquals(1, exit, "resolution past file coverage must exit 1, not silently clamp to end");
            assertTrue(!Files.exists(outputFile), "output file must not be written on resolution failure");
        }
    }

    @Nested
    @DisplayName("Edge cases")
    class EdgeCases {

        @Test
        @DisplayName("Single-era history: endBlock is OPEN_ENDED, one entry")
        void singleEra() throws IOException {
            final long[] picks = new long[] {7};
            final List<Instant> instants = blockInstants(picks);
            writeHistory(inputFile, List.of(dated(instants.get(0), addressBook(0, 1, "01"))));

            assertEquals(0, execute());

            RangedAddressBookHistory out = readOutput();
            assertEquals(1, out.addressBooks().size());
            assertEquals(picks[0], out.addressBooks().get(0).startBlock());
            assertEquals(
                    OPEN_ENDED,
                    out.addressBooks().get(0).endBlock(),
                    "single-era history must emit OPEN_ENDED endBlock");
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
            // entry must sort first. The eras' payload address books differ so we can identify
            // which one comes out first by inspecting the RSA pubkey.
            final long[] picks = new long[] {100, 200};
            final List<Instant> instants = blockInstants(picks);
            Instant base = instants.get(0);
            Instant a = base;
            Instant b = base.plusNanos(1);

            NodeAddressBook bookA = addressBook(0, 1, "aa");
            NodeAddressBook bookB = addressBook(0, 1, "bb");
            // Write scrambled: b before a.
            writeHistory(inputFile, List.of(dated(b, bookB), dated(a, bookA)));

            assertEquals(0, execute());

            RangedAddressBookHistory out = readOutput();
            // Era A (smaller nanos) must come first.
            assertEquals(slim(bookA), out.addressBooks().get(0).addressBook(), "smaller-nanos era must sort first");
            assertEquals(slim(bookB), out.addressBooks().get(1).addressBook());
        }
    }

    @Nested
    @DisplayName("Realistic scale + defaults")
    class RealisticScale {

        @Test
        @DisplayName("~10-era history: all ranges chain, last endBlock = OPEN_ENDED")
        void tenEraHistory() throws IOException {
            final long[] picks = new long[] {0, 50, 500, 1500, 3000, 5000, 8000, 12000, 15000, 18000};
            final List<Instant> instants = blockInstants(picks);
            List<DatedNodeAddressBook> books = new ArrayList<>();
            for (int i = 0; i < picks.length; i++) {
                books.add(dated(instants.get(i), addressBook(0, 3, String.format("e%02d", i))));
            }
            writeHistory(inputFile, books);

            assertEquals(0, execute());

            RangedAddressBookHistory out = readOutput();
            assertEquals(picks.length, out.addressBooks().size());
            for (int i = 0; i < picks.length; i++) {
                RangedNodeAddressBook era = out.addressBooks().get(i);
                assertEquals(picks[i], era.startBlock(), "era " + i + " startBlock");
                if (i < picks.length - 1) {
                    assertEquals(picks[i + 1] - 1, era.endBlock(), "era " + i + " endBlock chains to next.start-1");
                } else {
                    assertEquals(OPEN_ENDED, era.endBlock(), "final era endBlock must be OPEN_ENDED");
                }
            }
        }

        @Test
        @DisplayName("Default --output points at the BN's application-state directory")
        void defaultOutputUsesBnApplicationStateDir() {
            assertEquals(
                    Path.of("/opt/hiero/block-node/application-state/rsa-bootstrap-roster.json"),
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

    private RangedAddressBookHistory readOutput() throws IOException {
        try (ReadableStreamingData in = new ReadableStreamingData(Files.newInputStream(outputFile))) {
            return RangedAddressBookHistory.JSON.parse(in);
        } catch (Exception e) {
            throw new IOException("Failed to parse output as RangedAddressBookHistory: " + e.getMessage(), e);
        }
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

    /**
     * Mirror the command's slimming: keep only {@code nodeId} and {@code RSAPubKey} on each
     * {@link NodeAddress}. Tests that compare the emitted addressBook against a hand-built
     * input must slim the input first because the command drops the other fields.
     */
    private static NodeAddressBook slim(NodeAddressBook book) {
        List<NodeAddress> slim = new ArrayList<>(book.nodeAddress().size());
        for (NodeAddress addr : book.nodeAddress()) {
            slim.add(NodeAddress.newBuilder()
                    .nodeId(addr.nodeId())
                    .rsaPubKey(addr.rsaPubKey())
                    .build());
        }
        return new NodeAddressBook(slim);
    }

    private static void writeHistory(Path file, List<DatedNodeAddressBook> books) throws IOException {
        AddressBookHistory history = new AddressBookHistory(books);
        try (WritableStreamingData out = new WritableStreamingData(Files.newOutputStream(file))) {
            AddressBookHistory.JSON.write(history, out);
        }
    }
}
