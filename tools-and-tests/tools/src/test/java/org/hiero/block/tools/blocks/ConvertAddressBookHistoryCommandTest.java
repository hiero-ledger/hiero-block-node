// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.hapi.node.base.AccountID;
import com.hedera.hapi.node.base.NodeAddress;
import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.hedera.pbj.runtime.io.stream.ReadableStreamingData;
import com.hedera.pbj.runtime.io.stream.WritableStreamingData;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
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

        @Test
        @DisplayName("nodeId=0 is written explicitly per nodeAddress even though PBJ elides uint64 default")
        void explicitNodeIdZeroPresent() throws IOException {
            // Genesis-node-0 has nodeId=0, and PBJ elides proto3 uint64 defaults on write.
            // The BN's roster verifier routes by nodeId; without the fixup, node 0 loses
            // its identifier in the emitted JSON. Two-node era (0, 1) so we can also
            // confirm node 1's nodeId survives untouched.
            final long[] picks = new long[] {100};
            final List<Instant> instants = blockInstants(picks);
            writeHistory(inputFile, List.of(dated(instants.get(0), addressBook(0, 2, "dead"))));

            assertEquals(0, execute());

            final String json = Files.readString(outputFile);
            // PBJ writes uint64 as JSON strings; nodeId="0" for node 0, nodeId="1" for node 1.
            assertTrue(
                    json.contains("\"nodeId\" : \"0\""),
                    "node 0's nodeId must be written explicitly as \"0\" after the fixup");
            assertTrue(json.contains("\"nodeId\" : \"1\""), "node 1's nodeId must still be present");

            // Every nodeAddress entry must have a nodeId — no missing IDs after the fixup.
            final RangedAddressBookHistory out = readOutput();
            assertEquals(
                    2, out.addressBooks().get(0).addressBook().nodeAddress().size());
            assertEquals(
                    0L,
                    out.addressBooks().get(0).addressBook().nodeAddress().get(0).nodeId());
            assertEquals(
                    1L,
                    out.addressBooks().get(0).addressBook().nodeAddress().get(1).nodeId());
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

    @Nested
    @DisplayName("Legacy eras without node ids (#3955)")
    class LegacyNodeIds {

        @Test
        @DisplayName("Memo-only genesis era gets distinct ids from the node accounts (accountNum - 3)")
        void memoOnlyEraDerivesIds() throws IOException {
            final List<Instant> instants = blockInstants(new long[] {100});
            writeHistory(inputFile, List.of(dated(instants.get(0), legacyMemoOnly(3, 13))));

            assertEquals(0, execute());

            assertEquals(List.of(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L), nodeIds(readOutput(), 0));
        }

        @Test
        @DisplayName("Ids follow the memo account of each node, not its position in the book")
        void idsFollowTheAccountNotThePosition() throws IOException {
            final List<Instant> instants = blockInstants(new long[] {100});
            final NodeAddressBook shuffled = new NodeAddressBook(
                    List.of(legacyNode("0.0.7", "aa"), legacyNode("0.0.3", "bb"), legacyNode("0.0.5", "cc")));
            writeHistory(inputFile, List.of(dated(instants.get(0), shuffled)));

            assertEquals(0, execute());

            assertEquals(List.of(4L, 0L, 2L), nodeIds(readOutput(), 0));
        }

        @Test
        @DisplayName("Account-only nodes (no memo) derive their ids from nodeAccountId")
        void accountOnlyEraDerivesIds() throws IOException {
            final List<Instant> instants = blockInstants(new long[] {100});
            final List<NodeAddress> nodes = new ArrayList<>();
            for (int account = 3; account <= 6; account++) {
                nodes.add(NodeAddress.newBuilder()
                        .rsaPubKey("ab" + account)
                        .nodeAccountId(
                                AccountID.newBuilder().accountNum(account).build())
                        .build());
            }
            writeHistory(inputFile, List.of(dated(instants.get(0), new NodeAddressBook(nodes))));

            assertEquals(0, execute());

            assertEquals(List.of(0L, 1L, 2L, 3L), nodeIds(readOutput(), 0));
        }

        @Test
        @DisplayName("An era with unique, non-sequential ids is left untouched even if its accounts differ")
        void uniqueIdsAreNotRederived() throws IOException {
            final List<Instant> instants = blockInstants(new long[] {100});
            // ids 0, 1, 4, 5 on accounts 10..13: deriving would give 7, 8, 9, 10
            final List<NodeAddress> nodes = new ArrayList<>();
            final long[] ids = {0, 1, 4, 5};
            for (int i = 0; i < ids.length; i++) {
                nodes.add(NodeAddress.newBuilder()
                        .nodeId(ids[i])
                        .rsaPubKey("cd0" + i)
                        .nodeAccountId(AccountID.newBuilder().accountNum(10 + i).build())
                        .build());
            }
            writeHistory(inputFile, List.of(dated(instants.get(0), new NodeAddressBook(nodes))));

            assertEquals(0, execute());

            assertEquals(List.of(0L, 1L, 4L, 5L), nodeIds(readOutput(), 0));
        }

        @Test
        @DisplayName("A real node 0 in an era with unique ids stays node 0")
        void realNodeZeroIsKept() throws IOException {
            final List<Instant> instants = blockInstants(new long[] {100});
            final NodeAddressBook book = new NodeAddressBook(List.of(
                    NodeAddress.newBuilder()
                            .nodeId(0)
                            .rsaPubKey("e0")
                            .nodeAccountId(AccountID.newBuilder().accountNum(20).build())
                            .build(),
                    NodeAddress.newBuilder()
                            .nodeId(2)
                            .rsaPubKey("e1")
                            .nodeAccountId(AccountID.newBuilder().accountNum(21).build())
                            .build()));
            writeHistory(inputFile, List.of(dated(instants.get(0), book)));

            assertEquals(0, execute());

            assertEquals(List.of(0L, 2L), nodeIds(readOutput(), 0));
        }

        @Test
        @DisplayName("Legacy era 0 plus a normal era 1: era 0 is derived, era 1 is unchanged")
        void mixedHistory() throws IOException {
            final List<Instant> instants = blockInstants(new long[] {100, 5000});
            writeHistory(
                    inputFile,
                    List.of(
                            dated(instants.get(0), legacyMemoOnly(3, 13)),
                            dated(instants.get(1), addressBook(4, 3, "ff"))));

            assertEquals(0, execute());

            final RangedAddressBookHistory out = readOutput();
            assertEquals(List.of(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L), nodeIds(out, 0));
            assertEquals(List.of(4L, 5L, 6L), nodeIds(out, 1));
        }

        @Test
        @DisplayName("Era still duplicated after deriving fails with exit 1, names the era and writes nothing")
        void stillDuplicateFailsWithoutOutput() throws IOException {
            final List<Instant> instants = blockInstants(new long[] {100});
            final NodeAddressBook book = new NodeAddressBook(
                    List.of(legacyNode("0.0.5", "aa"), legacyNode("0.0.5", "bb"), legacyNode("0.0.6", "cc")));
            writeHistory(inputFile, List.of(dated(instants.get(0), book)));

            final ExecResult result = executeCapturingStderr();

            assertEquals(1, result.exit());
            assertTrue(result.stderr().contains("era 0"), result.stderr());
            assertTrue(result.stderr().contains("not unique"), result.stderr());
            assertFalse(Files.exists(outputFile), "no output may be written when an era cannot be resolved");
        }

        @Test
        @DisplayName("Unparsable memo in a duplicate era fails with exit 1 and writes nothing")
        void unparsableMemoFailsWithoutOutput() throws IOException {
            final List<Instant> instants = blockInstants(new long[] {100});
            final NodeAddressBook book =
                    new NodeAddressBook(List.of(legacyNode("not-an-account", "aa"), legacyNode("0.0.4", "bb")));
            writeHistory(inputFile, List.of(dated(instants.get(0), book)));

            final ExecResult result = executeCapturingStderr();

            assertEquals(1, result.exit());
            assertTrue(result.stderr().contains("era 0"), result.stderr());
            assertFalse(Files.exists(outputFile));
        }

        @Test
        @DisplayName("A bad era later in the history still leaves no output, even if era 0 resolved")
        void badLaterEraWritesNothing() throws IOException {
            final List<Instant> instants = blockInstants(new long[] {100, 5000});
            final NodeAddressBook bad =
                    new NodeAddressBook(List.of(legacyNode("0.0.5", "aa"), legacyNode("0.0.5", "bb")));
            writeHistory(inputFile, List.of(dated(instants.get(0), legacyMemoOnly(3, 4)), dated(instants.get(1), bad)));

            final ExecResult result = executeCapturingStderr();

            assertEquals(1, result.exit());
            assertTrue(result.stderr().contains("era 1"), result.stderr());
            assertFalse(Files.exists(outputFile));
        }

        @Test
        @DisplayName("resolveNodeIds returns the same book when ids are already unique")
        void resolveReturnsSameInstanceWhenUnique() {
            final NodeAddressBook unique = addressBook(0, 3, "aa");

            assertSame(unique, ConvertAddressBookHistoryCommand.resolveNodeIds(unique));
        }

        @Test
        @DisplayName("resolveNodeIds rejects an account below the node account offset")
        void resolveRejectsNegativeId() {
            final NodeAddressBook book =
                    new NodeAddressBook(List.of(legacyNode("0.0.2", "aa"), legacyNode("0.0.3", "bb")));

            assertThrows(IllegalArgumentException.class, () -> ConvertAddressBookHistoryCommand.resolveNodeIds(book));
        }
    }

    // ----- helpers -----

    private record ExecResult(int exit, String stderr) {}

    private ExecResult executeCapturingStderr() {
        final PrintStream originalErr = System.err;
        final ByteArrayOutputStream captured = new ByteArrayOutputStream();
        System.setErr(new PrintStream(captured, true, StandardCharsets.UTF_8));
        try {
            final int exit = execute();
            return new ExecResult(exit, captured.toString(StandardCharsets.UTF_8));
        } finally {
            System.setErr(originalErr);
        }
    }

    private static List<Long> nodeIds(RangedAddressBookHistory history, int era) {
        return history.addressBooks().get(era).addressBook().nodeAddress().stream()
                .map(NodeAddress::nodeId)
                .toList();
    }

    /** A legacy genesis style node: only the account memo (for example "0.0.7"), no nodeId. */
    private static NodeAddress legacyNode(String memo, String rsaPubKey) {
        return NodeAddress.newBuilder()
                .memo(Bytes.wrap(memo.getBytes(StandardCharsets.UTF_8)))
                .rsaPubKey(rsaPubKey)
                .build();
    }

    private static NodeAddressBook legacyMemoOnly(int firstAccount, int count) {
        final List<NodeAddress> nodes = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            nodes.add(legacyNode("0.0." + (firstAccount + i), String.format("a1%02x", i)));
        }
        return new NodeAddressBook(nodes);
    }

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
