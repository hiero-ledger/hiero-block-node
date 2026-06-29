// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.stream.ReadableStreamingData;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Callable;
import org.hiero.block.internal.AddressBookHistory;
import org.hiero.block.internal.DatedNodeAddressBook;
import org.hiero.block.tools.metadata.MetadataFiles;
import org.hiero.block.tools.mirrornode.BlockTimeReader;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Option;

/**
 * Convert the CLI's address-book history JSON into a block-number-scoped roster file the BN can load
 * to verify historical Wrapped Record Blocks.
 *
 * <p>Each entry in the input {@code AddressBookHistory} carries a consensus {@code block_timestamp};
 * this command resolves that timestamp to the block number that became valid at or after that time
 * (via {@link BlockTimeReader#getNearestBlockAfterTime(LocalDateTime)}) and emits a roster history
 * file with the following shape:
 *
 * <pre>{@code
 * {
 *   "entries": [
 *     {
 *       "startBlock": <long>,
 *       "endBlock":   <long>|null,
 *       "blockTimestamp": { "seconds": <long>, "nanos": <int> },
 *       "addressBook": "<base64-encoded NodeAddressBook proto>"
 *     },
 *     ...
 *   ]
 * }
 * }</pre>
 *
 * <p>{@code endBlock} is {@code (next.startBlock - 1)}; the most recent era's {@code endBlock}
 * is {@code null} to mark it as open-ended. The full {@code NodeAddressBook} proto is preserved
 * per era (Base64) so the BN can extract the fields it needs at load time.
 *
 * <p>Intended consumer: the BN's historical RSA-roster bootstrap (issue #2958 / T4).
 */
@Command(
        name = "convert-address-book-history",
        description =
                "Convert an AddressBookHistory JSON into a block-range-keyed BN roster history file for historical WRB verification",
        mixinStandardHelpOptions = true)
public class ConvertAddressBookHistoryCommand implements Callable<Integer> {

    @Option(
            names = {"-i", "--input"},
            description =
                    "Path to the AddressBookHistory JSON produced by the CLI (e.g. via `mirror generateAddressBook`)",
            required = true)
    private Path inputFile;

    /**
     * Default output path mirrors today's single-book layout (see
     * {@code ApplicationStateConfig.rsaBootstrapFilePath}) so that, by default, the file lands
     * where the BN's historical-roster bootstrap (T4) is expected to read it from.
     */
    static final Path DEFAULT_OUTPUT_PATH =
            Path.of("/opt/hiero/block-node/application-state/rsa-bootstrap-roster-history.json");

    @Option(
            names = {"-o", "--output"},
            description = "Path to write the roster history JSON (default: ${DEFAULT-VALUE})")
    private Path outputFile = DEFAULT_OUTPUT_PATH;

    @Option(
            names = {"--block-times-file"},
            description =
                    "Path to block_times.bin used for consensus-time → block-number lookup (default: ${DEFAULT-VALUE})")
    private Path blockTimesFile = MetadataFiles.BLOCK_TIMES_FILE;

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    @Override
    public Integer call() throws Exception {
        if (!Files.isRegularFile(inputFile)) {
            System.err.println("Error: input file does not exist: " + inputFile);
            return 1;
        }
        if (!Files.isRegularFile(blockTimesFile)) {
            System.err.println("Error: block_times.bin not found at: " + blockTimesFile);
            System.err.println("Use --block-times-file to point at a valid block_times.bin.");
            return 1;
        }

        System.out.println(Ansi.AUTO.string("@|yellow Converting address-book history:|@"));
        System.out.println("  Input:           " + inputFile.toAbsolutePath());
        System.out.println("  Output:          " + outputFile.toAbsolutePath());
        System.out.println("  block_times.bin: " + blockTimesFile.toAbsolutePath());

        final AddressBookHistory history = loadAddressBookHistory(inputFile);
        final List<DatedNodeAddressBook> sorted = sortByTimestampAscending(history.addressBooks());
        if (sorted.isEmpty()) {
            System.err.println("Error: input contained zero address-book entries.");
            return 1;
        }
        System.out.println("  Eras:            " + sorted.size());

        final long[] startBlocks;
        try (BlockTimeReader reader = new BlockTimeReader(blockTimesFile)) {
            startBlocks = resolveStartBlocks(sorted, reader);
        }

        return convertAndWrite(sorted, startBlocks);
    }

    private int convertAndWrite(List<DatedNodeAddressBook> sorted, long[] startBlocks) throws IOException {
        final ObjectNode root = JSON_MAPPER.createObjectNode();
        final ArrayNode entries = root.putArray("entries");
        for (int i = 0; i < sorted.size(); i++) {
            final DatedNodeAddressBook era = sorted.get(i);
            final Timestamp ts = era.blockTimestampOrThrow();
            final NodeAddressBook book = era.addressBookOrThrow();

            final ObjectNode entry = entries.addObject();
            entry.put("startBlock", startBlocks[i]);
            if (i + 1 < sorted.size()) {
                entry.put("endBlock", startBlocks[i + 1] - 1);
            } else {
                entry.putNull("endBlock");
            }
            final ObjectNode tsNode = entry.putObject("blockTimestamp");
            tsNode.put("seconds", ts.seconds());
            tsNode.put("nanos", ts.nanos());
            entry.put("addressBook", encodeAddressBook(book));
        }

        Path parent = outputFile.toAbsolutePath().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        // Atomic write: serialize to a sibling .tmp then rename, so a crash mid-write can't
        // leave the destination half-written for the BN to read.
        final Path tmp = outputFile.resolveSibling(outputFile.getFileName() + ".tmp");
        try {
            JSON_MAPPER.writeValue(tmp.toFile(), root);
            Files.move(tmp, outputFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            Files.deleteIfExists(tmp);
            throw e;
        }
        System.out.println(Ansi.AUTO.string(
                "@|green Wrote " + sorted.size() + " roster entries to " + outputFile.toAbsolutePath() + "|@"));
        return 0;
    }

    /**
     * Loads the {@link AddressBookHistory} from {@code file}. Distinguishes two error kinds the
     * caller may want to handle differently:
     *
     * <ul>
     *   <li>{@link IOException} &mdash; file IO failures (missing/unreadable file, broken stream)
     *       propagate directly from the underlying input stream.</li>
     *   <li>{@link RuntimeException} (wrapping a PBJ {@link ParseException}) &mdash; the file was
     *       readable but its contents are not a valid {@code AddressBookHistory} JSON.</li>
     * </ul>
     */
    private static AddressBookHistory loadAddressBookHistory(Path file) throws IOException {
        try (ReadableStreamingData in = new ReadableStreamingData(Files.newInputStream(file))) {
            return AddressBookHistory.JSON.parse(in);
        } catch (ParseException e) {
            throw new RuntimeException("Malformed AddressBookHistory JSON in " + file + ": " + e.getMessage(), e);
        }
    }

    private static List<DatedNodeAddressBook> sortByTimestampAscending(List<DatedNodeAddressBook> books) {
        final List<DatedNodeAddressBook> copy = new ArrayList<>(books);
        copy.sort(Comparator.comparingLong(
                        (DatedNodeAddressBook d) -> d.blockTimestampOrThrow().seconds())
                .thenComparingInt(d -> d.blockTimestampOrThrow().nanos()));
        return copy;
    }

    private static long[] resolveStartBlocks(List<DatedNodeAddressBook> sorted, BlockTimeReader reader) {
        final long[] startBlocks = new long[sorted.size()];
        for (int i = 0; i < sorted.size(); i++) {
            final Timestamp ts = sorted.get(i).blockTimestampOrThrow();
            final LocalDateTime ldt = Instant.ofEpochSecond(ts.seconds(), ts.nanos())
                    .atZone(ZoneOffset.UTC)
                    .toLocalDateTime();
            startBlocks[i] = reader.getNearestBlockAfterTime(ldt);
        }
        return startBlocks;
    }

    private static String encodeAddressBook(NodeAddressBook book) {
        return Base64.getEncoder()
                .encodeToString(NodeAddressBook.PROTOBUF.toBytes(book).toByteArray());
    }
}
