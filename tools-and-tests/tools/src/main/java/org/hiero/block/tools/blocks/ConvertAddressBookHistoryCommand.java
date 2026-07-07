// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.stream.ReadableStreamingData;
import com.hedera.pbj.runtime.io.stream.WritableStreamingData;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Callable;
import org.hiero.block.api.RangedAddressBookHistory;
import org.hiero.block.api.RangedNodeAddressBook;
import org.hiero.block.internal.AddressBookHistory;
import org.hiero.block.internal.DatedNodeAddressBook;
import org.hiero.block.tools.metadata.MetadataFiles;
import org.hiero.block.tools.mirrornode.BlockTimeReader;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Option;

/**
 * Convert the CLI's address-book history JSON into a {@link RangedAddressBookHistory} JSON that the
 * Block Node can load to verify historical Wrapped Record Blocks.
 *
 * <p>Each entry in the input {@code AddressBookHistory} carries a consensus {@code block_timestamp};
 * this command resolves that timestamp to the block number that became valid at or after that time
 * (via {@link BlockTimeReader#getNearestBlockAfterTime(LocalDateTime)}) and emits the shape defined
 * by the {@code RangedAddressBookHistory} proto (see {@code node_service.proto}):
 *
 * <pre>{@code
 * {
 *   "addressBooks": [
 *     {
 *       "addressBook": { "nodeAddress": [ { "nodeId": ..., "rsaPubKey": "...", ... }, ... ] },
 *       "startBlock":  <long>,
 *       "endBlock":    <long>
 *     },
 *     ...
 *   ]
 * }
 * }</pre>
 *
 * <p>{@code endBlock} for the last era is {@code -1}, the open-ended sentinel used by
 * {@code RangedAddressBookHistory}. For all other eras {@code endBlock} is {@code (next.startBlock - 1)}.
 *
 * <p>Intended consumer: the BN's historical RSA-roster bootstrap (issue #2958 / T4).
 */
@Command(
        name = "convert-address-book-history",
        description =
                "Convert an AddressBookHistory JSON into a block-range-keyed BN roster history file for historical WRB verification",
        mixinStandardHelpOptions = true)
public class ConvertAddressBookHistoryCommand implements Callable<Integer> {

    /** Sentinel value used by {@link RangedAddressBookHistory} for the open-ended (most-recent) era. */
    static final long OPEN_ENDED_END_BLOCK = -1L;

    @Option(
            names = {"-i", "--input"},
            description =
                    "Path to the AddressBookHistory JSON produced by the CLI (e.g. via `mirror generateAddressBook`)",
            required = true)
    private Path inputFile;

    /**
     * Default output path is the same file the BN reads for its RSA bootstrap
     * ({@code ApplicationStateConfig.rsaBootstrapFilePath}). T4's loader accepts
     * either a single-book {@link NodeAddressBook} JSON or a
     * {@link RangedAddressBookHistory} JSON at this path.
     */
    static final Path DEFAULT_OUTPUT_PATH =
            Path.of("/opt/hiero/block-node/application-state/rsa-bootstrap-roster.json");

    @Option(
            names = {"-o", "--output"},
            description = "Path to write the roster history JSON (default: ${DEFAULT-VALUE})")
    private Path outputFile = DEFAULT_OUTPUT_PATH;

    @Option(
            names = {"--block-times-file"},
            description =
                    "Path to block_times.bin used for consensus-time → block-number lookup (default: ${DEFAULT-VALUE})")
    private Path blockTimesFile = MetadataFiles.BLOCK_TIMES_FILE;

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

        // Use forCurrentNetwork() so that the genesis instant respects the top-level --network
        // option (mainnet / testnet / previewnet / other). The raw BlockTimeReader(Path) ctor
        // hardcodes the mainnet genesis, which would break resolution on any other network.
        final long[] startBlocks;
        final List<String> resolutionProblems;
        final Instant coverageStart;
        final Instant coverageEnd;
        final long coverageMaxBlock;
        try (BlockTimeReader reader = BlockTimeReader.forCurrentNetwork(blockTimesFile)) {
            coverageMaxBlock = reader.getMaxBlockNumber();
            coverageStart = reader.getBlockInstant(0);
            coverageEnd = reader.getBlockInstant(coverageMaxBlock);
            startBlocks = resolveStartBlocks(sorted, reader);
            resolutionProblems = validateResolutions(sorted, startBlocks, reader);
        }

        if (!resolutionProblems.isEmpty()) {
            System.err.println();
            System.err.println("Error: consensus-time -> block-number resolution failed for "
                    + resolutionProblems.size() + " era(s):");
            for (String p : resolutionProblems) {
                System.err.println("  * " + p);
            }
            System.err.println();
            System.err.println("block_times.bin coverage (" + blockTimesFile.toAbsolutePath() + "):");
            System.err.println("  first indexed block: 0 @ " + coverageStart);
            System.err.println("  last indexed block:  " + coverageMaxBlock + " @ " + coverageEnd);
            System.err.println();
            System.err.println("Check that --network matches the network the block_times.bin was extracted for,");
            System.err.println("and that the file covers your input's timestamp range. Regenerate via");
            System.err.println("`mirror extractBlockTimes` (and `mirror addNewerBlockTimes` to top it up)");
            System.err.println("if it's stale or short.");
            return 1;
        }

        return convertAndWrite(sorted, startBlocks);
    }

    /**
     * Sanity-check every resolved {@code startBlock}. The binary search inside
     * {@link BlockTimeReader#getNearestBlockAfterTime(LocalDateTime)} silently clamps to {@code 0}
     * when the target time falls before every indexed block, and to {@code maxBlock} when it falls
     * after every indexed block -- which combined with PBJ's {@code uint64} default-value elision
     * hides the failure downstream (era's {@code startBlock} disappears, next era's
     * {@code endBlock} collapses to {@code -1}). Detect both here and surface a real error.
     */
    private static List<String> validateResolutions(
            List<DatedNodeAddressBook> sorted, long[] startBlocks, BlockTimeReader reader) {
        final long maxBlock = reader.getMaxBlockNumber();
        final List<String> problems = new ArrayList<>();
        for (int i = 0; i < sorted.size(); i++) {
            final Timestamp ts = sorted.get(i).blockTimestampOrThrow();
            final Instant target = Instant.ofEpochSecond(ts.seconds(), ts.nanos());
            final long block = startBlocks[i];
            final Instant blockInstant = reader.getBlockInstant(block);
            if (block == 0 && blockInstant.isAfter(target)) {
                problems.add("era " + i + " (block_timestamp=" + target
                        + ") is before the earliest indexed block (block 0 @ " + blockInstant + ")");
            } else if (block == maxBlock && blockInstant.isBefore(target)) {
                problems.add("era " + i + " (block_timestamp=" + target + ") is after the last indexed block (block "
                        + maxBlock + " @ " + blockInstant + ")");
            }
        }
        return problems;
    }

    private int convertAndWrite(List<DatedNodeAddressBook> sorted, long[] startBlocks) throws IOException {
        final List<RangedNodeAddressBook> ranged = new ArrayList<>(sorted.size());
        for (int i = 0; i < sorted.size(); i++) {
            final NodeAddressBook book = sorted.get(i).addressBookOrThrow();
            final long start = startBlocks[i];
            final long end = (i + 1 < sorted.size()) ? startBlocks[i + 1] - 1 : OPEN_ENDED_END_BLOCK;
            ranged.add(RangedNodeAddressBook.newBuilder()
                    .addressBook(book)
                    .startBlock(start)
                    .endBlock(end)
                    .build());
        }
        final RangedAddressBookHistory rangedHistory =
                RangedAddressBookHistory.newBuilder().addressBooks(ranged).build();

        Path parent = outputFile.toAbsolutePath().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        // Atomic write: serialize to a sibling .tmp then rename, so a crash mid-write can't
        // leave the destination half-written for the BN to read.
        final Path tmp = outputFile.resolveSibling(outputFile.getFileName() + ".tmp");
        try {
            try (WritableStreamingData out = new WritableStreamingData(Files.newOutputStream(tmp))) {
                RangedAddressBookHistory.JSON.write(rangedHistory, out);
            }
            // PBJ's JSON codec elides proto3 default values (uint64 == 0), which makes a
            // legitimate genesis-era startBlock=0 or open-ended endBlock look identical to a
            // missing field. This bootstrap file gets inspected by operators, so re-emit it
            // with both fields always present: startBlock defaults to 0 (proto3 default),
            // endBlock defaults to -1 (OPEN_ENDED_END_BLOCK sentinel, used only for the last
            // era). PBJ's parse side reconstructs the same values from either shape, so the
            // BN loader is unaffected.
            ensureExplicitStartAndEndBlock(tmp);
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
     * Re-emit the roster JSON at {@code file} so every entry has an explicit
     * {@code startBlock} and {@code endBlock} field, defaulting to {@code 0} and
     * {@code -1} respectively when the PBJ codec elided them. Preserves entry order
     * and the nested {@code addressBook} structure verbatim.
     */
    private static void ensureExplicitStartAndEndBlock(Path file) throws IOException {
        final ObjectMapper mapper = new ObjectMapper();
        final ObjectNode root = (ObjectNode) mapper.readTree(file.toFile());
        final JsonNode addressBooksNode = root.get("addressBooks");
        if (addressBooksNode == null || !addressBooksNode.isArray()) {
            return; // nothing to patch — an empty roster history is legal
        }
        for (final JsonNode entry : addressBooksNode) {
            if (!(entry instanceof ObjectNode entryObj)) {
                continue;
            }
            // Match PBJ's convention: uint64 fields serialize as JSON strings so JS
            // consumers don't hit the 2^53 precision cliff.
            if (!entryObj.has("startBlock")) {
                entryObj.put("startBlock", Long.toString(0L));
            }
            if (!entryObj.has("endBlock")) {
                entryObj.put("endBlock", Long.toString(OPEN_ENDED_END_BLOCK));
            }
        }
        mapper.writerWithDefaultPrettyPrinter().writeValue(file.toFile(), root);
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
}
