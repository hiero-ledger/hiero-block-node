// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hedera.hapi.node.base.NodeAddress;
import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.stream.ReadableStreamingData;
import com.hedera.pbj.runtime.io.stream.WritableStreamingData;
import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
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

    // JSON keys emitted by PBJ's RangedAddressBookHistory.JSON codec. Kept as constants so any
    // schema rename lands in one place and is auto-suggestable at the call site.
    private static final String KEY_ADDRESS_BOOKS = "addressBooks";
    private static final String KEY_ADDRESS_BOOK = "addressBook";
    private static final String KEY_NODE_ADDRESS = "nodeAddress";
    private static final String KEY_NODE_ID = "nodeId";
    private static final String KEY_START_BLOCK = "startBlock";
    private static final String KEY_END_BLOCK = "endBlock";

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

        // Use the raw ctor (mainnet-anchored) even for --network testnet/previewnet. The
        // block_times.bin file format stores each block's time as nanos-since-mainnet-first-
        // block regardless of network: every writer in the extraction chain hardcodes that
        // anchor via RecordFileDates.instantToBlockTimeLong. Using forCurrentNetwork() here
        // reads the same bytes with a network-specific anchor and produces a multi-year
        // offset on non-mainnet networks (see PR #3166 review). Reader and writer must
        // agree — leave both mainnet-anchored until the write path is network-aware.
        final long[] startBlocks;
        final List<String> resolutionProblems;
        final Instant coverageStart;
        final Instant coverageEnd;
        final long coverageMaxBlock;
        try (BlockTimeReader reader = new BlockTimeReader(blockTimesFile)) {
            coverageMaxBlock = reader.getMaxBlockNumber();
            coverageStart = reader.getBlockInstant(0);
            coverageEnd = reader.getBlockInstant(coverageMaxBlock);
            startBlocks = resolveStartBlocks(sorted, reader);
            resolutionProblems = validateResolutions(sorted, startBlocks, reader);
        }

        if (!resolutionProblems.isEmpty()) {
            final StringBuilder problemLines = new StringBuilder();
            for (String p : resolutionProblems) {
                problemLines.append("  * ").append(p).append(System.lineSeparator());
            }
            System.err.print("""

                    Error: consensus-time -> block-number resolution failed for %d era(s):
                    %s
                    block_times.bin coverage (%s):
                      first indexed block: 0 @ %s
                      last indexed block:  %d @ %s

                    Check that --network matches the network the block_times.bin was extracted for,
                    and that the file covers your input's timestamp range. Regenerate via
                    `mirror extractBlockTimes` (and `mirror addNewerBlockTimes` to top it up)
                    if it's stale or short.
                    """.formatted(
                            resolutionProblems.size(),
                            problemLines,
                            blockTimesFile.toAbsolutePath(),
                            coverageStart,
                            coverageMaxBlock,
                            coverageEnd));
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
        requireSameLength(sorted, startBlocks);
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
        requireSameLength(sorted, startBlocks);
        final List<RangedNodeAddressBook> ranged = new ArrayList<>(sorted.size());
        for (int i = 0; i < sorted.size(); i++) {
            final NodeAddressBook book = slimAddressBook(sorted.get(i).addressBookOrThrow());
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
            ensureExplicitPbjDefaults(tmp);
            atomicMoveWithFallback(tmp, outputFile);
        } catch (IOException e) {
            Files.deleteIfExists(tmp);
            throw e;
        }
        System.out.println(Ansi.AUTO.string(
                "@|green Wrote " + sorted.size() + " roster entries to " + outputFile.toAbsolutePath() + "|@"));
        return 0;
    }

    /**
     * Precondition guard: enforce {@code startBlocks.length == sorted.size()} so an accidental
     * caller-side mismatch fails fast at the entry to any loop indexing {@code startBlocks[i]}
     * with {@code i} bounded by {@code sorted.size()}, instead of surfacing later as an
     * {@link ArrayIndexOutOfBoundsException} at some inner statement.
     */
    private static void requireSameLength(List<DatedNodeAddressBook> sorted, long[] startBlocks) {
        if (startBlocks.length != sorted.size()) {
            throw new IllegalArgumentException(
                    "startBlocks length (" + startBlocks.length + ") must match sorted eras (" + sorted.size() + ")");
        }
    }

    /**
     * Rename {@code src} to {@code dest} atomically when the filesystem supports it, otherwise
     * fall back to a non-atomic replace with a warning. Some filesystems (tmpfs on Docker,
     * certain NFS mounts, jimfs in tests) do not implement {@code ATOMIC_MOVE} and raise
     * {@link AtomicMoveNotSupportedException}; the destination is still overwritten, just not
     * atomically. Mirrors the pattern used in {@code BlockNodeApp.persistNodeAddressBookHistory}.
     */
    private static void atomicMoveWithFallback(Path src, Path dest) throws IOException {
        try {
            Files.move(src, dest, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            System.err.println("Warning: filesystem at " + dest.toAbsolutePath()
                    + " does not support atomic move; falling back to non-atomic replace. "
                    + "A crash mid-rename could leave the destination in a partial state.");
            Files.move(src, dest, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    /**
     * Rebuild {@code book} keeping only the two fields the BN's WRB roster verifier consults:
     * {@code nodeId} (routing key) and {@code RSAPubKey} (signature-verification key). Everything
     * else on {@link NodeAddress} — service endpoints, node cert hash, account id, description,
     * stake, memo — is dropped from the emitted bootstrap file to keep it small and free of
     * PII/network-topology metadata that the BN never reads.
     */
    private static NodeAddressBook slimAddressBook(NodeAddressBook book) {
        final List<NodeAddress> slim = new ArrayList<>(book.nodeAddress().size());
        for (final NodeAddress addr : book.nodeAddress()) {
            slim.add(NodeAddress.newBuilder()
                    .nodeId(addr.nodeId())
                    .rsaPubKey(addr.rsaPubKey())
                    .build());
        }
        return NodeAddressBook.newBuilder().nodeAddress(slim).build();
    }

    /**
     * Re-emit the roster JSON at {@code file} so every entry has explicit fields that PBJ's
     * JSON codec would otherwise elide as proto3 defaults:
     *
     * <ul>
     *   <li>Per-era: {@code startBlock} defaults to {@code 0}, {@code endBlock} defaults to
     *       {@code -1} (the {@link #OPEN_ENDED_END_BLOCK} sentinel used only for the last era).</li>
     *   <li>Per {@code nodeAddress}: {@code nodeId} defaults to {@code 0}. Without this,
     *       every genesis-node-0 entry loses its identifier and the BN's roster verifier
     *       can't route by nodeId.</li>
     * </ul>
     *
     * Preserves entry order and the nested {@code addressBook} structure verbatim. PBJ's
     * parse side reconstructs the same values from either shape, so the BN loader is unaffected.
     */
    private static void ensureExplicitPbjDefaults(Path file) throws IOException {
        final ObjectMapper mapper = new ObjectMapper();
        final ObjectNode root = (ObjectNode) mapper.readTree(file.toFile());
        final JsonNode addressBooksNode = root.get(KEY_ADDRESS_BOOKS);
        if (addressBooksNode == null || !addressBooksNode.isArray()) {
            return; // nothing to patch — an empty roster history is legal
        }
        for (final JsonNode entry : addressBooksNode) {
            if (!(entry instanceof ObjectNode entryObj)) {
                continue;
            }
            // Match PBJ's convention: uint64 fields serialize as JSON strings so JS
            // consumers don't hit the 2^53 precision cliff.
            if (!entryObj.has(KEY_START_BLOCK)) {
                entryObj.put(KEY_START_BLOCK, Long.toString(0L));
            }
            if (!entryObj.has(KEY_END_BLOCK)) {
                entryObj.put(KEY_END_BLOCK, Long.toString(OPEN_ENDED_END_BLOCK));
            }
            // Restore any nodeAddress[].nodeId that PBJ elided as the uint64 default (0).
            final JsonNode addressBookNode = entryObj.get(KEY_ADDRESS_BOOK);
            if (addressBookNode instanceof ObjectNode addressBookObj) {
                final JsonNode nodeAddressNode = addressBookObj.get(KEY_NODE_ADDRESS);
                if (nodeAddressNode != null && nodeAddressNode.isArray()) {
                    for (final JsonNode addr : nodeAddressNode) {
                        if (addr instanceof ObjectNode addrObj && !addrObj.has(KEY_NODE_ID)) {
                            addrObj.put(KEY_NODE_ID, Long.toString(0L));
                        }
                    }
                }
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
