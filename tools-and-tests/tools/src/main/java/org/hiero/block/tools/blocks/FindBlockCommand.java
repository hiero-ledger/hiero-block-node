// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks;

import static org.hiero.block.tools.utils.Sha384.sha384Digest;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.block.stream.RecordFileItem;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.hapi.streams.SidecarFile;
import com.hedera.hapi.streams.SidecarMetadata;
import com.hedera.hapi.streams.TransactionSidecarRecord;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.hiero.block.tools.blocks.model.BlockZipsUtilities;
import org.hiero.block.tools.blocks.model.BlockZipsUtilities.BlockSource;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Locate a specific block by number inside one or more WRB zip archives (or directories of them)
 * and print an inspection report.
 *
 * <p>Primary use case is investigating sidecar integrity failures reported by
 * {@code blocks validate-sidecars}. For each match the command prints:
 *
 * <ul>
 *   <li>Where the block was found (zip path + entry name)</li>
 *   <li>Block header summary (block number, timestamp, HAPI version)</li>
 *   <li>RecordFileItem summary (creation time, amendment count)</li>
 *   <li>Signed sidecar metadata from {@code record_file_contents.sidecars[]}</li>
 *   <li>Embedded raw sidecars from {@code sidecar_file_contents[]}, with each sidecar's
 *       computed SHA-384, whether it matches any signed metadata entry, and the internal
 *       {@code consensus_timestamp} of each contained {@link TransactionSidecarRecord}
 *       (which reveals which block period the sidecar bytes actually belong to when
 *       investigating a mis-attribution)</li>
 * </ul>
 *
 * <p>Optional {@code --extract-sidecars <dir>} writes each embedded sidecar's serialized bytes
 * to disk so they can be diffed against bucket files or fed back through PBJ.
 * Optional {@code --json} dumps the full parsed block as JSON.
 *
 * <p>Example:
 * <pre>{@code
 * java -jar tools-all.jar blocks find-block 62113066 /path/to/mainnet-wrbs
 * }</pre>
 */
@Command(
        name = "find-block",
        description = "Locate a block by number inside WRB zip archives and print its contents with sidecar analysis",
        mixinStandardHelpOptions = true)
public class FindBlockCommand implements Runnable {

    /** Maximum protobuf parse size in bytes (120 MB); matches ConvertToJson. */
    private static final int MAX_BLOCK_SIZE_BYTES = 120 * 1024 * 1024;

    @SuppressWarnings("unused")
    @Parameters(index = "0", description = "Block number to locate")
    private long blockNumber;

    @SuppressWarnings("unused")
    @Parameters(index = "1..*", description = "Block files, directories, or zip archives to search")
    private Path[] paths;

    @Option(
            names = {"--extract-sidecars"},
            description =
                    "If set, write each embedded sidecar's raw serialized bytes to this directory as <block>_<idx>.rcd")
    private Path extractSidecarsDir;

    @Option(
            names = {"--json"},
            description = "Print the full parsed block as JSON to stdout")
    private boolean printJson = false;

    @Option(
            names = {"--stop-on-first"},
            description = "Stop after the first match instead of iterating every hit across the inputs")
    private boolean stopOnFirst = true;

    @Override
    public void run() {
        if (paths == null || paths.length == 0) {
            System.err.println("Error: at least one file, directory, or zip must be given");
            return;
        }

        final AtomicLong corruptZipCount = new AtomicLong(0);
        final List<BlockSource> discovered = BlockZipsUtilities.findBlockSources(paths, corruptZipCount);
        if (discovered.isEmpty()) {
            System.err.println("Error: no block sources discovered in the given inputs");
            return;
        }

        System.out.println(Ansi.AUTO.string("@|yellow Finding block " + blockNumber + "|@"));
        System.out.println("  Sources discovered: " + discovered.size());
        if (corruptZipCount.get() > 0) {
            System.out.println("  Corrupt zips skipped: " + corruptZipCount.get());
        }
        System.out.println("  Expanding zip archives...");
        final List<BlockSource> sources = BlockZipsUtilities.expandWholeZipSources(discovered);
        System.out.println("  Total blocks visible: " + sources.size());
        System.out.println();

        final List<BlockSource> matches =
                sources.stream().filter(s -> s.blockNumber() == blockNumber).toList();

        if (matches.isEmpty()) {
            System.err.println(Ansi.AUTO.string("@|red Block " + blockNumber + " not found in the given inputs.|@"));
            System.exit(1);
        }

        System.out.println(Ansi.AUTO.string("@|green Found " + matches.size() + " match(es).|@"));
        System.out.println();

        int shown = 0;
        for (final BlockSource source : matches) {
            shown++;
            printMatch(source, shown, matches.size());
            if (stopOnFirst) {
                if (matches.size() > 1) {
                    System.out.println(Ansi.AUTO.string("@|yellow (--stop-on-first set; " + (matches.size() - 1)
                            + " additional match(es) not shown; pass --stop-on-first=false to see all)|@"));
                }
                break;
            }
        }
    }

    private void printMatch(final BlockSource source, final int matchIndex, final int totalMatches) {
        System.out.println("================================================================");
        System.out.println("Match " + matchIndex + " of " + totalMatches);
        System.out.println("  File:  " + source.filePath());
        if (source.zipEntryName() != null) {
            System.out.println("  Entry: " + source.zipEntryName());
        }
        System.out.println("  Compression: "
                + (source.compressionType() == null
                        ? "GZIP"
                        : source.compressionType().name()));

        final Block block;
        try {
            final byte[][] data = BlockZipsUtilities.readBlockData(source);
            final boolean[] flags = BlockZipsUtilities.compressionFlags(source);
            block = BlockZipsUtilities.decompressAndParse(data[0], flags[0], flags[1]);
        } catch (final Exception e) {
            System.err.println("Error reading/parsing block: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            return;
        }

        printBlockHeader(block);
        printSidecarAnalysis(block);

        if (extractSidecarsDir != null) {
            extractSidecarsToDir(block);
        }

        if (printJson) {
            System.out.println();
            System.out.println("--- Full block JSON ---");
            System.out.println(Block.JSON.toJSON(block));
        }
    }

    private void printBlockHeader(final Block block) {
        System.out.println();
        System.out.println("--- Block header ---");
        block.items().stream()
                .filter(BlockItem::hasBlockHeader)
                .findFirst()
                .ifPresentOrElse(
                        item -> {
                            final var header = item.blockHeader();
                            System.out.println("  block number:    " + header.number());
                            final Timestamp ts = header.blockTimestamp();
                            if (ts != null) {
                                System.out.println("  block timestamp: " + ts.seconds() + "." + pad9(ts.nanos()));
                            }
                            if (header.hapiProtoVersion() != null) {
                                final var v = header.hapiProtoVersion();
                                System.out.println(
                                        "  HAPI version:    " + v.major() + "." + v.minor() + "." + v.patch());
                            }
                            System.out.println("  hash algorithm:  " + header.hashAlgorithm());
                        },
                        () -> System.out.println("  (no BlockHeader item present)"));
        System.out.println("  total items:     " + block.items().size());
    }

    private void printSidecarAnalysis(final Block block) {
        System.out.println();
        System.out.println("--- Sidecar analysis ---");
        final RecordFileItem recordFileItem = block.items().stream()
                .filter(BlockItem::hasRecordFile)
                .map(BlockItem::recordFile)
                .findFirst()
                .orElse(null);
        if (recordFileItem == null) {
            System.out.println("  (block has no RecordFileItem; nothing to analyse)");
            return;
        }

        System.out.println("  RecordFileItem creation_time: "
                + tsString(recordFileItem.hasCreationTime() ? recordFileItem.creationTime() : null));
        System.out.println(
                "  Amendments in RecordFileItem: " + recordFileItem.amendments().size());

        final List<SidecarMetadata> metas = recordFileItem.hasRecordFileContents()
                ? recordFileItem.recordFileContentsOrThrow().sidecars()
                : List.of();
        System.out.println("  Signed sidecar metadata entries: " + metas.size());
        for (int i = 0; i < metas.size(); i++) {
            final SidecarMetadata m = metas.get(i);
            final String hh = m.hasHash() ? hex(m.hashOrThrow().hash().toByteArray()) : "(no hash)";
            System.out.println("    meta #" + i + " id=" + m.id() + " types=" + m.types() + " hash=" + hh);
        }

        final List<SidecarFile> sidecars = recordFileItem.sidecarFileContents();
        System.out.println("  Embedded sidecar_file_contents: " + sidecars.size());
        if (sidecars.isEmpty()) {
            return;
        }

        final MessageDigest digest = sha384Digest();
        for (int i = 0; i < sidecars.size(); i++) {
            final SidecarFile sf = sidecars.get(i);
            final byte[] serialized = SidecarFile.PROTOBUF.toBytes(sf).toByteArray();
            digest.update(serialized);
            final byte[] hash = digest.digest();

            System.out.println();
            System.out.println("  sidecar #" + i + ":");
            System.out.println("    serialized size: " + serialized.length + " bytes");
            System.out.println("    computed SHA-384: " + hex(hash));

            final boolean matchesMetadata = metas.stream()
                    .filter(SidecarMetadata::hasHash)
                    .anyMatch(m -> Arrays.equals(m.hashOrThrow().hash().toByteArray(), hash));
            System.out.println("    matches any signed metadata hash: "
                    + (matchesMetadata ? "YES" : Ansi.AUTO.string("@|red NO|@")));

            final List<TransactionSidecarRecord> records = sf.sidecarRecords();
            System.out.println("    TransactionSidecarRecord count: " + records.size());
            for (int j = 0; j < records.size(); j++) {
                final TransactionSidecarRecord r = records.get(j);
                final Timestamp cts = r.hasConsensusTimestamp() ? r.consensusTimestamp() : null;
                final String kind;
                if (r.hasStateChanges()) {
                    kind = "STATE_CHANGES";
                } else if (r.hasActions()) {
                    kind = "ACTIONS";
                } else if (r.hasBytecode()) {
                    kind = "BYTECODE";
                } else {
                    kind = "(none set)";
                }
                System.out.println("      record #" + j + ": consensus_timestamp=" + tsString(cts) + " migration="
                        + r.migration() + " kind=" + kind);
            }
        }
    }

    private void extractSidecarsToDir(final Block block) {
        try {
            Files.createDirectories(extractSidecarsDir);
        } catch (final IOException e) {
            System.err.println("Error creating extract dir " + extractSidecarsDir + ": " + e.getMessage());
            return;
        }
        final RecordFileItem recordFileItem = block.items().stream()
                .filter(BlockItem::hasRecordFile)
                .map(BlockItem::recordFile)
                .findFirst()
                .orElse(null);
        if (recordFileItem == null) {
            return;
        }
        final List<SidecarFile> sidecars = recordFileItem.sidecarFileContents();
        System.out.println();
        System.out.println("--- Extracted sidecar files ---");
        for (int i = 0; i < sidecars.size(); i++) {
            final byte[] bytes = SidecarFile.PROTOBUF.toBytes(sidecars.get(i)).toByteArray();
            final Path out = extractSidecarsDir.resolve(blockNumber + "_" + i + ".rcd");
            try {
                Files.write(out, bytes);
                System.out.println("  wrote " + out + " (" + bytes.length + " bytes)");
            } catch (final IOException e) {
                System.err.println("  failed to write " + out + ": " + e.getMessage());
            }
        }
    }

    private static String hex(final byte[] bytes) {
        final StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (final byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    private static String tsString(final Timestamp ts) {
        if (ts == null) {
            return "(none)";
        }
        return ts.seconds() + "." + pad9(ts.nanos());
    }

    private static String pad9(final int nanos) {
        final String s = Integer.toString(nanos);
        return "0".repeat(Math.max(0, 9 - s.length())) + s;
    }

    // Suppress unused warning; List retained for future filter flags.
    @SuppressWarnings("unused")
    private static final List<Object> UNUSED = new ArrayList<>();
}
