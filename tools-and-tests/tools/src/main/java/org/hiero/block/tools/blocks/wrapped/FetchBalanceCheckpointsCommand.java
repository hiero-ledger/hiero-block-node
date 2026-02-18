// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.wrapped;

import static org.hiero.block.tools.records.RecordFileDates.instantToBlockTimeLong;
import static org.hiero.block.tools.records.SigFileUtils.verifyRsaSha384;
import static org.hiero.block.tools.utils.Sha384.hashSha384;

import com.github.luben.zstd.ZstdOutputStream;
import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.hapi.streams.SignatureFile;
import com.hedera.pbj.runtime.io.stream.ReadableStreamingData;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Callable;
import org.hiero.block.tools.days.download.DownloadConstants;
import org.hiero.block.tools.days.model.AddressBookRegistry;
import org.hiero.block.tools.mirrornode.BlockTimeReader;
import org.hiero.block.tools.utils.gcp.BalanceFileBucket;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Option;

/**
 * Fetch balance checkpoint files from GCP, verify signatures, and compile them into a single
 * zstd-compressed resource file that can be used for offline balance validation.
 *
 * <p>The output file format contains sequentially written checkpoint records:
 * <ul>
 *   <li>Block number (8 bytes, long)</li>
 *   <li>Account count (4 bytes, int)</li>
 *   <li>For each account: accountNum (8 bytes, long) + balance (8 bytes, long)</li>
 * </ul>
 *
 * <p>This simple binary format avoids protobuf parsing limits and is more compact.
 * The compiled file can be loaded by {@link BalanceCheckpointsLoader} and used by the
 * validation command without requiring GCP access at runtime.
 */
@SuppressWarnings({"FieldCanBeLocal", "CallToPrintStackTrace", "NPathComplexity"})
@Command(
        name = "fetchBalanceCheckpoints",
        description = "Fetch balance checkpoint files from GCP and compile into a resource file",
        mixinStandardHelpOptions = true)
public class FetchBalanceCheckpointsCommand implements Callable<Integer> {

    @Option(
            names = {"-o", "--output"},
            description = "Output zstd-compressed file path (default: balance_checkpoints.zstd)")
    private Path outputFile = Path.of("balance_checkpoints.zstd");

    @Option(
            names = {"--start-day"},
            description = "Start day in format YYYY-MM-DD (default: 2019-09-13)",
            defaultValue = "2019-09-13")
    private String startDay;

    @Option(
            names = {"--end-day"},
            description = "End day in format YYYY-MM-DD (default: 2023-10-23)",
            defaultValue = "2023-10-23")
    private String endDay;

    @Option(
            names = {"--interval-hours"},
            description = "Only include checkpoints at this hour interval (default: 24 = one per day)",
            defaultValue = "24")
    private int intervalHours;

    @Option(
            names = {"--interval-days"},
            description =
                    "Only include one checkpoint every N days (e.g., 7 for weekly, 30 for monthly). Overrides --interval-hours.",
            defaultValue = "0")
    private int intervalDays;

    @Option(
            names = {"--gcp-project"},
            description = "GCP project for requester-pays bucket access (default: from GCP_PROJECT_ID env var)")
    private String gcpProject = DownloadConstants.GCP_PROJECT_ID;

    @Option(
            names = {"--cache-dir"},
            description = "Directory for caching downloaded files",
            defaultValue = "data/gcp-cache")
    private Path cacheDir;

    @Option(
            names = {"--min-node"},
            description = "Minimum node account ID",
            defaultValue = "3")
    private int minNodeAccountId;

    @Option(
            names = {"--max-node"},
            description = "Maximum node account ID",
            defaultValue = "34")
    private int maxNodeAccountId;

    @Option(
            names = {"--address-book"},
            description = "Path to address book history JSON file for signature verification",
            defaultValue = "data/addressBookHistory.json")
    private Path addressBookPath;

    @Option(
            names = {"--block-times"},
            description = "Path to block_times.bin file for timestamp to block mapping",
            defaultValue = "data/block_times.bin")
    private Path blockTimesPath;

    @Option(
            names = {"--skip-signatures"},
            description = "Skip signature verification (not recommended)")
    private boolean skipSignatures;

    @Override
    public Integer call() {
        try {
            System.out.println(
                    Ansi.AUTO.string("@|bold,cyan ════════════════════════════════════════════════════════════|@"));
            System.out.println(Ansi.AUTO.string("@|bold,cyan   FETCH BALANCE CHECKPOINTS|@"));
            System.out.println(
                    Ansi.AUTO.string("@|bold,cyan ════════════════════════════════════════════════════════════|@"));
            System.out.println();

            // Validate required files
            if (!skipSignatures && !Files.exists(addressBookPath)) {
                System.err.println(Ansi.AUTO.string("@|red Error:|@ Address book file not found: " + addressBookPath));
                return 1;
            }
            if (!Files.exists(blockTimesPath)) {
                System.err.println(Ansi.AUTO.string("@|red Error:|@ Block times file not found: " + blockTimesPath));
                return 1;
            }

            // Initialize components
            BalanceFileBucket bucket =
                    new BalanceFileBucket(true, cacheDir, minNodeAccountId, maxNodeAccountId, gcpProject);
            AddressBookRegistry addressBookRegistry = skipSignatures ? null : new AddressBookRegistry(addressBookPath);
            BlockTimeReader blockTimeReader = new BlockTimeReader(blockTimesPath);

            System.out.println(Ansi.AUTO.string("@|yellow Date range:|@ " + startDay + " to " + endDay));
            if (intervalDays > 0) {
                System.out.println(Ansi.AUTO.string("@|yellow Checkpoint interval:|@ every " + intervalDays + " days"));
            } else {
                System.out.println(
                        Ansi.AUTO.string("@|yellow Checkpoint interval:|@ every " + intervalHours + " hours"));
            }
            System.out.println(Ansi.AUTO.string(
                    "@|yellow Signature verification:|@ " + (skipSignatures ? "disabled" : "enabled")));
            System.out.println();

            // Collect all checkpoints to process
            List<CheckpointData> checkpoints = new ArrayList<>();
            LocalDate start = LocalDate.parse(startDay);
            LocalDate end = LocalDate.parse(endDay);

            System.out.println(Ansi.AUTO.string("@|yellow Discovering balance checkpoints...|@"));

            // Day increment based on interval-days option
            int dayIncrement = intervalDays > 0 ? intervalDays : 1;

            for (LocalDate date = start; !date.isAfter(end); date = date.plusDays(dayIncrement)) {
                List<Instant> dayCheckpoints = bucket.listBalanceTimestampsForDay(date.toString());

                if (dayCheckpoints.isEmpty()) {
                    continue;
                }

                if (intervalDays > 0) {
                    // When using interval-days, just take the first available checkpoint for the day
                    Instant timestamp = dayCheckpoints.getFirst();
                    long blockTimeLong = instantToBlockTimeLong(timestamp);
                    long blockNumber = blockTimeReader.getNearestBlockAfterTime(blockTimeLong);
                    checkpoints.add(new CheckpointData(timestamp, blockNumber, null));
                } else {
                    // Filter by interval hours (e.g., one per day at midnight)
                    for (Instant timestamp : dayCheckpoints) {
                        int hour = timestamp.atZone(java.time.ZoneOffset.UTC).getHour();
                        if (hour % intervalHours == 0) {
                            // Map timestamp to block number
                            long blockTimeLong = instantToBlockTimeLong(timestamp);
                            long blockNumber = blockTimeReader.getNearestBlockAfterTime(blockTimeLong);
                            checkpoints.add(new CheckpointData(timestamp, blockNumber, null));
                        }
                    }
                }
            }

            // Sort by block number
            checkpoints.sort(Comparator.comparingLong(c -> c.blockNumber));
            System.out.println(Ansi.AUTO.string("@|yellow Found:|@ " + checkpoints.size() + " checkpoints"));
            System.out.println();

            // Process each checkpoint
            int successCount = 0;
            int errorCount = 0;
            int sigFailCount = 0;

            // Use ZSTD compression level 22 (ultra) for maximum compression
            // Reduces ~650MB uncompressed to ~14MB (vs ~145MB at default level 3)
            ZstdOutputStream zstdOut = new ZstdOutputStream(Files.newOutputStream(outputFile));
            zstdOut.setLevel(22);
            try (DataOutputStream out = new DataOutputStream(zstdOut)) {

                for (int i = 0; i < checkpoints.size(); i++) {
                    CheckpointData checkpoint = checkpoints.get(i);
                    System.out.printf(
                            "[%d/%d] Block %d at %s... ",
                            i + 1, checkpoints.size(), checkpoint.blockNumber, checkpoint.timestamp);

                    try {
                        // Download balance file
                        byte[] pbBytes = bucket.downloadBalanceFile(checkpoint.timestamp);
                        if (pbBytes == null) {
                            System.out.println(Ansi.AUTO.string("@|yellow SKIP|@ (file not found)"));
                            errorCount++;
                            continue;
                        }

                        // Verify signatures if enabled
                        if (!skipSignatures && addressBookRegistry != null) {
                            int validSigs = verifyBalanceFileSignatures(
                                    checkpoint.timestamp, pbBytes, bucket, addressBookRegistry);
                            NodeAddressBook addressBook =
                                    addressBookRegistry.getAddressBookForBlock(checkpoint.timestamp);
                            int totalNodes = addressBook.nodeAddress().size();
                            int requiredSigs = (totalNodes / 3) + 1;

                            if (validSigs < requiredSigs) {
                                System.out.println(Ansi.AUTO.string(
                                        "@|red FAIL|@ (signatures: " + validSigs + "/" + requiredSigs + " required)"));
                                sigFailCount++;
                                continue;
                            }
                        }

                        // Write block number, then parse protobuf and write account data
                        out.writeLong(checkpoint.blockNumber);
                        int accountCount = BalanceProtobufParser.parseAndWrite(pbBytes, out);

                        System.out.println(Ansi.AUTO.string(
                                "@|green OK|@ (" + accountCount + " accounts, " + pbBytes.length + " bytes)"));
                        successCount++;

                    } catch (Exception e) {
                        System.out.println(Ansi.AUTO.string("@|red ERROR|@ " + e.getMessage()));
                        errorCount++;
                    }
                }
            }

            // Print summary
            System.out.println();
            System.out.println(
                    Ansi.AUTO.string("@|bold,cyan ════════════════════════════════════════════════════════════|@"));
            System.out.println(Ansi.AUTO.string("@|bold,cyan   SUMMARY|@"));
            System.out.println(
                    Ansi.AUTO.string("@|bold,cyan ════════════════════════════════════════════════════════════|@"));
            System.out.println(Ansi.AUTO.string("@|yellow Total checkpoints:|@ " + checkpoints.size()));
            System.out.println(Ansi.AUTO.string("@|green Successful:|@ " + successCount));
            if (sigFailCount > 0) {
                System.out.println(Ansi.AUTO.string("@|red Signature failures:|@ " + sigFailCount));
            }
            if (errorCount > 0) {
                System.out.println(Ansi.AUTO.string("@|red Errors:|@ " + errorCount));
            }
            System.out.println(Ansi.AUTO.string("@|yellow Output file:|@ " + outputFile.toAbsolutePath()));
            System.out.println(Ansi.AUTO.string("@|yellow Output size:|@ " + Files.size(outputFile) + " bytes"));

            blockTimeReader.close();
            return 0;

        } catch (Exception e) {
            System.err.println(Ansi.AUTO.string("@|red Fatal error:|@ " + e.getMessage()));
            e.printStackTrace();
            return 1;
        }
    }

    /**
     * Verify balance file signatures.
     *
     * @param checkpoint the checkpoint timestamp
     * @param pbBytes the balance file bytes
     * @param bucket the balance file bucket
     * @param addressBookRegistry the address book registry
     * @return the number of valid signatures
     */
    private int verifyBalanceFileSignatures(
            Instant checkpoint, byte[] pbBytes, BalanceFileBucket bucket, AddressBookRegistry addressBookRegistry) {
        byte[] fileHash = hashSha384(pbBytes);
        NodeAddressBook addressBook = addressBookRegistry.getAddressBookForBlock(checkpoint);

        int validCount = 0;
        for (int nodeAccountId = minNodeAccountId; nodeAccountId <= maxNodeAccountId; nodeAccountId++) {
            byte[] sigBytes = bucket.downloadBalanceSignatureFile(checkpoint, nodeAccountId);
            if (sigBytes == null) {
                continue;
            }

            try {
                if (verifySignatureFile(sigBytes, fileHash, nodeAccountId, addressBook)) {
                    validCount++;
                }
            } catch (Exception e) {
                // Skip invalid signatures silently
            }
        }
        return validCount;
    }

    /**
     * Verify a single signature file.
     */
    private boolean verifySignatureFile(
            byte[] sigBytes, byte[] fileHash, int nodeAccountId, NodeAddressBook addressBook) throws Exception {
        try (DataInputStream sin = new DataInputStream(new ByteArrayInputStream(sigBytes))) {
            int version = sin.read();
            if (version != 6) {
                return false;
            }

            SignatureFile signatureFile = SignatureFile.PROTOBUF.parse(new ReadableStreamingData(sin));
            if (signatureFile.fileSignature() == null) {
                return false;
            }

            byte[] sigFileHash =
                    signatureFile.fileSignature().hashObjectOrThrow().hash().toByteArray();
            if (!java.util.Arrays.equals(sigFileHash, fileHash)) {
                return false;
            }

            byte[] signature = signatureFile.fileSignature().signature().toByteArray();
            String rsaPubKey = AddressBookRegistry.publicKeyForNode(addressBook, 0, 0, nodeAccountId);
            if (rsaPubKey == null || rsaPubKey.isEmpty()) {
                return false;
            }

            return verifyRsaSha384(rsaPubKey, fileHash, signature);
        }
    }

    /** Internal record to hold checkpoint data during processing */
    private record CheckpointData(Instant timestamp, long blockNumber, byte[] data) {}
}
