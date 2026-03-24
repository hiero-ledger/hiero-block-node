// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks;

import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.node.base.AccountAmount;
import com.hedera.hapi.node.base.AccountID;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.hapi.node.base.Transaction;
import com.hedera.hapi.node.base.TransferList;
import com.hedera.hapi.node.transaction.TransactionRecord;
import com.hedera.hapi.streams.RecordStreamItem;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Amendment provider for Hedera testnet.
 *
 * <p>Unlike mainnet (where record streams began after the network was already running and
 * a genesis state snapshot is needed), testnet was freshly reset in February 2024 and its
 * record streams are complete from the very first genesis round. Block 0's record file
 * already contains the genesis minting transactions, so no genesis amendments are needed.
 *
 * <p>Testnet does have known consensus node bugs where certain transactions have unbalanced
 * transfer lists. Corrective amendments are stored in {@code testnet_missing_transactions.gz}
 * (same length-prefixed protobuf format as mainnet's {@code missing_transactions.gz}) and
 * loaded from the classpath at initialization. Each amendment's consensus timestamp is mapped
 * to its block number via {@link #TIMESTAMP_TO_BLOCK}, avoiding the need for a
 * {@code block_times.bin} file.
 *
 * @see <a href="https://github.com/hiero-ledger/hiero-block-node/issues/2401">#2401</a>
 */
public class TestnetAmendmentProvider implements AmendmentProvider {

    /** Classpath resource containing corrective amendments. */
    static final String RESOURCE_FILE = "testnet_missing_transactions.gz";

    /**
     * Block containing the unbalanced SCHEDULESIGN transaction.
     * See <a href="https://github.com/hiero-ledger/hiero-block-node/issues/2401">#2401</a>.
     */
    static final long BLOCK_7_557_270 = 7_557_270L;

    /**
     * Maps consensus timestamp (nanoseconds since epoch) to the block number it belongs to.
     * This avoids the need for a {@code block_times.bin} file for testnet.
     */
    static final Map<Long, Long> TIMESTAMP_TO_BLOCK =
            Map.of(1_723_039_890L * 1_000_000_000L + 403_229_581L, BLOCK_7_557_270);

    /** Amendments grouped by block number, loaded from classpath resource. */
    private final Map<Long, List<RecordStreamItem>> amendmentsByBlock;

    /**
     * Creates a TestnetAmendmentProvider, loading amendments from the classpath resource.
     */
    public TestnetAmendmentProvider() {
        this.amendmentsByBlock = loadAmendments();
        System.out.println("Initialized testnet amendment provider ("
                + amendmentsByBlock.values().stream().mapToInt(List::size).sum()
                + " amendments across " + amendmentsByBlock.size() + " blocks)");
    }

    @Override
    public String getNetworkName() {
        return "testnet";
    }

    @Override
    public boolean hasGenesisAmendments(long blockNumber) {
        return false;
    }

    @Override
    public List<BlockItem> getGenesisAmendments(long blockNumber) {
        return List.of();
    }

    @Override
    public List<RecordStreamItem> getMissingRecordStreamItems(long blockNumber) {
        return amendmentsByBlock.getOrDefault(blockNumber, List.of());
    }

    /**
     * Loads amendments from the classpath resource and indexes them by block number.
     *
     * @return unmodifiable map of block number to list of amendments
     */
    private Map<Long, List<RecordStreamItem>> loadAmendments() {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(RESOURCE_FILE)) {
            if (is == null) {
                System.out.println("Testnet amendments file not found on classpath: " + RESOURCE_FILE);
                return Map.of();
            }
            List<RecordStreamItem> items = readAllItems(is);
            return indexByBlock(items);
        } catch (IOException e) {
            System.out.println("Warning: Could not load testnet amendments: " + e.getMessage());
            return Map.of();
        }
    }

    /**
     * Reads all RecordStreamItems from a gzipped input stream.
     * Uses the same length-prefixed protobuf format as mainnet's missing_transactions.gz.
     */
    private static List<RecordStreamItem> readAllItems(InputStream rawStream) throws IOException {
        List<RecordStreamItem> items = new ArrayList<>();
        try (GZIPInputStream gzis = new GZIPInputStream(rawStream);
                DataInputStream dis = new DataInputStream(gzis)) {
            while (true) {
                int length;
                try {
                    length = dis.readInt();
                } catch (java.io.EOFException e) {
                    break;
                }
                if (length <= 0) {
                    throw new IOException("Invalid record length: " + length);
                }
                byte[] protoBytes = new byte[length];
                dis.readFully(protoBytes);
                try {
                    items.add(RecordStreamItem.PROTOBUF.parse(Bytes.wrap(protoBytes)));
                } catch (ParseException e) {
                    throw new IOException("Failed to parse RecordStreamItem", e);
                }
            }
        }
        return items;
    }

    /**
     * Groups items by block number using {@link #TIMESTAMP_TO_BLOCK}.
     */
    private static Map<Long, List<RecordStreamItem>> indexByBlock(List<RecordStreamItem> items) {
        Map<Long, List<RecordStreamItem>> index = new HashMap<>();
        for (RecordStreamItem item : items) {
            var ts = item.record().consensusTimestamp();
            long nanos = ts.seconds() * 1_000_000_000L + ts.nanos();
            Long blockNumber = TIMESTAMP_TO_BLOCK.get(nanos);
            if (blockNumber == null) {
                System.out.println("Warning: No block mapping for amendment at timestamp " + ts.seconds() + "."
                        + ts.nanos() + ", skipping");
                continue;
            }
            index.computeIfAbsent(blockNumber, k -> new ArrayList<>()).add(item);
        }
        // Make lists unmodifiable
        Map<Long, List<RecordStreamItem>> result = new HashMap<>();
        for (var entry : index.entrySet()) {
            result.put(entry.getKey(), Collections.unmodifiableList(entry.getValue()));
        }
        return Collections.unmodifiableMap(result);
    }

    // ---- Generation utilities (used to produce the resource file) ----

    /**
     * Builds the corrective amendment for the unbalanced SCHEDULESIGN in block 7,557,270.
     *
     * <p>The original transfer list had erroneous debits on 0.0.98 (-74,388,710) and
     * 0.0.800 (-8,265,412) instead of proper fee credits, netting to -84,373,572 tinybar.
     * This amendment uses the same consensus timestamp so the merge logic in
     * {@code TransferListExtractor} replaces the buggy transfer list with this correct one.
     *
     * <p>The correct fee distribution matches all other SCHEDULESIGN transactions from
     * the same payer (0.0.1282) in this time period:
     * <ul>
     *   <li>0.0.4 (node): +55,801</li>
     *   <li>0.0.98 (network fee): +1,547,505</li>
     *   <li>0.0.800 (service fee): +171,945</li>
     *   <li>0.0.1282 (payer): -1,775,251</li>
     * </ul>
     */
    static RecordStreamItem buildBlock7557270Amendment() {
        Timestamp consensusTimestamp = new Timestamp(1_723_039_890L, 403_229_581);

        TransferList transferList = TransferList.newBuilder()
                .accountAmounts(List.of(
                        accountAmount(4, 55_801L),
                        accountAmount(98, 1_547_505L),
                        accountAmount(800, 171_945L),
                        accountAmount(1282, -1_775_251L)))
                .build();

        TransactionRecord record = TransactionRecord.newBuilder()
                .consensusTimestamp(consensusTimestamp)
                .transferList(transferList)
                .build();

        return new RecordStreamItem(Transaction.newBuilder().build(), record);
    }

    /**
     * Writes the testnet amendments file to the given path.
     * Same format as mainnet's missing_transactions.gz: sequential length-prefixed
     * RecordStreamItem protobufs in a gzipped stream.
     *
     * @param outputPath the file to write
     * @throws IOException if writing fails
     */
    static void generateAmendmentsFile(Path outputPath) throws IOException {
        List<RecordStreamItem> amendments = List.of(buildBlock7557270Amendment());
        try (OutputStream fos = Files.newOutputStream(outputPath);
                GZIPOutputStream gzos = new GZIPOutputStream(fos);
                DataOutputStream dos = new DataOutputStream(gzos)) {
            for (RecordStreamItem item : amendments) {
                byte[] bytes = RecordStreamItem.PROTOBUF.toBytes(item).toByteArray();
                dos.writeInt(bytes.length);
                dos.write(bytes);
            }
        }
        System.out.println("Wrote " + amendments.size() + " amendments to " + outputPath);
    }

    private static AccountAmount accountAmount(long accountNum, long amount) {
        return AccountAmount.newBuilder()
                .accountID(AccountID.newBuilder()
                        .shardNum(0)
                        .realmNum(0)
                        .accountNum(accountNum)
                        .build())
                .amount(amount)
                .build();
    }
}
