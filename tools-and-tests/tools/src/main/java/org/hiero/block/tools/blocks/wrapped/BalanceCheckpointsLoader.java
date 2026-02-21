// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.wrapped;

import com.github.luben.zstd.ZstdInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.zip.GZIPInputStream;

/**
 * Loads balance checkpoints from a compiled resource file created by
 * {@link FetchBalanceCheckpointsCommand}.
 *
 * <p>The file format contains sequentially written length-prefixed protobuf records:
 * <ul>
 *   <li>Block number (8 bytes, long)</li>
 *   <li>Protobuf length (4 bytes, int)</li>
 *   <li>Raw protobuf bytes (AllAccountBalances format)</li>
 * </ul>
 *
 * <p>This format preserves the standard protobuf structure including token balances.
 *
 * <p>This class provides methods to:
 * <ul>
 *   <li>Get all checkpoint block numbers</li>
 *   <li>Get HBAR and token balances for a specific block number</li>
 *   <li>Find the nearest checkpoint at or before a given block</li>
 * </ul>
 *
 * <p>Also supports loading custom balance files in the format
 * {@code accountBalances_{blockNumber}.pb.gz} from a directory.
 */
public class BalanceCheckpointsLoader {

    /** Map of block number to HBAR balance map (account number -> tinybar balance) */
    private final NavigableMap<Long, Map<Long, Long>> checkpoints = new TreeMap<>();

    /** Map of block number to token balance map (account number -> token number -> balance) */
    private final NavigableMap<Long, Map<Long, Map<Long, Long>>> tokenCheckpoints = new TreeMap<>();

    /**
     * Load balance checkpoints from a compiled zstd file.
     *
     * @param checkpointsFile path to the balance_checkpoints.zstd file
     * @throws IOException if the file cannot be read
     */
    public void loadFromCompiledFile(Path checkpointsFile) throws IOException {
        try (DataInputStream in = new DataInputStream(new ZstdInputStream(Files.newInputStream(checkpointsFile)))) {
            loadFromDataInputStream(in);
        }
    }

    /**
     * Load balance checkpoints from resource stream (for loading from classpath).
     *
     * @param inputStream input stream of the balance_checkpoints.zstd file
     * @throws IOException if the stream cannot be read
     */
    public void loadFromStream(InputStream inputStream) throws IOException {
        try (DataInputStream in = new DataInputStream(new ZstdInputStream(inputStream))) {
            loadFromDataInputStream(in);
        }
    }

    /**
     * Load checkpoints from a DataInputStream using length-prefixed protobuf format.
     */
    private void loadFromDataInputStream(DataInputStream in) throws IOException {
        while (in.available() > 0) {
            long blockNumber = in.readLong();
            int pbLength = in.readInt();
            byte[] pbBytes = in.readNBytes(pbLength);

            // Parse HBAR and token balances from protobuf
            Map<Long, Long> hbarBalances = new HashMap<>();
            Map<Long, Map<Long, Long>> tokenBalances = new HashMap<>();
            BalanceProtobufParser.parseWithTokens(pbBytes, hbarBalances, tokenBalances);

            checkpoints.put(blockNumber, hbarBalances);
            if (!tokenBalances.isEmpty()) {
                tokenCheckpoints.put(blockNumber, tokenBalances);
            }
        }
    }

    /**
     * Load custom balance files from a directory.
     * Files must be named {@code accountBalances_{blockNumber}.pb.gz}.
     *
     * @param directory the directory containing custom balance files
     * @throws IOException if files cannot be read
     */
    public void loadFromDirectory(Path directory) throws IOException {
        if (!Files.isDirectory(directory)) {
            return;
        }

        try (var files = Files.list(directory)) {
            files.filter(p -> p.getFileName().toString().startsWith("accountBalances_"))
                    .filter(p -> p.getFileName().toString().endsWith(".pb.gz")
                            || p.getFileName().toString().endsWith(".pb"))
                    .forEach(path -> {
                        try {
                            String filename = path.getFileName().toString();
                            // Extract block number from filename: accountBalances_{blockNumber}.pb.gz
                            String blockStr = filename.replace("accountBalances_", "")
                                    .replace(".pb.gz", "")
                                    .replace(".pb", "");
                            long blockNumber = Long.parseLong(blockStr);

                            byte[] pbBytes;
                            if (filename.endsWith(".gz")) {
                                try (GZIPInputStream gzIn = new GZIPInputStream(Files.newInputStream(path))) {
                                    pbBytes = gzIn.readAllBytes();
                                }
                            } else {
                                pbBytes = Files.readAllBytes(path);
                            }

                            // Parse HBAR and token balances
                            Map<Long, Long> hbarBalances = new HashMap<>();
                            Map<Long, Map<Long, Long>> tokenBalances = new HashMap<>();
                            BalanceProtobufParser.parseWithTokens(pbBytes, hbarBalances, tokenBalances);
                            checkpoints.put(blockNumber, hbarBalances);
                            if (!tokenBalances.isEmpty()) {
                                tokenCheckpoints.put(blockNumber, tokenBalances);
                            }
                        } catch (Exception e) {
                            System.err.println("Warning: Could not load balance file " + path + ": " + e.getMessage());
                        }
                    });
        }
    }

    /**
     * Get the number of loaded checkpoints.
     *
     * @return the checkpoint count
     */
    public int getCheckpointCount() {
        return checkpoints.size();
    }

    /**
     * Check if a checkpoint exists for the given block number.
     *
     * @param blockNumber the block number
     * @return true if a checkpoint exists
     */
    public boolean hasCheckpoint(long blockNumber) {
        return checkpoints.containsKey(blockNumber);
    }

    /**
     * Get the HBAR balances for a specific checkpoint block number.
     *
     * @param blockNumber the block number
     * @return the balance map, or null if no checkpoint exists for that block
     */
    public Map<Long, Long> getBalances(long blockNumber) {
        return checkpoints.get(blockNumber);
    }

    /**
     * Get the token balances for a specific checkpoint block number.
     *
     * @param blockNumber the block number
     * @return the token balance map (accountNum -> tokenNum -> balance), or null if no checkpoint exists
     */
    public Map<Long, Map<Long, Long>> getTokenBalances(long blockNumber) {
        return tokenCheckpoints.get(blockNumber);
    }

    /**
     * Check if token balances are available for the given block number.
     *
     * @param blockNumber the block number
     * @return true if token balances exist for that block
     */
    public boolean hasTokenBalances(long blockNumber) {
        return tokenCheckpoints.containsKey(blockNumber);
    }

    /**
     * Get the nearest checkpoint at or before the given block number.
     *
     * @param blockNumber the block number
     * @return the nearest checkpoint block number, or null if none exists
     */
    public Long getNearestCheckpointAtOrBefore(long blockNumber) {
        Map.Entry<Long, Map<Long, Long>> entry = checkpoints.floorEntry(blockNumber);
        return entry != null ? entry.getKey() : null;
    }

    /**
     * Get the balances from the nearest checkpoint at or before the given block number.
     *
     * @param blockNumber the block number
     * @return the balance map, or null if no checkpoint exists
     */
    public Map<Long, Long> getBalancesAtOrBefore(long blockNumber) {
        Map.Entry<Long, Map<Long, Long>> entry = checkpoints.floorEntry(blockNumber);
        return entry != null ? entry.getValue() : null;
    }

    /**
     * Get all checkpoint block numbers.
     *
     * @return navigable set of checkpoint block numbers
     */
    public NavigableMap<Long, Map<Long, Long>> getAllCheckpoints() {
        return checkpoints;
    }

    /**
     * Get the first (lowest) checkpoint block number.
     *
     * @return the first block number, or null if no checkpoints loaded
     */
    public Long getFirstBlockNumber() {
        return checkpoints.isEmpty() ? null : checkpoints.firstKey();
    }

    /**
     * Get the last (highest) checkpoint block number.
     *
     * @return the last block number, or null if no checkpoints loaded
     */
    public Long getLastBlockNumber() {
        return checkpoints.isEmpty() ? null : checkpoints.lastKey();
    }
}
