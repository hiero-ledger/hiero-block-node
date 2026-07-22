// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks;

import com.hedera.hapi.block.stream.Block;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import org.hiero.block.tools.blocks.model.BlockReader;
import org.hiero.block.tools.blocks.model.hashing.BlockStreamBlockHashRegistry;
import org.hiero.block.tools.push.LiveBlockPushClient;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Pushes already-wrapped blocks from a local {@code blocks wrap} output directory to a Block Node over
 * the standard publish gRPC stream.
 *
 * <p>Queries the target BN's {@code lastAvailableBlock} once via {@link LiveBlockPushClient} and pushes
 * only the blocks it doesn't have yet, up to the highest block recorded in the local
 * {@code blockStreamBlockHashes.bin} watermark (the same one {@code blocks wrap} maintains). This makes
 * repeated invocations double as both a one-time historical backfill (first run, BN starts empty) and a
 * live continuation (later runs, after {@code blocks wrap} has produced more blocks) — the command
 * itself is one-shot; a caller loop is responsible for re-invoking it.
 */
@Command(
        name = "push",
        description = "Push already-wrapped blocks from a local directory to a Block Node's publish endpoint",
        mixinStandardHelpOptions = true)
public class PushWrappedBlocksCommand implements Callable<Integer> {

    @Option(
            names = {"-i", "--input-dir"},
            description = "Directory of wrapped blocks to push (same directory 'blocks wrap --output-dir' writes to)")
    private Path inputDir = Path.of("wrappedBlocks");

    @Option(
            names = {"--bn-host"},
            description = "Block Node host to push to (default: ${DEFAULT-VALUE})")
    private String bnHost = "localhost";

    @Option(
            names = {"--bn-port"},
            description = "Block Node Publish port to push to (default: ${DEFAULT-VALUE})")
    private int bnPort = 40840;

    @Option(
            names = {"--push-queue-capacity"},
            description = "Bounded backpressure queue size between this command and the push worker "
                    + "(default: ${DEFAULT-VALUE})")
    private int pushQueueCapacity = 32;

    /** Empty default constructor to remove the Javadoc warning. */
    public PushWrappedBlocksCommand() {}

    @Override
    public Integer call() throws Exception {
        if (!Files.isDirectory(inputDir)) {
            System.err.println("[push] Input directory does not exist: " + inputDir);
            return 1;
        }

        final long localHighest;
        try (final BlockStreamBlockHashRegistry registry =
                new BlockStreamBlockHashRegistry(inputDir.resolve("blockStreamBlockHashes.bin"))) {
            localHighest = registry.highestBlockNumberStored();
        }
        if (localHighest < 0) {
            System.out.println("[push] No wrapped blocks found in " + inputDir + "; nothing to push.");
            return 0;
        }

        try (final LiveBlockPushClient pushClient = new LiveBlockPushClient(
                bnHost, bnPort, pushQueueCapacity, LiveBlockPushClient.loadDefaultWebConfig())) {
            pushClient.start();
            final long bnLastAvailable = pushClient.queryLastAvailableBlock();
            if (localHighest <= bnLastAvailable) {
                System.out.println("[push] " + bnHost + ":" + bnPort + " already has block " + bnLastAvailable
                        + " (local highest is " + localHighest + "); nothing to push.");
                return 0;
            }

            final long startBlock = bnLastAvailable + 1;
            System.out.println(
                    "[push] Pushing blocks " + startBlock + ".." + localHighest + " to " + bnHost + ":" + bnPort);
            long pushedCount = 0;
            for (long blockNumber = startBlock; blockNumber <= localHighest; blockNumber++) {
                final Block block = BlockReader.readBlock(inputDir, blockNumber);
                pushClient.pushBlock(blockNumber, block);
                pushedCount++;
            }
            pushClient.shutdown();
            System.out.println("[push] Pushed " + pushedCount + " block(s); BN acked through block "
                    + pushClient.lastAcked() + " (submitted=" + pushClient.submitted() + ", acked="
                    + pushClient.acked() + ", reconnects=" + pushClient.reconnects() + ")");
        }
        return 0;
    }
}
