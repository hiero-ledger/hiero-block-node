// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks;

import com.hedera.hapi.block.stream.Block;
import java.io.IOException;
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
            if (bnLastAvailable < 0) {
                // -1 is also returned when the query itself failed (BN unreachable, RPC error), not just
                // when the BN is genuinely empty; queryLastAvailableBlock() can't tell these apart. If
                // it's really the latter, the pushes below will fail to connect and the ack check at the
                // end of this method will catch it - this is just to make that ambiguity diagnosable.
                System.out.println(
                        "[push] " + bnHost + ":" + bnPort + " reports no available blocks (or was unreachable);"
                                + " treating as empty and starting from block 0.");
            }
            if (localHighest <= bnLastAvailable) {
                System.out.println("[push] " + bnHost + ":" + bnPort + " already has block " + bnLastAvailable
                        + " (local highest is " + localHighest + "); nothing to push.");
                return 0;
            }

            final long startBlock = bnLastAvailable + 1;
            System.out.println(
                    "[push] Pushing blocks " + startBlock + ".." + localHighest + " to " + bnHost + ":" + bnPort);
            long pushedCount = 0;
            long highestPushed = bnLastAvailable;
            for (long blockNumber = startBlock; blockNumber <= localHighest; blockNumber++) {
                final Block block;
                try {
                    block = BlockReader.readBlock(inputDir, blockNumber);
                } catch (final IOException e) {
                    // The blockStreamBlockHashes.bin watermark is updated before its block's zip is
                    // durably written (see ToWrappedBlocksCommand), so a concurrent/crashed `blocks wrap`
                    // run can leave localHighest ahead of what's actually on disk. Stop here rather than
                    // letting the gap propagate as an uncaught exception - the next invocation will pick
                    // up wherever wrap left off.
                    System.err.println("[push] Block " + blockNumber + " not yet readable from " + inputDir
                            + "; stopping this run early (" + e.getMessage() + ")");
                    break;
                }
                pushClient.pushBlock(blockNumber, block);
                highestPushed = blockNumber;
                pushedCount++;
            }

            if (pushedCount == 0) {
                pushClient.shutdown();
                System.out.println("[push] Block " + startBlock + " not readable yet; nothing pushed this run.");
                return 0;
            }

            pushClient.shutdown();
            final long lastAcked = pushClient.lastAcked();
            System.out.println("[push] Pushed " + pushedCount + " block(s); BN acked through block " + lastAcked
                    + " (submitted=" + pushClient.submitted() + ", acked=" + pushClient.acked() + ", reconnects="
                    + pushClient.reconnects() + ")");
            if (lastAcked < highestPushed) {
                System.err.println("[push] BN only acked through block " + lastAcked + "; expected " + highestPushed
                        + " - shutdown timed out waiting for outstanding ACKs");
                return 1;
            }
        }
        return 0;
    }
}
