// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.suites.publisher.positive;

import static org.hiero.block.api.protoc.BlockResponse.Code.NOT_FOUND;
import static org.hiero.block.suites.utils.BlockAccessUtils.getBlock;
import static org.hiero.block.suites.utils.BlockAccessUtils.getLatestBlock;
import static org.hiero.block.suites.utils.BlockSimulatorUtils.createBlockSimulator;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.stream.Stream;
import org.hiero.block.api.protoc.BlockResponse;
import org.hiero.block.api.protoc.BlockResponse.Code;
import org.hiero.block.simulator.BlockStreamSimulatorApp;
import org.hiero.block.suites.BaseSuite;
import org.hiero.block.suites.BlockNodeContainerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test class for verifying correct behaviour of the application, when more than one publisher is streaming data.
 *
 * <p>Inherits from {@link BaseSuite} to reuse the container setup and teardown logic for the Block
 * Node.
 */
@DisplayName("Positive Multiple Publishers Tests")
public class PositiveMultiplePublishersTests extends BaseSuite {

    private final List<Future<?>> simulators = new ArrayList<>();
    private final List<BlockStreamSimulatorApp> simulatorAppsRef = new ArrayList<>();

    /** Default constructor for the {@link PositiveMultiplePublishersTests} class. */
    public PositiveMultiplePublishersTests() {}

    @AfterEach
    void teardownEnvironment() {
        simulatorAppsRef.forEach(simulator -> {
            try {
                simulator.stop();
                while (simulator.isRunning()) {
                    Thread.sleep(100);
                }
            } catch (InterruptedException e) {
                // Do nothing, this is not mandatory, we try to shut down  cleaner and graceful
            }
        });
        simulatorAppsRef.clear();
        simulators.forEach(simulator -> simulator.cancel(true));
        simulators.clear();
    }

    // @todo(1498)
    @Test
    @Disabled("@todo(1498) - the test is disabled because failure is seen after block node closes connection correctly")
    @DisplayName("Publisher should send TOO_FAR_BEHIND to activate backfill on demand")
    @Timeout(30)
    public void testBackfillOnDemand() throws IOException, InterruptedException {
        launchBlockNodes(List.of(new BlockNodeContainerConfig(8082, 9989, "/resources/block-nodes.json")));
        final Map<String, String> firstSimulatorConfiguration = Map.of(
                "blockStream.streamingMode",
                "MILLIS_PER_BLOCK",
                "blockStream.millisecondsPerBlock",
                "250",
                "generator.endBlockNumber",
                "6",
                "grpc.port",
                "40840");
        final Map<String, String> secondSimulatorConfiguration = Map.of(
                "blockStream.streamingMode",
                "MILLIS_PER_BLOCK",
                "blockStream.millisecondsPerBlock",
                "250",
                "generator.endBlockNumber",
                "3",
                "grpc.port",
                "8082");

        final BlockStreamSimulatorApp firstSimulator = createBlockSimulator(firstSimulatorConfiguration);
        final BlockStreamSimulatorApp secondSimulator = createBlockSimulator(secondSimulatorConfiguration);
        startSimulatorInstance(firstSimulator);
        startSimulatorInstanceWithErrorResponse(secondSimulator);
        Thread.sleep(3000);

        final Map<String, String> thirdSimulatorConfiguration = Map.of(
                "blockStream.streamingMode",
                "MILLIS_PER_BLOCK",
                "blockStream.millisecondsPerBlock",
                "250",
                "generator.startBlockNumber",
                "6",
                "blockStream.endStreamMode",
                "TOO_FAR_BEHIND",
                "blockStream.endStreamEarliestBlockNumber",
                "0",
                "blockStream.endStreamLatestBlockNumber",
                "6",
                "grpc.port",
                "8082");
        final BlockStreamSimulatorApp thirdSimulator = createBlockSimulator(thirdSimulatorConfiguration);
        startSimulatorInstanceWithErrorResponse(thirdSimulator);
        Thread.sleep(3000);

        final BlockResponse latestPublishedBlockAfter = getLatestBlock(blockAccessStubs.get(8082));
        final long latestBlockNodeBlockNumber = latestPublishedBlockAfter
                .getBlock()
                .getItemsList()
                .getFirst()
                .getBlockHeader()
                .getNumber();

        teardownBlockNodes();
        assertEquals(6, latestBlockNodeBlockNumber);
    }

    @Test
    @DisplayName("Autonomous backfill should fill the gaps")
    @Timeout(30)
    public void testAutonomousBackfill() throws IOException, InterruptedException {
        launchBlockNodes(List.of(new BlockNodeContainerConfig(8082, 9989, "/resources/block-nodes.json")));
        final Map<String, String> firstSimulatorConfiguration = Map.of(
                "blockStream.streamingMode",
                "MILLIS_PER_BLOCK",
                "blockStream.millisecondsPerBlock",
                "250",
                "generator.endBlockNumber",
                "6",
                "grpc.port",
                "40840");
        final Map<String, String> secondSimulatorConfiguration = Map.of(
                "blockStream.streamingMode",
                "MILLIS_PER_BLOCK",
                "blockStream.millisecondsPerBlock",
                "250",
                "generator.endBlockNumber",
                "6",
                "grpc.port",
                "8082");

        final BlockStreamSimulatorApp firstSimulator = createBlockSimulator(firstSimulatorConfiguration);
        final BlockStreamSimulatorApp secondSimulator = createBlockSimulator(secondSimulatorConfiguration);
        startSimulatorInstance(firstSimulator);
        startSimulatorInstanceWithErrorResponse(secondSimulator);
        Thread.sleep(3000);
        deleteBlocks(0, 3);
        BlockResponse block0Deleted = getBlock(blockAccessStubs.get(8082), 0);
        BlockResponse block1Deleted = getBlock(blockAccessStubs.get(8082), 1);
        BlockResponse block2Deleted = getBlock(blockAccessStubs.get(8082), 2);
        assertEquals(NOT_FOUND, block0Deleted.getStatus());
        assertEquals(NOT_FOUND, block1Deleted.getStatus());
        assertEquals(NOT_FOUND, block2Deleted.getStatus());

        restartBlockNode(0);
        Thread.sleep(6000);

        long block0 = getBlock(blockAccessStubs.get(8082), 0)
                .getBlock()
                .getItemsList()
                .getFirst()
                .getBlockHeader()
                .getNumber();

        long block1 = getBlock(blockAccessStubs.get(8082), 1)
                .getBlock()
                .getItemsList()
                .getFirst()
                .getBlockHeader()
                .getNumber();

        long block2 = getBlock(blockAccessStubs.get(8082), 2)
                .getBlock()
                .getItemsList()
                .getFirst()
                .getBlockHeader()
                .getNumber();

        teardownBlockNodes();
        assertEquals(0, block0);
        assertEquals(1, block1);
        assertEquals(2, block2);
    }

    @Test
    @DisplayName("Publisher should handle BEHIND and send TOO_FAR_BEHIND")
    @Timeout(30)
    public void publisherShouldSendTooFarBehindAfterBehind() throws IOException, InterruptedException {
        // ===== Prepare and Start first simulator and make sure it's streaming ======================
        final Map<String, String> firstSimulatorConfiguration = Map.of("generator.startBlockNumber", "0");
        final BlockStreamSimulatorApp firstSimulator = createBlockSimulator(firstSimulatorConfiguration);
        final Future<?> firstSimulatorThread = startSimulatorInstance(firstSimulator);
        // ===== Stop simulator and assert ===========================================================]
        firstSimulator.stop();
        final String firstSimulatorLatestStatus = firstSimulator
                .getStreamStatus()
                .lastKnownPublisherClientStatuses()
                .getLast();
        final long firstSimulatorLatestPublishedBlockNumber =
                firstSimulator.getStreamStatus().publishedBlocks() - 1; // we subtract one since we started on 0
        firstSimulatorThread.cancel(true);

        final BlockResponse latestPublishedBlockBefore =
                getBlock(blockAccessStub, firstSimulatorLatestPublishedBlockNumber);
        final BlockResponse nextPublishedBlockBefore =
                getBlock(blockAccessStub, firstSimulatorLatestPublishedBlockNumber + 1);

        assertNotNull(firstSimulatorLatestStatus);
        assertTrue(firstSimulatorLatestStatus.contains(Long.toString(firstSimulatorLatestPublishedBlockNumber)));
        assertEquals(
                firstSimulatorLatestPublishedBlockNumber,
                latestPublishedBlockBefore
                        .getBlock()
                        .getItemsList()
                        .getFirst()
                        .getBlockHeader()
                        .getNumber());
        assertEquals(Code.NOT_AVAILABLE, nextPublishedBlockBefore.getStatus());

        // ===== Prepare and Start second simulator and make sure it's streaming =====================
        final Map<String, String> secondSimulatorConfiguration =
                Map.of("generator.startBlockNumber", Long.toString(15), "blockStream.endStreamMode", "TOO_FAR_BEHIND");
        final BlockStreamSimulatorApp secondSimulator = createBlockSimulator(secondSimulatorConfiguration);
        final Future<?> secondSimulatorThread = startSimulatorInstanceWithErrorResponse(secondSimulator);

        final String secondSimulatorLatestStatus = secondSimulator
                .getStreamStatus()
                .lastKnownPublisherClientStatuses()
                .getFirst();

        final BlockResponse latestPublishedBlockAfter = getLatestBlock(blockAccessStub);
        secondSimulatorThread.cancel(true);
        assertNotNull(secondSimulatorLatestStatus);
        assertNotNull(latestPublishedBlockAfter);

        final long latestBlockNodeBlockNumber = latestPublishedBlockAfter
                .getBlock()
                .getItemsList()
                .getFirst()
                .getBlockHeader()
                .getNumber();

        assertTrue(secondSimulatorLatestStatus.contains("status: BEHIND"));
        assertEquals(firstSimulatorLatestPublishedBlockNumber, latestBlockNodeBlockNumber);
    }

    /**
     * Verifies that block data is taken from the faster publisher, when two publishers are streaming to the block-node.
     * The test asserts that the slower one receives skip block response.
     */
    @Disabled("Temporarily disabled whiles publisher plugin is being rewritten. Currently produces a false positive")
    @Test
    @DisplayName("Should switch to faster publisher when it catches up with current block number")
    @Timeout(30)
    public void shouldSwitchToFasterPublisherWhenCaughtUp() throws IOException, InterruptedException {
        // ===== Prepare environment =================================================================
        final Map<String, String> slowerSimulatorConfiguration = Map.of("blockStream.millisecondsPerBlock", "1000");
        final Map<String, String> fasterSimulatorConfiguration = Map.of("blockStream.millisecondsPerBlock", "100");

        final BlockStreamSimulatorApp slowerSimulator = createBlockSimulator(slowerSimulatorConfiguration);
        final BlockStreamSimulatorApp fasterSimulator = createBlockSimulator(fasterSimulatorConfiguration);
        simulatorAppsRef.add(slowerSimulator);
        simulatorAppsRef.add(fasterSimulator);

        String lastFasterSimulatorStatusBefore = null;
        String lastFasterSimulatorStatusAfter = null;

        boolean fasterSimulatorCaughtUp = false;

        long slowerSimulatorPublishedBlocksAfter = 0;
        long fasterSimulatorPublishedBlocksAfter = 0;

        // ===== Start slower simulator and make sure it's streaming ==================================
        // slow simulator will publish 5 (current default) statuses before we start the faster one
        final Future<?> slowerSimulatorThread = startSimulatorInstance(slowerSimulator);
        simulators.add(slowerSimulatorThread);
        // ===== Start faster simulator and make sure it's streaming ==================================
        final Future<?> fasterSimulatorThread = startSimulatorInThread(fasterSimulator);
        simulators.add(fasterSimulatorThread);
        while (lastFasterSimulatorStatusBefore == null) {
            if (!fasterSimulator
                    .getStreamStatus()
                    .lastKnownPublisherClientStatuses()
                    .isEmpty()) {
                lastFasterSimulatorStatusBefore = fasterSimulator
                        .getStreamStatus()
                        .lastKnownPublisherClientStatuses()
                        .getLast();
            }
        }

        // faster simulator is expected to hit a duplicate block case on its first status response
        assertTrue(lastFasterSimulatorStatusBefore.contains("DUPLICATE_BLOCK"));

        // ===== Assert whether catching up to the slower will result in correct statutes =============

        long slowerSimulatorPublishedBlocksBefore =
                slowerSimulator.getStreamStatus().publishedBlocks();
        long fasterSimulatorPublishedBlocksBefore =
                fasterSimulator.getStreamStatus().publishedBlocks();

        while (!fasterSimulatorCaughtUp) {
            if (fasterSimulator.getStreamStatus().publishedBlocks()
                    > slowerSimulator.getStreamStatus().publishedBlocks()) {
                fasterSimulatorCaughtUp = true;
                lastFasterSimulatorStatusAfter = fasterSimulator
                        .getStreamStatus()
                        .lastKnownPublisherClientStatuses()
                        .getLast();
                slowerSimulatorPublishedBlocksAfter =
                        slowerSimulator.getStreamStatus().publishedBlocks();
                fasterSimulatorPublishedBlocksAfter =
                        fasterSimulator.getStreamStatus().publishedBlocks();
            }
            Thread.sleep(100); // not necessary, just to avoid not needed iterations
        }

        assertTrue(slowerSimulatorPublishedBlocksBefore > fasterSimulatorPublishedBlocksBefore);
        assertTrue(fasterSimulatorPublishedBlocksAfter > slowerSimulatorPublishedBlocksAfter);
        assertFalse(lastFasterSimulatorStatusAfter.contains("DUPLICATE_BLOCK"));
    }

    /**
     * Verifies that the block-node correctly prioritizes publishers that are streaming current blocks
     * over publishers that are streaming future blocks. This test asserts that the block-node maintains
     * a consistent and chronological order of block processing, even when multiple publishers with
     * different block timings are connected simultaneously.
     */
    @Disabled("Temporarily disabled whiles publisher plugin is being rewritten.")
    @Test
    @DisplayName("Should prefer publisher with current blocks over future blocks")
    @Timeout(30)
    public void shouldPreferCurrentBlockPublisher() throws IOException {
        // ===== Prepare environment =================================================================
        final Map<String, String> currentSimulatorConfiguration = Map.of("generator.startBlockNumber", "0");
        final Map<String, String> futureSimulatorConfiguration = Map.of("generator.startBlockNumber", "1000");

        final BlockStreamSimulatorApp currentSimulator = createBlockSimulator(currentSimulatorConfiguration);
        final BlockStreamSimulatorApp futureSimulator = createBlockSimulator(futureSimulatorConfiguration);
        simulatorAppsRef.add(currentSimulator);
        simulatorAppsRef.add(futureSimulator);

        // ===== Start current simulator and make sure it's streaming ==================================
        final Future<?> currentSimulatorThread = startSimulatorInstance(currentSimulator);
        simulators.add(currentSimulatorThread);

        // ===== Start future simulator and make sure it's streaming ===================================
        final Future<?> futureSimulatorThread = startSimulatorInstance(futureSimulator);
        simulators.add(futureSimulatorThread);

        // ===== Assert that we are persisting only the current blocks =================================
        final BlockResponse currentBlockResponse = getLatestBlock(blockAccessStub);
        final BlockResponse futureBlockResponse = getBlock(blockAccessStub, 1000);

        assertNotNull(currentBlockResponse);
        assertNotNull(futureBlockResponse);
        assertEquals(Code.SUCCESS, currentBlockResponse.getStatus());
        assertEquals(Code.NOT_AVAILABLE, futureBlockResponse.getStatus());
        assertTrue(currentBlockResponse.getBlock().getItemsList().getFirst().hasBlockHeader());
    }

    @ParameterizedTest
    @DisplayName("Publisher should handle error responses and resume streaming")
    @MethodSource("provideDataForErrorResponses")
    @Timeout(30)
    public void publisherShouldResumeAfterError(Map<String, String> config, String errorStatus)
            throws IOException, InterruptedException {
        // ===== Prepare and Start first simulator and make sure it's streaming ======================
        final Map<String, String> firstSimulatorConfiguration = Map.of("generator.startBlockNumber", "0");
        final BlockStreamSimulatorApp firstSimulator = createBlockSimulator(firstSimulatorConfiguration);
        final Future<?> firstSimulatorThread = startSimulatorInstance(firstSimulator);
        // ===== Stop simulator and assert ===========================================================]
        firstSimulator.stop();
        final String firstSimulatorLatestStatus = firstSimulator
                .getStreamStatus()
                .lastKnownPublisherClientStatuses()
                .getLast();
        final long firstSimulatorLatestPublishedBlockNumber =
                firstSimulator.getStreamStatus().publishedBlocks() - 1; // we subtract one since we started on 0
        firstSimulatorThread.cancel(true);

        final BlockResponse latestPublishedBlockBefore =
                getBlock(blockAccessStub, firstSimulatorLatestPublishedBlockNumber);
        final BlockResponse nextPublishedBlockBefore =
                getBlock(blockAccessStub, firstSimulatorLatestPublishedBlockNumber + 1);

        assertNotNull(firstSimulatorLatestStatus);
        assertTrue(firstSimulatorLatestStatus.contains(Long.toString(firstSimulatorLatestPublishedBlockNumber)));
        assertEquals(
                firstSimulatorLatestPublishedBlockNumber,
                latestPublishedBlockBefore
                        .getBlock()
                        .getItemsList()
                        .getFirst()
                        .getBlockHeader()
                        .getNumber());
        assertEquals(Code.NOT_AVAILABLE, nextPublishedBlockBefore.getStatus());

        // ===== Prepare and Start second simulator and make sure it's streaming =====================
        final BlockStreamSimulatorApp secondSimulator = createBlockSimulator(config);
        final Future<?> secondSimulatorThread = startSimulatorInstanceWithErrorResponse(secondSimulator);

        final String secondSimulatorLatestStatus = secondSimulator
                .getStreamStatus()
                .lastKnownPublisherClientStatuses()
                .getFirst();

        Thread.sleep(2000);

        final BlockResponse latestPublishedBlockAfter = getLatestBlock(blockAccessStub);
        secondSimulatorThread.cancel(true);
        assertNotNull(secondSimulatorLatestStatus);
        assertNotNull(latestPublishedBlockAfter);

        final long latestBlockNodeBlockNumber = latestPublishedBlockAfter
                .getBlock()
                .getItemsList()
                .getFirst()
                .getBlockHeader()
                .getNumber();

        assertTrue(secondSimulatorLatestStatus.contains(errorStatus));
        assertTrue(latestBlockNodeBlockNumber > 4);
    }

    /**
     * Verifies that block streaming continues from a new publisher after the primary publisher disconnects.
     * The test asserts that the block-node successfully switches to the new publisher and resumes block streaming
     * once the new publisher catches up to the current block number.
     */
    @Disabled("Temporarily disabled whiles publisher plugin is being rewritten. Currently hangs after duplicate block")
    @Test
    @DisplayName("Should resume block streaming from new publisher after primary publisher disconnects")
    @Timeout(30)
    public void shouldResumeFromNewPublisherAfterPrimaryDisconnects() throws IOException, InterruptedException {
        // ===== Prepare and Start first simulator and make sure it's streaming ======================
        final Map<String, String> firstSimulatorConfiguration = Map.of("generator.startBlockNumber", "0");
        final BlockStreamSimulatorApp firstSimulator = createBlockSimulator(firstSimulatorConfiguration);
        final Future<?> firstSimulatorThread = startSimulatorInstance(firstSimulator);
        // ===== Stop simulator and assert ===========================================================]
        firstSimulator.stop();
        final String firstSimulatorLatestStatus = firstSimulator
                .getStreamStatus()
                .lastKnownPublisherClientStatuses()
                .getLast();
        final long firstSimulatorLatestPublishedBlockNumber =
                firstSimulator.getStreamStatus().publishedBlocks() - 1; // we subtract one since we started on 0
        firstSimulatorThread.cancel(true);

        final BlockResponse latestPublishedBlockBefore =
                getBlock(blockAccessStub, firstSimulatorLatestPublishedBlockNumber);
        final BlockResponse nextPublishedBlockBefore =
                getBlock(blockAccessStub, firstSimulatorLatestPublishedBlockNumber + 1);

        assertNotNull(firstSimulatorLatestStatus);
        assertTrue(firstSimulatorLatestStatus.contains(Long.toString(firstSimulatorLatestPublishedBlockNumber)));
        assertEquals(
                firstSimulatorLatestPublishedBlockNumber,
                latestPublishedBlockBefore
                        .getBlock()
                        .getItemsList()
                        .getFirst()
                        .getBlockHeader()
                        .getNumber());
        assertEquals(Code.NOT_AVAILABLE, nextPublishedBlockBefore.getStatus());

        // ===== Prepare and Start second simulator and make sure it's streaming =====================
        final Map<String, String> secondSimulatorConfiguration =
                Map.of("generator.startBlockNumber", Long.toString(firstSimulatorLatestPublishedBlockNumber - 1));
        final BlockStreamSimulatorApp secondSimulator = createBlockSimulator(secondSimulatorConfiguration);
        final Future<?> secondSimulatorThread = startSimulatorInstance(secondSimulator);
        // ===== Assert that we are persisting blocks from the second simulator ======================
        secondSimulator.stop();
        final String secondSimulatorLatestStatus = secondSimulator
                .getStreamStatus()
                .lastKnownPublisherClientStatuses()
                .getLast();
        secondSimulatorThread.cancel(true);

        final BlockResponse latestPublishedBlockAfter = getLatestBlock(blockAccessStub);

        assertNotNull(secondSimulatorLatestStatus);
        assertNotNull(latestPublishedBlockAfter);

        final long latestBlockNodeBlockNumber = latestPublishedBlockAfter
                .getBlock()
                .getItemsList()
                .getFirst()
                .getBlockHeader()
                .getNumber();
        assertTrue(secondSimulatorLatestStatus.contains(Long.toString(latestBlockNodeBlockNumber)));
    }

    /**
     * Starts a simulator in a thread and make sure that it's running and trying to publish blocks
     *
     * @param simulator instance with configuration depending on the test
     * @return a {@link Future} representing the asynchronous execution of the block stream simulator
     */
    private Future<?> startSimulatorInstance(@NonNull final BlockStreamSimulatorApp simulator) {
        Objects.requireNonNull(simulator);
        final int statusesRequired = 5; // we wait for at least 5 statuses, to avoid flakiness

        final Future<?> simulatorThread = startSimulatorInThread(simulator);
        simulators.add(simulatorThread);
        String simulatorStatus = null;
        while (simulator.getStreamStatus().lastKnownPublisherClientStatuses().size() < statusesRequired) {
            if (!simulator.getStreamStatus().lastKnownPublisherClientStatuses().isEmpty()) {
                simulatorStatus = simulator
                        .getStreamStatus()
                        .lastKnownPublisherClientStatuses()
                        .getLast();
            }
        }
        assertNotNull(simulatorStatus);
        assertTrue(simulator.isRunning());
        return simulatorThread;
    }

    private Future<?> startSimulatorInstanceWithErrorResponse(@NonNull final BlockStreamSimulatorApp simulator)
            throws InterruptedException {
        Objects.requireNonNull(simulator);

        final Future<?> simulatorThread = startSimulatorInThread(simulator);
        simulators.add(simulatorThread);
        String simulatorStatus = null;
        Thread.sleep(500);
        if (!simulator.getStreamStatus().lastKnownPublisherClientStatuses().isEmpty()) {
            simulatorStatus = simulator
                    .getStreamStatus()
                    .lastKnownPublisherClientStatuses()
                    .getFirst();
        }
        assertNotNull(simulatorStatus);
        assertTrue(simulator.isRunning());
        return simulatorThread;
    }

    private static Stream<Arguments> provideDataForErrorResponses() {
        return Stream.of(
                Arguments.of(Map.of("generator.startBlockNumber", Long.toString(15)), "status: BEHIND"),
                Arguments.of(Map.of("generator.startBlockNumber", Long.toString(4)), "status: DUPLICATE_BLOCK"),
                Arguments.of(Map.of("generator.startBlockNumber", Long.toString(0)), "status: DUPLICATE_BLOCK"));
    }
}
