// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator;

import static java.lang.System.Logger.Level.INFO;
import static org.hiero.block.common.constants.StringsConstants.APPLICATION_PROPERTIES;

import com.hedera.hapi.block.stream.protoc.BlockItem;
import com.hedera.hapi.block.stream.protoc.Block;
import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import com.swirlds.config.extensions.sources.ClasspathFileConfigSource;
import com.swirlds.config.extensions.sources.SystemEnvironmentConfigSource;
import com.swirlds.config.extensions.sources.SystemPropertiesConfigSource;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.lang.System.Logger;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.hiero.block.api.protoc.BlockAccessServiceGrpc;
import org.hiero.block.api.protoc.BlockAccessServiceGrpc.BlockAccessServiceStub;
import org.hiero.block.api.protoc.BlockRequest;
import org.hiero.block.api.protoc.BlockResponse;
import org.hiero.block.simulator.config.SimulatorMappedConfigSourceInitializer;
import org.hiero.block.simulator.config.data.GrpcConfig;
import org.hiero.block.simulator.exception.BlockSimulatorParsingException;

/** The BlockStreamSimulator class defines the simulator for the block stream. */
public class BlockStreamSimulator {
    private static final Logger LOGGER = System.getLogger(BlockStreamSimulator.class.getName());

    /** This constructor should not be instantiated. */
    private BlockStreamSimulator() {}

    /**
     * The main entry point for the block stream simulator.
     *
     * @param args the arguments to be passed to the block stream simulator
     * @throws IOException if an I/O error occurs
     * @throws InterruptedException if the thread is interrupted
     * @throws BlockSimulatorParsingException if a parse error occurs
     */
    public static void main(final String[] args)
            throws IOException, InterruptedException, BlockSimulatorParsingException {

//        LOGGER.log(INFO, "Starting Block Stream Simulator!");
//
//        final ConfigurationBuilder configurationBuilder = ConfigurationBuilder.create()
//                .withSource(SimulatorMappedConfigSourceInitializer.getMappedConfigSource())
//                .withSource(SystemEnvironmentConfigSource.getInstance())
//                .withSource(SystemPropertiesConfigSource.getInstance())
//                .withSource(new ClasspathFileConfigSource(Path.of(APPLICATION_PROPERTIES)))
//                .autoDiscoverExtensions();
//
//        final Configuration configuration = configurationBuilder.build();
//
//        final BlockStreamSimulatorInjectionComponent DIComponent =
//                DaggerBlockStreamSimulatorInjectionComponent.factory().create(configuration);
//
//        final BlockStreamSimulatorApp blockStreamSimulatorApp = DIComponent.getBlockStreamSimulatorApp();
//        blockStreamSimulatorApp.start();
        getBlockRequest();
    }

    private static final GrpcConfig conf = new GrpcConfig("localhost", 40840);

    private static void getBlockRequest() throws InterruptedException {
        final ManagedChannel channel = ManagedChannelBuilder.forAddress(conf.serverAddress(), conf.port())
                .usePlaintext()
                .build();
        final BlockAccessServiceStub stub = BlockAccessServiceGrpc.newStub(channel);
        final List<BlockItem> list = new CopyOnWriteArrayList<>();
        final CountDownLatch latch = new CountDownLatch(1);
        stub.getBlock(BlockRequest.newBuilder().setBlockNumber(0L).build(), new StreamObserver<>() {
            @Override
            public void onNext(final BlockResponse blockResponse) {
                final Block block = blockResponse.getBlock();
                list.addAll(block.getItemsList());
//                list.add(block);
                latch.countDown();
            }

            @Override
            public void onError(final Throwable throwable) {
                throw new RuntimeException(throwable);
            }

            @Override
            public void onCompleted() {
                System.out.println("Stream completed");
            }
        });
        final int timeout = 1_000;
        if (latch.await(timeout, TimeUnit.MILLISECONDS)) {
            final String block = list.stream().map(BlockItem::toString).collect(Collectors.joining(",\n"));
            System.out.println(block);
        } else {
            System.out.printf("No block received for %d milliseconds%n", timeout);
        }
    }
}
