// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.suites.metrics;

import static org.hiero.block.suites.utils.BlockSimulatorUtils.createBlockSimulator;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.dockerjava.api.command.CreateContainerCmd;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Future;
import org.hiero.block.simulator.BlockStreamSimulatorApp;
import org.hiero.block.suites.BaseSuite;
import org.hiero.block.suites.BlockNodeContainerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;

///
/// Regression guard for issue #3026: the block node must run within the nano profile's resource
/// limits (`charts/block-node-server/values-overrides/nano.yaml`): 1 CPU and 192 MiB.
///
/// The nano profile targets memory-constrained environments (minikube, laptops). Its key overrides:
/// - `SERVER_SOCKET_RECEIVE_BUFFER_SIZE_BYTES=131072` (128 KB, vs kernel autotune in the production default)
/// - `JAVA_OPTS` with `-Xmx24m -XX:MaxDirectMemorySize=8m -XX:MaxMetaspaceSize=32m`
/// - Reduced messaging queue sizes
/// - `MALLOC_ARENA_MAX=2`, which caps the native memory arenas of the TSS library's verification threads
///
/// The container gets the same CPU and memory limits as the nano pod. The JVM and the TSS library size
/// their thread pools by the CPUs they see, so without the CPU limit the result depends on the size of
/// the machine running the test. If the node needs more memory than the limit, it is OOM-killed and the
/// test fails. That covers removing or raising the socket buffer override (an 8 MB buffer pins ~640 MB),
/// raising the JVM limits, and dropping `MALLOC_ARENA_MAX` (60 to 130 MB with TSS block proofs).
/// The RSS must also stay under a soft ceiling below the limit, to catch growth before it ends in an OOM kill.
///
/// The image's entrypoint (`app || sleep 30d`) keeps the container running after the node dies, so the
/// test relies on the container's OOM-killed flag and checks it again after reading the RSS.
///
/// The streamed blocks carry TSS block proofs, and the verification plugin loads the TSS library's
/// native code and embedded verification key at start. Measured at 1 CPU: 111 MB with
/// hiero-cryptography 3.13.0, and a ~129 MB peak with 3.22.0, which is OOM-killed at 128 MB (#3879).
///
@DisplayName("Memory Regression Tests")
public class MemoryRegressionTests extends BaseSuite {

    /// gRPC port for the nano-profile container. Must not conflict with the default (40840).
    private static final int NANO_PORT = 40841;

    /// Metrics port for the nano-profile container.
    private static final int NANO_METRICS_PORT = 16008;

    /// CPU limit of the nano profile (`limits.cpu: "1"`), in units of 10^-9 CPUs.
    private static final long NANO_CPU_LIMIT_NANO_CPUS = 1_000_000_000L;

    /// Memory limit of the nano profile (`limits.memory: "192Mi"`), in bytes.
    private static final long NANO_MEMORY_LIMIT_BYTES = 192L * 1024 * 1024;

    /// Soft RSS ceiling (160 MiB), in KB: ~20% above the measured 126 to 133 MiB, below the memory limit.
    private static final long NANO_RSS_CEILING_KB = 160L * 1024;

    /// Environment variable overrides mirroring `values-overrides/nano.yaml`.
    private static final Map<String, String> NANO_ENV = Map.of(
            "JAVA_OPTS",
            "-Xmx24m -XX:MaxDirectMemorySize=8m -XX:MaxMetaspaceSize=32m"
                    + " -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp/dump.hprof",
            "SERVER_SOCKET_RECEIVE_BUFFER_SIZE_BYTES",
            "131072",
            "SERVER_SOCKET_SEND_BUFFER_SIZE_BYTES",
            "131072",
            "MESSAGING_BLOCK_ITEM_QUEUE_SIZE",
            "128",
            "MESSAGING_BLOCK_NOTIFICATION_QUEUE_SIZE",
            "16",
            "MALLOC_ARENA_MAX",
            "2");

    private Future<?> simulatorFuture;

    /// Default constructor.
    public MemoryRegressionTests() {}

    @AfterEach
    void stopSimulatorAndContainers() throws InterruptedException {
        if (simulatorFuture != null) {
            simulatorFuture.cancel(true);
        }
        teardownBlockNodes();
    }

    @Test
    @DisplayName("Nano profile runs within its 1 CPU / 192 MiB limits (issue #3026 regression guard)")
    void nanoProfileRunsWithinItsResourceLimits() throws IOException, InterruptedException {
        BlockNodeContainerConfig nanoConfig = new BlockNodeContainerConfig(NANO_PORT, NANO_METRICS_PORT, "", NANO_ENV);
        GenericContainer<?> nanoContainer = createContainer(nanoConfig);
        nanoContainer.withCreateContainerCmdModifier(MemoryRegressionTests::applyNanoResourceLimits);
        nanoContainer.start();
        blockNodeContainers.add(nanoContainer);

        BlockStreamSimulatorApp simulatorApp = createBlockSimulator(
                Map.of("grpc.port", String.valueOf(NANO_PORT), "generator.endBlockNumber", "9999"));
        simulatorFuture = startSimulatorInThread(simulatorApp);
        Thread.sleep(30_000);
        // Checked before stopping the simulator: stopping it fails once the node is gone.
        assertNotOomKilled(nanoContainer);
        simulatorApp.stop();
        Thread.sleep(2_000);

        long rssKb = readMaxVmRssKb(nanoContainer);
        System.out.printf("Nano profile RSS: %d KB / %.1f MiB%n", rssKb, rssKb / 1024.0);
        // Checked again: after a late OOM kill, the RSS read above only sees the entrypoint's shell.
        assertNotOomKilled(nanoContainer);
        assertTrue(
                rssKb < NANO_RSS_CEILING_KB,
                "Nano profile RSS (%d KB / %.1f MiB) exceeded the %d KB soft ceiling. Check values-overrides/nano.yaml"
                        .formatted(rssKb, rssKb / 1024.0, NANO_RSS_CEILING_KB));
    }

    private static void assertNotOomKilled(GenericContainer<?> container) {
        assertFalse(
                wasOomKilled(container),
                "The nano profile node was OOM-killed at its %d byte memory limit. Check values-overrides/nano.yaml:"
                                .formatted(NANO_MEMORY_LIMIT_BYTES)
                        + " SERVER_SOCKET_RECEIVE_BUFFER_SIZE_BYTES must be 131072 (128 KB), JAVA_OPTS must"
                        + " constrain heap and direct memory, and MALLOC_ARENA_MAX must be 2.");
    }

    /// Gives the container the CPU and memory limits of the nano pod, with no swap.
    private static void applyNanoResourceLimits(CreateContainerCmd command) {
        command.getHostConfig()
                .withNanoCPUs(NANO_CPU_LIMIT_NANO_CPUS)
                .withMemory(NANO_MEMORY_LIMIT_BYTES)
                .withMemorySwap(NANO_MEMORY_LIMIT_BYTES);
    }

    /// Whether the container was killed for exceeding its memory limit.
    private static boolean wasOomKilled(GenericContainer<?> container) {
        return Boolean.TRUE.equals(container
                .getDockerClient()
                .inspectContainerCmd(container.getContainerId())
                .exec()
                .getState()
                .getOOMKilled());
    }

    /// Reads the maximum VmRSS (KB) across all processes in the container by scanning `/proc`.
    /// The JVM process is the largest consumer and will dominate the max.
    private static long readMaxVmRssKb(GenericContainer<?> container) throws IOException, InterruptedException {
        Container.ExecResult result = execInContainerWithRetry(
                container, "sh", "-c", "awk '/^VmRSS/{if($2>m)m=$2}END{print (m+0)}' /proc/[0-9]*/status 2>/dev/null");
        String out = result.getStdout().strip();
        if (out.isEmpty() || "0".equals(out)) {
            throw new IOException("Could not read VmRSS from container (exit=%d stderr=%s)."
                    .formatted(result.getExitCode(), result.getStderr()));
        }
        return Long.parseLong(out);
    }
}
