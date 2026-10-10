// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.hiero.block.node.app.SocketBufferCheck.BufferSizes;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Tests for [SocketBufferCheck].
class SocketBufferCheckTest {
    private static final int REQUESTED_BYTES = 8_388_608;
    private static final int LINUX_DEFAULT_MAX_BYTES = 212_992;
    private static final int LINUX_CALIBRATION_REPORTED_BYTES = 131_072;
    private static final int OTHER_CALIBRATION_REPORTED_BYTES = 65_536;

    @Test
    @DisplayName("no shortfall when the kernel grants at least the requested sizes")
    void findShortfalls_grantedAtLeastRequested_returnsNone() {
        final BufferSizes requested = new BufferSizes(REQUESTED_BYTES, REQUESTED_BYTES);
        final BufferSizes granted = new BufferSizes(REQUESTED_BYTES, REQUESTED_BYTES);

        assertTrue(SocketBufferCheck.findShortfalls(requested, granted).isEmpty());
    }

    @Test
    @DisplayName("a capped receive buffer is reported with net.core.rmem_max")
    void findShortfalls_cappedReceiveBuffer_namesRmemMax() {
        final BufferSizes requested = new BufferSizes(REQUESTED_BYTES, REQUESTED_BYTES);
        final BufferSizes granted = new BufferSizes(LINUX_DEFAULT_MAX_BYTES, REQUESTED_BYTES);

        final List<String> shortfalls = SocketBufferCheck.findShortfalls(requested, granted);

        assertEquals(1, shortfalls.size());
        assertTrue(shortfalls.getFirst().contains("net.core.rmem_max"), shortfalls.getFirst());
    }

    @Test
    @DisplayName("a capped send buffer is reported with net.core.wmem_max")
    void findShortfalls_cappedSendBuffer_namesWmemMax() {
        final BufferSizes requested = new BufferSizes(REQUESTED_BYTES, REQUESTED_BYTES);
        final BufferSizes granted = new BufferSizes(REQUESTED_BYTES, LINUX_DEFAULT_MAX_BYTES);

        final List<String> shortfalls = SocketBufferCheck.findShortfalls(requested, granted);

        assertEquals(1, shortfalls.size());
        assertTrue(shortfalls.getFirst().contains("net.core.wmem_max"), shortfalls.getFirst());
    }

    @Test
    @DisplayName("both capped buffers are reported")
    void findShortfalls_bothCapped_reportsBoth() {
        final BufferSizes requested = new BufferSizes(REQUESTED_BYTES, REQUESTED_BYTES);
        final BufferSizes granted = new BufferSizes(LINUX_DEFAULT_MAX_BYTES, LINUX_DEFAULT_MAX_BYTES);

        assertEquals(2, SocketBufferCheck.findShortfalls(requested, granted).size());
    }

    @Test
    @DisplayName("Linux reported sizes are halved, so a cap at half the request is still detected")
    void toGranted_linuxReportsDoubleTheCappedSize_detectsHalfCap() {
        final BufferSizes requested = new BufferSizes(REQUESTED_BYTES, REQUESTED_BYTES);
        final BufferSizes reported = new BufferSizes(REQUESTED_BYTES, 2 * REQUESTED_BYTES);

        final BufferSizes granted = SocketBufferCheck.toGranted(reported, LINUX_CALIBRATION_REPORTED_BYTES);

        assertEquals(1, SocketBufferCheck.findShortfalls(requested, granted).size());
    }

    @Test
    @DisplayName("sizes are kept as reported on platforms that report the granted size")
    void toGranted_platformReportsGrantedSize_keepsReportedSizes() {
        final BufferSizes reported = new BufferSizes(REQUESTED_BYTES, LINUX_DEFAULT_MAX_BYTES);

        assertEquals(reported, SocketBufferCheck.toGranted(reported, OTHER_CALIBRATION_REPORTED_BYTES));
    }
}
