// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app;

import static java.lang.System.Logger.Level.WARNING;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.net.Socket;
import java.net.StandardSocketOptions;
import java.util.ArrayList;
import java.util.List;

/// Warns at startup when the kernel grants a socket less buffer than the server is configured to use.
///
/// The kernel silently truncates `SO_RCVBUF`/`SO_SNDBUF` to `net.core.rmem_max`/`net.core.wmem_max`, and an explicit
/// size also turns off buffer autotuning. On a host with the default limits (212,992 bytes) an 8 MB request ends up
/// as a ~200 KB buffer, which caps every stream at about that much data per round trip. A size of `0` is not set on
/// the socket (the kernel autotunes it), so it is never checked.
final class SocketBufferCheck {
    private static final System.Logger LOGGER = System.getLogger(SocketBufferCheck.class.getName());
    /// Small enough that no kernel caps it, so the reported value shows how the platform scales reported sizes.
    private static final int CALIBRATION_BYTES = 65_536;

    /// Receive and send buffer sizes of one socket, in bytes; `0` means the size is not set.
    ///
    /// @param receiveBytes the `SO_RCVBUF` size
    /// @param sendBytes the `SO_SNDBUF` size
    record BufferSizes(int receiveBytes, int sendBytes) {}

    private SocketBufferCheck() {}

    /// Applies the requested sizes to a throwaway socket and logs a WARNING for each one the kernel capped.
    ///
    /// @param requested the configured buffer sizes
    static void warnIfCapped(@NonNull final BufferSizes requested) {
        if (requested.receiveBytes() == 0 && requested.sendBytes() == 0) {
            return;
        }
        try {
            for (final String shortfall : findShortfalls(requested, readGranted(requested))) {
                LOGGER.log(WARNING, shortfall);
            }
        } catch (final IOException e) {
            LOGGER.log(WARNING, "Could not check the effective socket buffer sizes", e);
        }
    }

    /// Returns one message per buffer whose granted size is below the requested size.
    ///
    /// @param requested the configured buffer sizes
    /// @param granted the sizes the kernel granted, already normalized by [#toGranted]
    /// @return the shortfall messages, empty when every buffer that is set is at least the requested size
    static List<String> findShortfalls(@NonNull final BufferSizes requested, @NonNull final BufferSizes granted) {
        final List<String> shortfalls = new ArrayList<>();
        if (granted.receiveBytes() < requested.receiveBytes()) {
            shortfalls.add(shortfallMessage(
                    "receive",
                    granted.receiveBytes(),
                    requested.receiveBytes(),
                    "server.socketReceiveBufferSizeBytes",
                    "net.core.rmem_max"));
        }
        if (granted.sendBytes() < requested.sendBytes()) {
            shortfalls.add(shortfallMessage(
                    "send",
                    granted.sendBytes(),
                    requested.sendBytes(),
                    "server.socketSendBufferSizeBytes",
                    "net.core.wmem_max"));
        }
        return shortfalls;
    }

    /// Converts the sizes the kernel reports into the sizes it granted.
    ///
    /// Linux stores and reports twice the granted size (the extra half is bookkeeping overhead), while other
    /// platforms report the granted size as is. Comparing raw reported values would hide a Linux cap anywhere
    /// between half and all of the request, so the reported sizes are divided by the factor observed for an
    /// uncapped calibration size.
    ///
    /// @param reported the sizes the kernel reports after applying the request
    /// @param calibrationReportedBytes the size the kernel reports for a `CALIBRATION_BYTES` request
    /// @return the granted sizes
    static BufferSizes toGranted(@NonNull final BufferSizes reported, final int calibrationReportedBytes) {
        final int reportingFactor = Math.max(1, calibrationReportedBytes / CALIBRATION_BYTES);
        return new BufferSizes(reported.receiveBytes() / reportingFactor, reported.sendBytes() / reportingFactor);
    }

    private static BufferSizes readGranted(final BufferSizes requested) throws IOException {
        try (Socket socket = new Socket()) {
            socket.setOption(StandardSocketOptions.SO_RCVBUF, CALIBRATION_BYTES);
            final int calibrationReportedBytes = socket.getOption(StandardSocketOptions.SO_RCVBUF);
            if (requested.receiveBytes() > 0) {
                socket.setOption(StandardSocketOptions.SO_RCVBUF, requested.receiveBytes());
            }
            if (requested.sendBytes() > 0) {
                socket.setOption(StandardSocketOptions.SO_SNDBUF, requested.sendBytes());
            }
            final BufferSizes reported = new BufferSizes(
                    socket.getOption(StandardSocketOptions.SO_RCVBUF),
                    socket.getOption(StandardSocketOptions.SO_SNDBUF));
            return toGranted(reported, calibrationReportedBytes);
        }
    }

    private static String shortfallMessage(
            final String direction,
            final int grantedBytes,
            final int requestedBytes,
            final String property,
            final String sysctl) {
        return String.format(
                "The kernel capped the socket %s buffer at %,d bytes, below the configured %,d (%s). Raise %s to at"
                        + " least %d so streams are not limited to about %,d bytes per round trip.",
                direction, grantedBytes, requestedBytes, property, sysctl, requestedBytes, grantedBytes);
    }
}
