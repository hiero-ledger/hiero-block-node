// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.suites.utils;

import edu.umd.cs.findbugs.annotations.NonNull;
import org.hiero.block.api.protoc.BlockNodeServiceGrpc;
import org.hiero.block.api.protoc.ServerStatusRequest;
import org.hiero.block.api.protoc.ServerStatusResponse;

public final class ServerStatusUtils {
    private ServerStatusUtils() {
        // Prevent instantiation
    }

    public static ServerStatusResponse requestServerStatus(
            @NonNull final BlockNodeServiceGrpc.BlockNodeServiceBlockingStub blockNodeServiceStub) {
        return blockNodeServiceStub.serverStatus(
                ServerStatusRequest.newBuilder().build());
    }
}
