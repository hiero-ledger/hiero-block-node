// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.pbj;

import static com.hedera.block.server.Constants.FULL_SERVICE_NAME_BLOCK_NODE;
import static com.hedera.block.server.Constants.SERVICE_NAME_BLOCK_NODE;

import com.hedera.pbj.runtime.grpc.ServiceInterface;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Arrays;
import java.util.List;

public interface PbjServerStatusService extends ServiceInterface {
    enum ServerStatusMethod implements Method {
        serverStatus
    }

    @NonNull
    default String serviceName() {
        return SERVICE_NAME_BLOCK_NODE;
    }

    @NonNull
    default String fullName() {
        return FULL_SERVICE_NAME_BLOCK_NODE;
    }

    @NonNull
    default List<Method> methods() {
        return Arrays.asList(PbjServerStatusService.ServerStatusMethod.values());
    }
}
