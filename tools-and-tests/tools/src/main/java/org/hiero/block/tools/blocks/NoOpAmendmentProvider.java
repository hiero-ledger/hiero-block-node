// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks;

import com.hedera.hapi.block.stream.BlockItem;
import java.util.List;

/**
 * A no-op amendment provider that never applies any amendments.
 *
 * <p>Use this provider for networks that don't require any amendments,
 * or when you want to disable amendment insertion entirely.
 */
public class NoOpAmendmentProvider implements AmendmentProvider {

    private final String networkName;

    /**
     * Creates a NoOpAmendmentProvider with the specified network name.
     *
     * @param networkName the network name for logging purposes
     */
    public NoOpAmendmentProvider(String networkName) {
        this.networkName = networkName;
        System.out.println("Using no-op amendment provider for network: " + networkName);
    }

    /**
     * Creates a NoOpAmendmentProvider with "none" as the network name.
     */
    public NoOpAmendmentProvider() {
        this("none");
    }

    @Override
    public String getNetworkName() {
        return networkName;
    }

    @Override
    public boolean hasGenesisAmendments(long blockNumber) {
        return false;
    }

    @Override
    public List<BlockItem> getGenesisAmendments(long blockNumber) {
        return List.of();
    }
}
