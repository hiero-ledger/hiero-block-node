// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks;

import com.hedera.hapi.block.stream.BlockItem;
import java.util.List;
import org.hiero.block.tools.states.TestnetBlockZeroState;

/**
 * Amendment provider for Hedera testnet.
 *
 * <p>Provides genesis amendments for block 0 representing the initial network state at
 * testnet genesis (2024-02-01). The genesis state is fetched from the testnet mirror node
 * API, which returns account balances at the genesis timestamp.
 *
 * <p>Unlike mainnet, testnet does not have missing transaction amendments since the testnet
 * was freshly reset and record streams are complete from genesis.
 */
public class TestnetAmendmentProvider implements AmendmentProvider {

    /** Genesis is always block 0 */
    private static final long GENESIS_BLOCK = 0L;

    /**
     * Creates a TestnetAmendmentProvider.
     */
    public TestnetAmendmentProvider() {
        System.out.println("Initialized testnet amendment provider");
    }

    @Override
    public String getNetworkName() {
        return "testnet";
    }

    @Override
    public boolean hasGenesisAmendments(long blockNumber) {
        return blockNumber == GENESIS_BLOCK;
    }

    @Override
    public List<BlockItem> getGenesisAmendments(long blockNumber) {
        if (blockNumber == GENESIS_BLOCK) {
            return TestnetBlockZeroState.loadStartBlockZeroStateChanges();
        }
        return List.of();
    }
}
