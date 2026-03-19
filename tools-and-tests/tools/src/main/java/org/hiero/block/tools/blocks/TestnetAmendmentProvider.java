// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks;

import com.hedera.hapi.block.stream.BlockItem;
import java.util.List;

/**
 * Amendment provider for Hedera testnet.
 *
 * <p>Unlike mainnet (where record streams began after the network was already running and
 * a genesis state snapshot is needed), testnet was freshly reset in February 2024 and its
 * record streams are complete from the very first genesis round. Block 0's record file
 * already contains the genesis minting transactions, so no genesis amendments are needed.
 * Injecting mirror-node-sourced genesis state would double-count the initial HBAR supply
 * (once from the amendments' STATE_CHANGES and once from block 0's transfer lists).
 *
 * <p>Testnet also does not have missing transaction amendments since the record streams
 * are complete from genesis.
 */
public class TestnetAmendmentProvider implements AmendmentProvider {

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
        return false;
    }

    @Override
    public List<BlockItem> getGenesisAmendments(long blockNumber) {
        return List.of();
    }
}
