// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.common.hasher;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.List;

public record BlockMerkleTreeInfo(
        List<List<Bytes>> inputsMerkleTree,
        List<List<Bytes>> outputsMerkleTree,
        Bytes previousBlockHash,
        Bytes stateRootHash,
        Bytes blockHash) {}
