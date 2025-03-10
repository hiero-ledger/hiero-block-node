package com.hedera.block.common.hasher;

import com.hedera.pbj.runtime.io.buffer.Bytes;

public record MerkleProofElement(Bytes hash, boolean isLeft) { }
