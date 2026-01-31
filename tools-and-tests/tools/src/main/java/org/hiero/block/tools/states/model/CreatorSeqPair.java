// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

// com.swirlds.platform.internal.CreatorSeqPair
/** An immutable pair of event creator ID and sequence number, replacing {@code com.swirlds.platform.internal.CreatorSeqPair}. */
public record CreatorSeqPair(Long creatorId, Long seq) {}
