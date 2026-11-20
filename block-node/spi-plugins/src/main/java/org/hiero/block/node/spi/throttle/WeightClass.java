// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.throttle;

/// The cost tier a request is classified into by a [ContentAwareWeigher], which selects the
/// rate/concurrency policy applied to it. See `docs/design/apis/api-throttling.md`.
public enum WeightClass {
    /// A cheap request, e.g. one for a live/recent block. Also the fallback policy for any weight
    /// class with no configuration of its own.
    STANDARD,
    /// An expensive request, e.g. one for a historical/archived block.
    HEAVY
}
