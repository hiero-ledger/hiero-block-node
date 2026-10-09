// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.throttle;

/// A cost tier a [ContentAwareWeigher] classifies a request into, before its admission-control
/// checks run, so a single API can apply a stricter policy to its more expensive requests (e.g. a
/// `getBlock` call for a historical/archived block) without needing a separate throttled service
/// per cost tier.
public enum WeightClass {
    /// The API's normal, default cost tier.
    STANDARD,

    /// A materially more expensive request than [#STANDARD] for the same API, warranting a
    /// stricter policy — e.g. a request for a historical/archived block instead of a live one.
    HEAVY
}
