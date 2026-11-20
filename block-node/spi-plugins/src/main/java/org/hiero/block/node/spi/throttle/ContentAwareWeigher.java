// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.throttle;

import com.hedera.pbj.runtime.grpc.ServiceInterface.Method;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;

/// Classifies a request into a [WeightClass] based on its content, so
/// [WeightedThrottledServiceInterface] can apply a different admission policy depending on how
/// expensive a specific request is, rather than one static policy for the whole API.
///
/// Evaluated once per call, as soon as the request bytes are available, which is in the pipeline's
/// `onNext`, not in `open()`. Implementations should read only what they need straight off the
/// wire rather than fully parsing a message the delegate will parse again once admitted.
public interface ContentAwareWeigher {
    /// @param method the method being called
    /// @param requestBytes the raw, not-yet-parsed request message bytes
    /// @return the weight class this request should be throttled as
    @NonNull
    WeightClass classify(@NonNull Method method, @NonNull Bytes requestBytes);
}
