// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.throttle;

import com.hedera.pbj.runtime.grpc.ServiceInterface.Method;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;

/// Classifies a request into a [WeightClass] based on its content, so
/// [WeightedThrottledServiceInterface] can apply a different admission policy depending on how
/// expensive a specific request actually is, rather than one static policy for the whole API.
///
/// Evaluated once per call, as soon as the request bytes become available — for both unary and
/// server-streaming calls, that is *not* the same time `open()` is called; see
/// [WeightedThrottledServiceInterface] for why.
public interface ContentAwareWeigher {
    /// @param method the method being called
    /// @param requestBytes the raw, not-yet-parsed request message bytes
    /// @return the weight class this request should be throttled as
    @NonNull
    WeightClass classify(@NonNull Method method, @NonNull Bytes requestBytes);
}
