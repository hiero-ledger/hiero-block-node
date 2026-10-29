// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.throttle;

import com.hedera.pbj.runtime.grpc.ServiceInterface.RequestOptions;
import edu.umd.cs.findbugs.annotations.NonNull;

/// Derives the key used to group a caller's requests for per-client rate/concurrency limiting.
///
/// The default [RemoteAddressKeyExtractor] keys by network address. This interface lets an
/// identity-based implementation (mTLS client cert, API key) replace it later with no change to
/// [ThrottledServiceInterface] or [ClientThrottle]; [RequestOptions#remoteCertificateChain()]
/// already exists, unused, for exactly that future extractor.
public interface ClientKeyExtractor {
    /// Derives a stable key identifying the caller of this request.
    ///
    /// @param options the request options for the call
    /// @return a non-null key grouping this caller's requests
    @NonNull
    String extractKey(@NonNull RequestOptions options);
}
