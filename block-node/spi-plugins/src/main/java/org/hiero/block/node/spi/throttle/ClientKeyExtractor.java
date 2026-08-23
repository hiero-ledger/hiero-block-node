// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.throttle;

import com.hedera.pbj.runtime.grpc.ServiceInterface.RequestOptions;
import edu.umd.cs.findbugs.annotations.NonNull;

/// Derives the key used to group a caller's requests for per-client rate/concurrency limiting.
///
/// The default implementation ([RemoteAddressKeyExtractor]) keys by network address. This
/// interface exists specifically so that an authenticated-identity-based implementation (an mTLS
/// client certificate, or an API key) can replace it later without any change to
/// [ThrottledServiceInterface], [ThrottlePolicy], or any plugin configuration.
/// [RequestOptions#remoteCertificateChain()] already exists today, unused, as exactly what a
/// future certificate-based extractor would read.
public interface ClientKeyExtractor {
    /// Derives a stable key identifying the caller of this request.
    ///
    /// @param options the request options for the call
    /// @return a non-null key grouping this caller's requests
    @NonNull
    String extractKey(@NonNull RequestOptions options);
}
