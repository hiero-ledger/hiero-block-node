// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.base.client;

import com.hedera.pbj.runtime.grpc.GrpcException;
import com.hedera.pbj.runtime.grpc.GrpcStatus;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.function.Supplier;

/// Helper for calling a gRPC method that older peers do not have yet.
///
/// A peer that does not implement a method answers `UNIMPLEMENTED`. During a rollout a client talks to peers on
/// different versions, so it calls the new method first and falls back to the old one only for that status. Any other
/// failure is not hidden: it reaches the caller, which treats the peer as failed.
public final class UnimplementedFallback {

    private UnimplementedFallback() {}

    /// Calls `call`, and calls `fallback` instead if the peer answered `UNIMPLEMENTED`.
    ///
    /// @param call the call to the new method
    /// @param fallback the call to the old method, used only for `UNIMPLEMENTED`
    /// @param <T> the result type
    /// @return the result of `call`, or of `fallback` when the peer does not implement the new method
    /// @throws RuntimeException any failure of `call` other than `UNIMPLEMENTED`, or any failure of `fallback`
    public static <T> T callOrFallback(@NonNull final Supplier<T> call, @NonNull final Supplier<T> fallback) {
        try {
            return call.get();
        } catch (final RuntimeException e) {
            if (isUnimplemented(e)) {
                return fallback.get();
            }
            throw e;
        }
    }

    /// Returns true if the failure, or any of its causes, is a gRPC `UNIMPLEMENTED` status.
    ///
    /// @param failure the failure to inspect
    /// @return true if the peer answered `UNIMPLEMENTED`
    public static boolean isUnimplemented(final Throwable failure) {
        Throwable current = failure;
        // The depth limit guards against a cause chain that loops back on itself.
        for (int depth = 0; current != null && depth < 10; depth++) {
            if (current instanceof GrpcException grpc && grpc.status() == GrpcStatus.UNIMPLEMENTED) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }
}
