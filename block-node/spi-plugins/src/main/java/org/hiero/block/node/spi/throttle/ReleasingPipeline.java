// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.throttle;

import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicReference;

/// Wraps the outgoing `responses` pipeline so a call's concurrency permit is released exactly
/// once, whichever of [#onComplete] or [#onError] fires first — both are reliable, single-fire
/// completion signals for every RPC shape, unlike the pipeline `open()` returns (see
/// [ThrottledServiceInterface] and [WeightedThrottledServiceInterface] for the full reasoning).
///
/// The release action is held in an [AtomicReference] rather than passed directly, because
/// [WeightedThrottledServiceInterface] doesn't know which weight class's permit to release — and
/// therefore which action to run — until admission is decided, which happens after this pipeline
/// is constructed (see its class-level documentation for why). [ThrottledServiceInterface], which
/// always knows the release action up front, simply pre-populates the reference.
final class ReleasingPipeline implements Pipeline<Bytes> {
    private final Pipeline<? super Bytes> delegate;
    private final AtomicReference<Runnable> releasePermit;

    ReleasingPipeline(
            @NonNull final Pipeline<? super Bytes> delegate, @NonNull final AtomicReference<Runnable> releasePermit) {
        this.delegate = delegate;
        this.releasePermit = releasePermit;
    }

    @Override
    public void onSubscribe(final Flow.Subscription subscription) {
        delegate.onSubscribe(subscription);
    }

    @Override
    public void onNext(final Bytes item) {
        delegate.onNext(item);
    }

    @Override
    public void onError(final Throwable throwable) {
        releasePermit.get().run();
        delegate.onError(throwable);
    }

    @Override
    public void onComplete() {
        releasePermit.get().run();
        delegate.onComplete();
    }

    @Override
    public void clientEndStreamReceived() {
        delegate.clientEndStreamReceived();
    }

    @Override
    public void closeConnection() {
        delegate.closeConnection();
    }
}
