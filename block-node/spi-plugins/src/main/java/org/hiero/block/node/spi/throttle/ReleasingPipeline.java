// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.throttle;

import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.concurrent.Flow;

/// Wraps the outgoing `responses` pipeline so a call's concurrency permit is released exactly
/// once, whichever of [#onComplete] or [#onError] fires first — both are reliable, single-fire
/// completion signals for every RPC shape, unlike the pipeline `open()` returns (see
/// [ThrottledServiceInterface] for the full reasoning).
final class ReleasingPipeline implements Pipeline<Bytes> {
    private final Pipeline<? super Bytes> delegate;
    private final Runnable releasePermit;

    ReleasingPipeline(@NonNull final Pipeline<? super Bytes> delegate, @NonNull final Runnable releasePermit) {
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
        releasePermit.run();
        delegate.onError(throwable);
    }

    @Override
    public void onComplete() {
        releasePermit.run();
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
