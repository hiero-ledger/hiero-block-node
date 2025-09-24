// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.pipeline;

import com.hedera.pbj.runtime.grpc.Pipeline;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A simple test implementation of the {@link Pipeline} interface. It keeps
 * track of the calls made to its methods and allows for assertions to be
 * made on the calls made to it.
 */
public class TestResponsePipeline<R> implements Pipeline<R> {
    private final AtomicInteger clientEndStreamCalls = new AtomicInteger(0);
    private final AtomicInteger onCompleteCalls = new AtomicInteger(0);
    private final List<R> onNextCalls = new CopyOnWriteArrayList<>();
    private final List<Subscription> onSubscriptionCalls = new CopyOnWriteArrayList<>();
    private final List<Throwable> onErrorCalls = new CopyOnWriteArrayList<>();

    @Override
    public void clientEndStreamReceived() {
        clientEndStreamCalls.incrementAndGet();
    }

    @Override
    public void onNext(final R item) {
        onNextCalls.add(Objects.requireNonNull(item));
    }

    @Override
    public void onSubscribe(final Subscription subscription) {
        onSubscriptionCalls.add(Objects.requireNonNull(subscription));
    }

    @Override
    public void onError(final Throwable throwable) {
        onErrorCalls.add(Objects.requireNonNull(throwable));
    }

    @Override
    public void onComplete() {
        onCompleteCalls.incrementAndGet();
    }

    public AtomicInteger getClientEndStreamCalls() {
        return clientEndStreamCalls;
    }

    public AtomicInteger getOnCompleteCalls() {
        return onCompleteCalls;
    }

    public List<R> getOnNextCalls() {
        return onNextCalls;
    }

    public List<Subscription> getOnSubscriptionCalls() {
        return onSubscriptionCalls;
    }

    public List<Throwable> getOnErrorCalls() {
        return onErrorCalls;
    }

    /**
     * Fixture method, clears all recorded calls.
     */
    public void clear() {
        clientEndStreamCalls.set(0);
        onCompleteCalls.set(0);
        onNextCalls.clear();
        onSubscriptionCalls.clear();
        onErrorCalls.clear();
    }
}
