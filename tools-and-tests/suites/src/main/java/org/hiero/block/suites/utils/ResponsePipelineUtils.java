// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.suites.utils;

import com.hedera.pbj.runtime.grpc.Pipeline;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A simple test implementation of the {@link Pipeline} interface. It keeps
 * track of the calls made to its methods and allows for assertions to be
 * made on the calls made to it.
 * Inspired by TestResponsePipeline in block-node-app testFixtures.
 */
public class ResponsePipelineUtils<R> implements Pipeline<R> {
    private final AtomicInteger clientEndStreamCalls = new AtomicInteger(0);
    private final AtomicInteger onCompleteCalls = new AtomicInteger(0);
    private final List<R> onNextCalls = new CopyOnWriteArrayList<>();
    private final List<Subscription> onSubscriptionCalls = new CopyOnWriteArrayList<>();
    private final List<Throwable> onErrorCalls = new CopyOnWriteArrayList<>();
    private AtomicReference<CountDownLatch> onNextLatch = new AtomicReference<>();
    private AtomicReference<CountDownLatch> onCompleteLatch = new AtomicReference<>();

    @Override
    public void clientEndStreamReceived() {
        clientEndStreamCalls.incrementAndGet();
    }

    @Override
    public void onNext(final R item) {
        onNextCalls.add(Objects.requireNonNull(item));
        if (onNextLatch.get() != null) {
            onNextLatch.get().countDown();
        }
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
        if (onCompleteLatch.get() != null) {
            onCompleteLatch.get().countDown();
        }
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

    public CountDownLatch setAndGetOnNextLatch(int count) {
        onNextLatch.set(new CountDownLatch(count));
        return onNextLatch.get();
    }

    public CountDownLatch setAndGetOnCompleteLatch(int count) {
        onCompleteLatch.set(new CountDownLatch(count));
        return onCompleteLatch.get();
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
