// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.publisher.fixtures;

import com.hedera.pbj.runtime.grpc.Pipeline;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.AtomicInteger;
import org.hiero.block.api.PublishStreamResponse;

/**
 * A simple test implementation of the {@link Pipeline} interface. It keeps
 * track of the calls made to its methods and allows for assertions to be
 * made on the calls made to it.
 */
public class TestResponsePipeline implements Pipeline<PublishStreamResponse> {
    private final AtomicInteger clientEndStreamCalls = new AtomicInteger(0);
    private final AtomicInteger onCompleteCalls = new AtomicInteger(0);
    private final List<PublishStreamResponse> onNextCalls = new ArrayList<>();
    private final List<Subscription> onSubscriptionCalls = new ArrayList<>();
    private final List<Throwable> onErrorCalls = new ArrayList<>();

    @Override
    public void clientEndStreamReceived() {
        clientEndStreamCalls.incrementAndGet();
    }

    @Override
    public void onNext(final PublishStreamResponse item) throws RuntimeException {
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

    public List<PublishStreamResponse> getOnNextCalls() {
        return onNextCalls;
    }

    public List<Subscription> getOnSubscriptionCalls() {
        return onSubscriptionCalls;
    }

    public List<Throwable> getOnErrorCalls() {
        return onErrorCalls;
    }
}
