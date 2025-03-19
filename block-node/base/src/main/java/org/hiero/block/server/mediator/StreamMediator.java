// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.mediator;

/**
 * The StreamMediator marker interface defines the combination of Publisher and SubscriptionHandler
 * contracts. It defines multiple views of the underlying implementation, allowing producers to
 * publish data while the service and downstream subscribers can manage which consumers are
 * subscribed to the stream of events.
 *
 * @param <U> the type of the data to publish
 * @param <V> the type of the events the SubscriptionHandler processes
 */
public interface StreamMediator<U, V> extends Publisher<U>, SubscriptionHandler<V> {}
