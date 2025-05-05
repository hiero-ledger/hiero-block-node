// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.threading;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Objects;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import org.hiero.block.common.utils.Preconditions;

/**
 * A simple thread factory that creates threads with a given prefix and suffixes
 * an incrementing number. Optionally, a {@link Thread.UncaughtExceptionHandler}
 * can be supplied to this factory. If supplied, it will be used when creating
 * the threads. This class is not to be shared! Each pool that utilizes this
 * factory should have its own instance of this class.
 *
 * <pre>
 * <b>Thread naming example</b>:
 * Suppose we initialize the factory with the prefix "MyThread".
 * The counter always starts at 0, so the first thread created will be named:
 *     <i>"MyThread-0"</i>
 * The second thread created will be named:
 *     <i>"MyThread-1"</i>
 * and so on and so forth.
 * </pre>
 */
public final class PrefixedIncrementingThreadFactory implements ThreadFactory {
    private final String prefix;
    private final Thread.UncaughtExceptionHandler uncaughtExceptionHandler;
    private final AtomicInteger threadCounter;

    /**
     * Constructor.
     *
     * @param prefix the prefix to use for the thread name, must not be blank
     */
    public PrefixedIncrementingThreadFactory(@NonNull final String prefix) {
        this(prefix, null);
    }

    /**
     * Constructor.
     *
     * @param prefix the prefix to use for the thread name, must not be blank
     * @param uncaughtExceptionHandler the uncaught exception handler to use for
     * the threads, this handler is shared, so it should be thread safe, it is
     * also optional and is nullable
     */
    public PrefixedIncrementingThreadFactory(
            @NonNull final String prefix, @Nullable final Thread.UncaughtExceptionHandler uncaughtExceptionHandler) {
        this.prefix = Preconditions.requireNotBlank(prefix);
        this.uncaughtExceptionHandler = uncaughtExceptionHandler;
        this.threadCounter = new AtomicInteger(0);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Thread newThread(@NonNull final Runnable runnable) {
        final Thread result =
                new Thread(Objects.requireNonNull(runnable), prefix + "-" + threadCounter.getAndIncrement());
        // Optionally set the uncaught exception handler
        if (uncaughtExceptionHandler != null) {
            result.setUncaughtExceptionHandler(uncaughtExceptionHandler);
        }
        return result;
    }
}
