// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.notifier;

import dagger.Binds;
import dagger.Module;
import javax.inject.Singleton;

/** A Dagger module for providing dependencies for Notifier Module. */
@Module
public interface NotifierInjectionModule {

    /**
     * Provides the notifier.
     *
     * @param notifier requires a notifier implementation
     * @return the notifier
     */
    @Binds
    @Singleton
    Notifier bindNotifier(NotifierImpl notifier);
}
