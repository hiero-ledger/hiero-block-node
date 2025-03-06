// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.notifier;

/** Use this interface contract to notify the implementation of critical system events. */
public interface Notifiable {

    /**
     * This method is called to notify of an unrecoverable error and the system will be shut down.
     */
    void notifyUnrecoverableError();
}
