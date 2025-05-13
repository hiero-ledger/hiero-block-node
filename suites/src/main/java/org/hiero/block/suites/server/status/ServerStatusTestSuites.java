// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.suites.server.status;

import org.hiero.block.suites.server.status.positive.PositiveServerStatusTests;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

@Suite
@SelectClasses({PositiveServerStatusTests.class})
public class ServerStatusTestSuites {
    /**
     * Default constructor for the {@link ServerStatusTestSuites} class.This constructor is
     * empty as it does not need to perform any initialization.
     */
    public ServerStatusTestSuites() {}
}
