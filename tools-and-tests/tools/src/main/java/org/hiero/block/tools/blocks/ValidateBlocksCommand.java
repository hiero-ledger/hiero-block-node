// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks;

import java.io.File;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

@Command(name = "validate", description = "Validates a block stream")
public class ValidateBlocksCommand implements Runnable {

    @Parameters(index = "0..*")
    private File[] files;

    /**
     * Runs this operation.
     */
    @Override
    public void run() {}
}
