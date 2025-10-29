// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.logging;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.logging.FileHandler;
import java.util.logging.LogManager;

/**
 * A FileHandler that rolls over the log file based on date in addition to size.
 * The log file name pattern can include %1$t to include the current date in
 * yyyy-MM-dd format.
 */
public class RollingFileHandler extends FileHandler {

    public RollingFileHandler() throws IOException {
        super(
                fileName(LogManager.getLogManager().getProperty("java.util.logging.FileHandler.pattern")),
                Integer.parseInt(LogManager.getLogManager().getProperty("java.util.logging.FileHandler.limit")),
                Integer.parseInt(LogManager.getLogManager().getProperty("java.util.logging.FileHandler.count")),
                Boolean.parseBoolean(LogManager.getLogManager().getProperty("java.util.logging.FileHandler.append")));
    }

    private static String fileName(String pattern) {
        DateTimeFormatter formatter =
                DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneOffset.UTC);

        final String fileName = pattern.replace("%d", formatter.format(Instant.now()));

        // check if the directory exists, if not create it
        final java.io.File file = new java.io.File(fileName);
        final java.io.File parentDir = file.getParentFile();
        if (parentDir != null && !parentDir.exists()) {
            parentDir.mkdirs();
        }

        return fileName;
    }
}
