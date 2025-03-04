// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.util;

import java.io.File;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import org.mockito.stubbing.Answer;

public final class TestUtils {
    private TestUtils() {}

    private static final String NO_PERMS = "---------";
    private static final String NO_READ = "-wx-wx-wx";
    private static final String NO_WRITE = "r-xr-xr-x";

    public static boolean deleteDirectory(File directoryToBeDeleted) {

        if (!directoryToBeDeleted.exists()) {
            return true;
        }

        if (directoryToBeDeleted.isDirectory()) {
            File[] allContents = directoryToBeDeleted.listFiles();
            if (allContents != null) {
                for (File file : allContents) {
                    deleteDirectory(file);
                }
            }
        }

        return directoryToBeDeleted.delete();
    }

    public static FileAttribute<Set<PosixFilePermission>> getNoPerms() {
        return PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString(NO_PERMS));
    }

    public static FileAttribute<Set<PosixFilePermission>> getNoRead() {
        return PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString(NO_READ));
    }

    public static FileAttribute<Set<PosixFilePermission>> getNoWrite() {
        return PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString(NO_WRITE));
    }

    public static Answer<Void> onEventLatchCountdown(CountDownLatch latch) {
        return invocation -> {
            if (latch.getCount() == 0) {
                throw new IllegalStateException("Event calls exceeded");
            }
            latch.countDown();
            return null;
        };
    }
}
