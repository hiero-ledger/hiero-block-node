// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.utils;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class ProgressDemo {
    public static void main(String[] args) throws Exception {
        int total = 500;
        DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
        for (int i = 0; i < total; i++) {
            // simulate a block timestamp every 50 items
            LocalDateTime ts = LocalDateTime.now().minusMinutes(total - i);
            String block = fmt.format(ts);
            String fileName = "record_" + (i % 10) + ".rcd";
            PrettyPrint.printProgress(i, total, "block=" + block + " :: " + fileName);
            // every so often, print a normal log line to ensure progress is cleared first
            if (i == 123 || i == 321) {
                PrettyPrint.clearProgress();
                System.out.println("\n[INFO] checkpoint at i=" + i + ", block=" + block);
            }
            Thread.sleep(10);
        }
        PrettyPrint.clearProgress();
        System.out.println("All done");
    }
}
