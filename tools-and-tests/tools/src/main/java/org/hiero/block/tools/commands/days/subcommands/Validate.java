// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.commands.days.subcommands;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.File;
import java.util.concurrent.atomic.AtomicReference;
import org.hiero.block.tools.commands.days.model.TarZstdUtils;
import org.hiero.block.tools.records.RecordFileInfo;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

@SuppressWarnings("CallToPrintStackTrace")
@Command(name = "validate", description = "Validate blocks using BlockchainValidator for each record file set")
public class Validate implements Runnable {
    private static final Bytes ZERO_HASH = Bytes.wrap(new byte[48]);

    @Spec
    CommandSpec spec;

    @Parameters(index = "0..*", description = "Files or directories to process")
    private final File[] compressedDayOrDaysDirs = new File[0];

    @Override
    public void run() {
        // If no inputs are provided, print usage help for this subcommand
        if (compressedDayOrDaysDirs.length == 0) {
            spec.commandLine().usage(spec.commandLine().getOut());
            return;
        }

        final AtomicReference<Bytes> carryOverHash = new AtomicReference<>(ZERO_HASH);
        System.out.println("Staring hash[" + carryOverHash.get() + "]");
        TarZstdUtils.processPaths(compressedDayOrDaysDirs, set -> {
            try {
                final RecordFileInfo recordFileInfo =
                        RecordFileInfo.parse(set.primaryRecordFile().data());
                final Bytes previousBlockHash = carryOverHash.getAndSet(recordFileInfo.blockHash());
                System.out.printf(
                        "\r%-32s prev[%s] hash[%s]",
                        set.recordFileTime(),
                        recordFileInfo.previousBlockHash().toString().substring(0, 8),
                        recordFileInfo.blockHash().toString().substring(0, 8));
                if (recordFileInfo.previousBlockHash().equals(previousBlockHash)) {
                    System.out.println(" -> VALID");
                } else {
                    System.out.println(" Validation failed!!");
                    System.out.flush();
                    System.exit(1);
                }
            } catch (Exception ex) {
                System.err.println("Validation threw for " + set.recordFileTime() + ": " + ex.getMessage());
                ex.printStackTrace();
                System.exit(1);
            }
        });
    }
}
