package org.hiero.block.tools.commands.days.listing;

import static java.nio.file.StandardOpenOption.CREATE;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Writer that writes binary listing files full of serialized RecordFile. The file starts with a long for number of
 * RecordFiles contained then repeated serialized RecordFile objects.
 */
public class DayListingFileWriter implements AutoCloseable {
    private final Path filePath;
    private final DataOutputStream out;
    private long recordCount = 0;
    private long recordSigCount = 0;
    private long recordSidecarCount = 0;

    public DayListingFileWriter(int year, int month, int day) throws IOException {
        this.filePath = ListingDir.getFileForDay(year, month, day);
        this.out = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(filePath, CREATE), 4096));
        out.writeLong(0); // reserve space for number of files
    }

    public synchronized void writeRecordFile(ListingRecordFile recordFile) throws IOException {
        // Write unshared to avoid retaining back-references; reset periodically to clear tables.
        recordFile.write(out);
        switch (recordFile.type()) {
            case RECORD -> recordCount++;
            case RECORD_SIG -> recordSigCount++;
            case RECORD_SIDECAR -> recordSidecarCount++;
        }
    }

    @Override
    public synchronized String toString() {
        return "DayListingFileWriter{" +
                "filePath=" + filePath +
                ", recordCount=" + recordCount +
                ", recordSigCount=" + recordSigCount +
                ", recordSidecarCount=" + recordSidecarCount +
                '}';
    }

    @Override
    public synchronized void close() throws Exception {
        out.flush();
        out.close();
        // reopen and write total number of files
        try (RandomAccessFile raf = new RandomAccessFile(filePath.toFile(), "rw")) {
            raf.writeLong(recordCount + recordSigCount + recordSidecarCount);
        }
    }
}
