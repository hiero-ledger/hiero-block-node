// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.commands.days.listing;

import static org.hiero.block.tools.utils.PrettyPrint.prettyPrintFileSize;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import org.hiero.block.tools.utils.PrettyPrint;

/**
 * App to read giant JSON listing file and print summary info. It parses the JSON and extracts file info. It also
 * writes bad JSON lines that are rejected to a separate file <pre>bad-lines&lt;DATE&gt;.txt.</pre>
 *
 * <b>Example run command:</b>
 * <pre>jdk-25/bin/java -XX:+UseParallelGC -XX:GCTimeRatio=1 -XX:-UseGCOverheadLimit -XX:+AlwaysPreTouch
 *  -XX:SoftRefLRUPolicyMSPerMB=0 -XX:+HeapDumpOnOutOfMemoryError -Xmx64g -XX:+UseCompactObjectHeaders
 *  -cp record-file-dedup-1.0-SNAPSHOT-all.jar  com.hedera.recorddedup.listing.JsonFileScanner</pre>
 *
 * <b>Example JSON line:</b>
 * <pre>{"Path":"record0.0.23/2025-01-30T18_20_36.339511000Z.rcd.gz","Name":"2025-01-30T18_20_36.339511000Z.rcd.gz",
 * "Size":3225,"ModTime":"","IsDir":false,"Hashes":{"md5":"eaf4861782da61994ef60f8a2f230e93"}},</pre>
 */
public class JsonFileScanner {
    /** Regex pattern to extract fields from JSON line */
    private static final Pattern EXTACT_FILEDS = Pattern.compile("\"(Path|Name|Size|IsDir|md5)\":\"?([^\",}]+)\"?");

    /**
     * Main method to run the JSON file scanner. in a test mode that just prints summary info.
     *
     * @param args command line arguments, ignored
     * @throws IOException if file operations fail
     */
    public static void main(String[] args) throws IOException {
        // json file to read
        final Path jsonFile = Path.of("files.json");
        System.out.println("jsonFile = " + jsonFile);
        // simple size and file counting handler
        final AtomicLong totalSize = new AtomicLong(0);
        final Set<String> fileExtensions = new CopyOnWriteArraySet<>();
        final long totalFiles = scanJsonFile(jsonFile, (path, name, size, md5Hex) -> {
            totalSize.addAndGet(size);
            int idx = name.lastIndexOf('.');
            if (idx > 0 && idx < name.length() - 1) {
                fileExtensions.add(name.substring(idx + 1).toLowerCase());
            } else {
                fileExtensions.add(""); // no extension
            }
        });
        System.out.println("\nTotal files: " + totalFiles + ", total size: " + prettyPrintFileSize(totalSize.get())
                + " (" + totalSize.get() + "bytes)");
        System.out.println("File extensions: ");
        fileExtensions.stream()
                .sorted()
                .forEach(ext -> System.out.println("  " + (ext.isEmpty() ? "<no extension>" : ext)));
    }

    /**
     * Scans the given JSON file, extracts file information, and processes each file using the provided handler.
     * It also prints a progress bar to the console.
     *
     * @param jsonFile the path to the JSON file to scan
     * @param recordFileHandler the handler to process each extracted file information
     * @return the total number of files processed
     * @throws IOException if an I/O error occurs reading the file
     */
    public static long scanJsonFile(Path jsonFile, RecordFileHandler recordFileHandler) throws IOException {
        final long fileSize = Files.size(jsonFile);
        final long printInterval = Math.max(fileSize / 10_000, 1);
        final AtomicLong totalChars = new AtomicLong(0);
        final AtomicLong lastPrint = new AtomicLong(0);
        try (var linesStream = Files.lines(jsonFile);
                BadLinesWriter badLinesWriter = new BadLinesWriter()) {
            final long fileCount = linesStream
                    .parallel()
                    .mapToLong(line -> {
                        String path = null;
                        String name = null;
                        int size = 0;
                        boolean isDir = false;
                        String md5Hex = null;
                        var matcher = EXTACT_FILEDS.matcher(line);
                        while (matcher.find()) {
                            String field = matcher.group(1);
                            String value = matcher.group(2);
                            switch (field) {
                                case "Path" -> path = value;
                                case "Name" -> name = value;
                                case "Size" -> size = Integer.parseInt(value);
                                case "IsDir" ->
                                    isDir = Boolean.parseBoolean(value)
                                            || value.toLowerCase().contains("true");
                                case "md5" -> md5Hex = value;
                            }
                        }
                        // ignore directories and small lines like "[" or "]"
                        if (!isDir && line.length() > 3) {
                            // Extract file name from path if not present
                            if (name == null && path != null) {
                                int idx = path.lastIndexOf('/');
                                name = (idx >= 0) ? path.substring(idx + 1) : path;
                            }
                            // check if required fields are present
                            if (path == null || size < 0 || md5Hex == null) {
                                throw new RuntimeException("Missing required fields in JSON object: path=" + path
                                        + ", name=" + size + ", name=" + size + ", md5Hex=" + md5Hex + ", \nline=>>>"
                                        + line + "<<<");
                            }
                            recordFileHandler.handle(path, name, size, md5Hex);
                        } else {
                            badLinesWriter.writeBadLine(line);
                        }

                        // Print progress bar
                        final long charRead = totalChars.addAndGet(line.length() + 1); // +1 for newline
                        if (charRead - lastPrint.get() >= printInterval) {
                            PrettyPrint.printProgress(charRead, fileSize);
                            lastPrint.set(charRead);
                        }
                        return 1;
                    })
                    .sum();
            PrettyPrint.printProgress(fileSize, fileSize); // ensure 100% at end
            return fileCount;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Functional interface for handling extracted file information.
     */
    public interface RecordFileHandler {
        void handle(String path, String name, int size, String md5Hex);
    }
}
