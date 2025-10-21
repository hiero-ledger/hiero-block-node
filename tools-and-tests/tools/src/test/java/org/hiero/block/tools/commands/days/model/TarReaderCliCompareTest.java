package org.hiero.block.tools.commands.days.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.github.luben.zstd.ZstdInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.hiero.block.tools.utils.TarReader;
import org.junit.jupiter.api.Test;

/**
 * Test class that compares the output of TarReader with the system tar command for a zstd-compressed tar file.
 */
@SuppressWarnings("DataFlowIssue")
public class TarReaderCliCompareTest {

    @Test
    public void compareWithTarCommand() throws Exception {
        assumeTrue(isAvailable("zstd"), "Skipping test: zstd not available");
        assumeTrue(isAvailable("tar"), "Skipping test: tar not available");

        Path resourcePath = Path.of(TarReaderCliCompareTest.class.getResource("/2019-09-13.tar.zstd").toURI());
        // First read using tar CLI
        final TreeMap<String, Long> cliMap = listTarFilesAndSizes(resourcePath);
        // Now read using TarReader
        final TreeMap<String, Long> tarReaderMap = new TreeMap<>();
        try (var stream = TarReader.readTarContents(new ZstdInputStream(
                TarReaderCliCompareTest.class.getResourceAsStream("/2019-09-13.tar.zstd")))) {
            stream.forEach(inMemFile ->
                tarReaderMap.put(inMemFile.path().toString(), (long) inMemFile.data().length));
        }
        // first compare sizes of both maps
        assertEquals(cliMap.size(), tarReaderMap.size(), "Number of files must match between tar CLI and TarReader");
        // now compare the name and size of first files
        if (!cliMap.isEmpty() && !tarReaderMap.isEmpty()) {
            String firstCliKey = cliMap.firstKey();
            String firstTarReaderKey = tarReaderMap.firstKey();
            assertEquals(firstCliKey, firstTarReaderKey, "First file names must match between tar CLI and TarReader");
            assertEquals(cliMap.get(firstCliKey), tarReaderMap.get(firstTarReaderKey),
                    "First file sizes must match between tar CLI and TarReader");
        }
        // Compare keys and sizes
        assertEquals(cliMap.keySet(), tarReaderMap.keySet(), "File sets must match between tar CLI and TarReader");
        for (String k : cliMap.keySet()) {
            assertEquals(cliMap.get(k), tarReaderMap.get(k), "Size mismatch for " + k);
        }
    }

    /**
     * Check if a command is available in the system PATH.
     *
     * @param cmd the command to check
     * @return true if the command is available, false otherwise
     */
    private static boolean isAvailable(String cmd) {
        try {
            Process p = new ProcessBuilder("which", cmd).start();
            int rc = p.waitFor();
            return rc == 0;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * List files and their sizes in a zstd-compressed tar archive using the tar CLI.
     *
     * @param tarFilePath path to the zstd-compressed tar file
     * @return a map of file names to their sizes
     * @throws Exception if an error occurs while executing the command or parsing output
     */
    private static TreeMap<String, Long> listTarFilesAndSizes(Path tarFilePath) throws Exception {
        // Run shell command: zstd -dc <file> | tar -tvf -
        String cmd = String.format("zstd -dc '%s' | tar -tvf -", tarFilePath.toAbsolutePath());
        ProcessBuilder pb = new ProcessBuilder("sh", "-c", cmd);
        pb.redirectErrorStream(true);
        Process proc = pb.start();

        TreeMap<String, Long> cliMap = new TreeMap<>();
        // pattern to parse tar -tvf output lines, like:
        // -rw-r--r--  0 0      0         438 Oct  8 16:19 2019-09-13T23_59_56.056644Z/node_0.0.7.rcs_sig
        // -rw-r--r--  0 0      0         438 Oct  8 16:19 2019-09-13T23_59_56.056644Z/node_0.0.8.rcs_sig
        // -rw-r--r--  0 0      0         438 Oct  8 16:19 2019-09-13T23_59_56.056644Z/node_0.0.9.rcs_sig
        Pattern p = Pattern.compile("^(\\S+)\\s+\\S+\\s+\\S+\\s+\\S+\\s+(\\d+)\\s+\\S+\\s+\\d+\\s+\\S+\\s+(.+)$");
        try (BufferedReader br = new BufferedReader(
            new InputStreamReader(proc.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty())
                    continue;
                Matcher m = p.matcher(line);
                if (!m.matches())
                    continue;
                String perms = m.group(1);
                String sizeToken = m.group(2);
                String filename = m.group(3);
                // Only include regular files (perms starting with '-')
                if (perms.isEmpty() || perms.charAt(0) != '-')
                    continue;
                long size = Long.parseLong(sizeToken);
                cliMap.put(filename, size);
            }
        }
        int rc = proc.waitFor();
        if (rc != 0) {
            throw new RuntimeException("tar command failed with exit code " + rc);
        }
        return cliMap;
    }

    /**
     * Main method to run the listing independently.
     */
    public static void main(String[] args) throws Exception {
        Path resourcePath = Path.of(TarReaderCliCompareTest.class.getResource("/2019-09-13.tar.zstd").toURI());
        final TreeMap<String, Long> cliMap = listTarFilesAndSizes(resourcePath);
        // print size
        System.out.printf("Found %,d files in tar archive:%n", cliMap.size());
        // print first 10 entries
        int count = 0;
        for (Map.Entry<String, Long> entry : cliMap.entrySet()) {
            System.out.printf("%s : %d%n", entry.getKey(), entry.getValue());
            count++;
            if (count >= 10) break;
        }
    }
}
