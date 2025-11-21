// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.days.model;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.hiero.block.tools.records.model.unparsed.InMemoryFile;
import org.hiero.block.tools.records.model.unparsed.UnparsedRecordBlock;
import org.hiero.block.tools.utils.TarReader;
import org.hiero.block.tools.utils.ZstCmdInputStream;

/**
 * Utility to read and group record files from a compressed daily tar archive compressed with zstd.
 *
 * <p>This class previously invoked system {@code zstd} and {@code tar} binaries in a two-pass
 * approach. It now uses {@link ZstCmdInputStream} to decompress the .zstd archive and
 * {@link TarReader} to stream tar entries directly in Java.
 */
public class TarZstdDayReaderUsingExec {

    /**
     * Decompresses the given {@code .tar.zstd} file and returns a stream of
     * {@link UnparsedRecordBlock} grouped by the per-timestamp directory structure in the
     * archive.
     * <p>Uses subprocess-based decompression by default.</p>
     *
     * @param zstdFile the path to a .tar.zstd archive; must not be {@code null}
     * @return a {@link Stream} of {@link UnparsedRecordBlock} representing grouped record files
     *         found in the archive. The caller should consume or close the stream promptly.
     */
    @SuppressWarnings("unused")
    public static Stream<UnparsedRecordBlock> streamTarZstd(Path zstdFile) {
        return streamTarZstd(zstdFile, false);
    }

    /**
     * Decompresses the given {@code .tar.zstd} file and returns a stream of
     * {@link UnparsedRecordBlock} grouped by the per-timestamp directory structure in the
     * archive.
     *
     * @param zstdFile the path to a .tar.zstd archive; must not be {@code null}
     * @param useJni if true, use zstd-jni library; if false, use subprocess (ProcessBuilder)
     * @return a {@link Stream} of {@link UnparsedRecordBlock} representing grouped record files
     *         found in the archive. The caller should consume or close the stream promptly.
     */
    @SuppressWarnings("unused")
    public static Stream<UnparsedRecordBlock> streamTarZstd(Path zstdFile, boolean useJni) {
        if (zstdFile == null) throw new IllegalArgumentException("zstdFile is null");

        final Stream<InMemoryFile> filesStream;
        try {
            if (useJni) {
                // Use zstd-jni library
                java.io.InputStream zstIn = new java.io.FileInputStream(zstdFile.toFile());
                java.io.InputStream decompressed = new com.github.luben.zstd.ZstdInputStream(zstIn);
                filesStream = TarReader.readTarContents(decompressed);
            } else {
                // Use subprocess
                ZstCmdInputStream zstIn = new ZstCmdInputStream(zstdFile);
                filesStream = TarReader.readTarContents(zstIn);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to open zstd input: " + zstdFile, e);
        }

        final Iterator<InMemoryFile> it = filesStream.iterator();

        Spliterator<UnparsedRecordBlock> spl = new Spliterator<>() {
            private final List<InMemoryFile> buffer = new ArrayList<>();
            private final Deque<UnparsedRecordBlock> ready = new ArrayDeque<>();
            private String currentDir = null;
            private boolean finished = false;

            @Override
            public boolean tryAdvance(Consumer<? super UnparsedRecordBlock> action) {
                if (action == null) throw new NullPointerException();
                if (finished) return false;

                // If we already have ready blocks, emit one
                if (!ready.isEmpty()) {
                    action.accept(ready.removeFirst());
                    return true;
                }

                while (it.hasNext()) {
                    InMemoryFile f = it.next();
                    String entryName = f.path().toString();
                    String parentDir = parentDirectory(entryName);
                    if (currentDir == null) {
                        currentDir = parentDir;
                    } else if (!currentDir.equals(parentDir)) {
                        // process buffered files for previous directory -> collect results into a temp list then
                        // enqueue
                        List<UnparsedRecordBlock> tmp = new ArrayList<>();
                        processDirectoryFiles(currentDir, buffer, tmp);
                        for (UnparsedRecordBlock b : tmp) ready.addLast(b);
                        buffer.clear();
                        currentDir = parentDir;
                        // add the current file to the new buffer
                        buffer.add(f);
                        if (!ready.isEmpty()) {
                            action.accept(ready.removeFirst());
                            return true;
                        }
                        continue;
                    }
                    buffer.add(f);
                }

                // Reached end-of-input; process any remaining buffered files
                finished = true;
                if (currentDir != null && !buffer.isEmpty()) {
                    // processDirectoryFiles expects a List<InMemoryBlock> for results; collect into a temporary list
                    // then move into deque
                    List<UnparsedRecordBlock> tmp = new ArrayList<>();
                    processDirectoryFiles(currentDir, buffer, tmp);
                    for (UnparsedRecordBlock b : tmp) ready.addLast(b);
                    buffer.clear();
                }

                if (!ready.isEmpty()) {
                    action.accept(ready.removeFirst());
                    return true;
                }

                // No more data; close the underlying filesStream to ensure resources are released even if caller
                // doesn't explicitly close the returned Stream.
                try {
                    filesStream.close();
                } catch (Throwable ignore) {
                }
                return false;
            }

            @Override
            public Spliterator<UnparsedRecordBlock> trySplit() {
                return null;
            }

            @Override
            public long estimateSize() {
                return Long.MAX_VALUE;
            }

            @Override
            public int characteristics() {
                return 0;
            }
        };

        // Build a stream from the spliterator and ensure underlying streams are closed when the stream is closed
        return StreamSupport.stream(spl, false).onClose(() -> {
            try {
                filesStream.close(); // TarReader will close the zstIn when filesStream is closed
            } catch (Throwable t) {
                // swallow - closing best-effort
            }
        });
    }

    /**
     * Decompresses the given {@code .tar.zstd} file and returns a list of
     * {@link UnparsedRecordBlock} grouped by the per-timestamp directory structure in the
     * archive.
     *
     * @param zstdFile the path to a .tar.zstd archive; must not be {@code null}
     * @return a {@link List} of {@link UnparsedRecordBlock} representing grouped record files
     *         found in the archive.
     */
    public static List<UnparsedRecordBlock> readTarZstd(Path zstdFile) {
        if (zstdFile == null) throw new IllegalArgumentException("zstdFile is null");
        final List<UnparsedRecordBlock> results = new ArrayList<>();

        // Use ZstCmdInputStream to decompress and TarReader to stream tar entries.
        try (ZstCmdInputStream zstIn = new ZstCmdInputStream(zstdFile);
                Stream<InMemoryFile> files = TarReader.readTarContents(zstIn)) {

            List<InMemoryFile> currentFiles = new ArrayList<>();
            String currentDir = null;

            Iterator<InMemoryFile> it = files.iterator();
            while (it.hasNext()) {
                InMemoryFile f = it.next();
                String entryName = f.path().toString();
                String parentDir = parentDirectory(entryName);
                if (currentDir == null) currentDir = parentDir;
                else if (!currentDir.equals(parentDir)) {
                    processDirectoryFiles(currentDir, currentFiles, results);
                    currentFiles.clear();
                    currentDir = parentDir;
                }
                currentFiles.add(f);
            }

            if (currentDir != null && !currentFiles.isEmpty()) {
                processDirectoryFiles(currentDir, currentFiles, results);
            }

        } catch (IOException ioe) {
            throw new RuntimeException("IOException while reading tar.zstd file: " + zstdFile, ioe);
        }

        return results;
    }

    /**
     * Process a batch of files that belong to the same parent directory and append the resulting
     * {@link UnparsedRecordBlock} objects to {@code results}.
     *
     * <p>This method implements the grouping and classification rules:
     * <ul>
     *   <li>Collects all {@code .rcd} files and groups them by their extracted base key
     *       (timestamp portion).</li>
     *   <li>Collects signature files ({@code .rcs_sig}) and attempts to associate them with an
     *       existing record-group by matching an extracted base key or by placing them under the
     *       timestamp directory's base key when appropriate.</li>
     *   <li>Detects the primary record file (exact match to {@code baseKey + ".rcd"}) and
     *       classifies other {@code .rcd} files as other-record or sidecar files based on naming
     *       patterns.</li>
     *   <li>Primary sidecars are ordered by index (1..N) when present and attached to the
     *       {@link UnparsedRecordBlock} in index order.</li>
     * </ul>
     *
     * @param currentDir the parent directory path (as a string ending with '/'), may be {@code "/"}
     *                   when entries are at the archive root; used to infer a directory-level base
     *                   key for signatures that do not include timestamps in their names
     * @param currentFiles files read from the TAR that share the same parent directory; may include
     *                     {@code .rcd} and {@code .rcs_sig} files
     * @param results the list to append created {@link UnparsedRecordBlock} instances to
     */
    @SuppressWarnings("ReplaceNullCheck")
    private static void processDirectoryFiles(
            String currentDir, List<InMemoryFile> currentFiles, List<UnparsedRecordBlock> results) {
        if (currentFiles == null || currentFiles.isEmpty()) return;

        // Compute directory base key if directory name looks like a timestamp directory
        String dirBaseKey = null;
        if (currentDir != null && !"/".equals(currentDir)) {
            String dir = currentDir;
            if (dir.endsWith("/")) dir = dir.substring(0, dir.length() - 1);
            int idx = dir.lastIndexOf('/');
            dirBaseKey = (idx >= 0) ? dir.substring(idx + 1) : dir;
            // dirBaseKey is like 2019-09-13T22_48_30.277013Z when entries are inside the timestamp dir
        }

        // First, collect rcd files and signature files separately
        // Use LinkedHashMap to ensure deterministic iteration order (files processed in tar order)
        Map<String, List<InMemoryFile>> byBase = new LinkedHashMap<>();
        List<InMemoryFile> signatureFilesAll = new ArrayList<>();

        for (InMemoryFile f : currentFiles) {
            String name = f.path().getFileName().toString();
            if (name.endsWith(".rcd")) {
                String baseKey = extractBaseKey(name);
                byBase.computeIfAbsent(baseKey, k -> new ArrayList<>()).add(f);
            } else if (name.endsWith(".rcs_sig")) {
                signatureFilesAll.add(f);
            }
            // ignore other files
        }

        if (byBase.isEmpty() && signatureFilesAll.isEmpty()) return; // nothing interesting

        // Now, associate signature files: try to map them to existing rcd baseKeys; if none matches,
        // and dirBaseKey exists, attach to dirBaseKey group.
        for (InMemoryFile sig : signatureFilesAll) {
            String sigName = sig.path().getFileName().toString();
            String sigBase = extractBaseKey(sigName);
            if (byBase.containsKey(sigBase)) {
                byBase.get(sigBase).add(sig);
            } else if (dirBaseKey != null && byBase.containsKey(dirBaseKey)) {
                byBase.get(dirBaseKey).add(sig);
            } else if (dirBaseKey != null) {
                // create a group keyed by dirBaseKey and add the signature (no rcd files present in that dir)
                byBase.computeIfAbsent(dirBaseKey, k -> new ArrayList<>()).add(sig);
            } else {
                // fallback: group by sigBase alone
                byBase.computeIfAbsent(sigBase, k -> new ArrayList<>()).add(sig);
            }
        }

        // For each group in byBase, now separate signatureFiles and rcdFiles and build sets
        for (Map.Entry<String, List<InMemoryFile>> e : byBase.entrySet()) {
            String baseKey = e.getKey();
            List<InMemoryFile> files = e.getValue();

            List<InMemoryFile> signatureFiles = new ArrayList<>();
            List<InMemoryFile> rcdFiles = new ArrayList<>();

            for (InMemoryFile f : files) {
                String name = f.path().getFileName().toString();
                if (name.endsWith(".rcs_sig")) signatureFiles.add(f);
                else if (name.endsWith(".rcd")) rcdFiles.add(f);
            }

            if (rcdFiles.isEmpty() && signatureFiles.isEmpty()) continue; // nothing to build

            // find the primary record file: exact match baseKey + ".rcd"
            InMemoryFile primaryRecord = null;
            List<InMemoryFile> otherRecordFiles = new ArrayList<>();

            for (InMemoryFile f : rcdFiles) {
                String name = f.path().getFileName().toString();
                String noExt = name.substring(0, name.length() - 4); // remove .rcd
                if (noExt.equals(baseKey)) {
                    primaryRecord = f;
                    break;
                }
            }

            // Enforce invariant: primary record file (exact timestamp .rcd) must exist
            if (primaryRecord == null) {
                System.err.println(
                        "Missing primary record file for baseKey='" + baseKey + "' in dir='" + currentDir + "'");
                for (InMemoryFile f : rcdFiles) System.err.println("    " + f.path());
                throw new RuntimeException(
                        "Primary record file not found for baseKey='" + baseKey + "' in dir='" + currentDir + "'");
            }

            // There must be at least one signature file for the group; enforce invariant
            if (signatureFiles.isEmpty()) {
                System.err.println("Missing signature files for baseKey='" + baseKey + "' in dir='" + currentDir + "'");
                System.err.println(" File set: "
                        + files.stream()
                                .map(InMemoryFile::path)
                                .map(Path::toString)
                                .collect(Collectors.joining(", ")));
                for (InMemoryFile f : rcdFiles) System.err.println("    " + f.path());
                throw new RuntimeException(
                        "No signature files found for baseKey='" + baseKey + "' in dir='" + currentDir + "'");
            }

            // classify other record files (exclude the primary)
            for (InMemoryFile f : rcdFiles) {
                if (f == primaryRecord) continue;
                String name = f.path().getFileName().toString();
                String noExt = name.substring(0, name.length() - 4);
                if (!isSidecarName(noExt, baseKey)) { // non-sidecars -> otherRecordFiles
                    otherRecordFiles.add(f);
                }
            }

            // sidecars: collect primary sidecars (no node suffix) per index and other sidecars (with node suffix)
            Map<Integer, InMemoryFile> primarySidecarMap = new HashMap<>();
            Map<Integer, List<InMemoryFile>> otherSidecarsByIndex = new HashMap<>();
            List<InMemoryFile> otherSidecarFiles = new ArrayList<>();

            for (InMemoryFile f : rcdFiles) {
                String name = f.path().getFileName().toString();
                String noExt = name.substring(0, name.length() - 4);
                int sidecarKind = classifySidecar(noExt, baseKey);
                if (sidecarKind > 0) { // primary sidecar: kind is its index
                    primarySidecarMap.put(sidecarKind, f);
                } else if (sidecarKind == -2) { // other sidecar with node suffix
                    // Extract the index from the other sidecar filename
                    int sidecarIndex = extractSidecarIndex(noExt, baseKey);
                    if (sidecarIndex > 0) {
                        otherSidecarsByIndex
                                .computeIfAbsent(sidecarIndex, k -> new ArrayList<>())
                                .add(f);
                    }
                    otherSidecarFiles.add(f);
                }
            }

            // Handle edge case: if there's no primary sidecar for an index but there are other sidecars,
            // promote one of the other sidecars to be the primary for that index
            for (Map.Entry<Integer, List<InMemoryFile>> entry : otherSidecarsByIndex.entrySet()) {
                int index = entry.getKey();
                if (!primarySidecarMap.containsKey(index)) {
                    // No primary sidecar for this index - pick the first other sidecar as primary
                    List<InMemoryFile> otherSidecarsForIndex = entry.getValue();
                    if (!otherSidecarsForIndex.isEmpty()) {
                        InMemoryFile promoted = otherSidecarsForIndex.getFirst();
                        primarySidecarMap.put(index, promoted);
                        // Remove the promoted sidecar from otherSidecarFiles list
                        otherSidecarFiles.remove(promoted);
                    }
                }
            }

            // Build an ordered list of primary sidecars by index (1…max)
            List<InMemoryFile> primarySidecars = new ArrayList<>();
            if (!primarySidecarMap.isEmpty()) {
                int max = primarySidecarMap.keySet().stream()
                        .mapToInt(Integer::intValue)
                        .max()
                        .orElse(0);
                for (int i = 1; i <= max; i++) {
                    InMemoryFile sf = primarySidecarMap.get(i);
                    if (sf != null) primarySidecars.add(sf);
                }
            }

            // Finally, compose the InMemoryRecordFileSet
            Instant recordTime;
            try {
                recordTime = parseInstantFromBaseKey(baseKey);
            } catch (Exception ex) {
                // if parsing fails, use epoch as fallback to avoid crash and still return a set
                recordTime = Instant.EPOCH;
            }

            UnparsedRecordBlock set = UnparsedRecordBlock.newInMemoryBlock(
                    recordTime, primaryRecord, otherRecordFiles, signatureFiles, primarySidecars, otherSidecarFiles);

            results.add(set);
        }
    }

    /**
     * Return the parent directory portion of a TAR entry name, preserving a trailing slash.
     *
     * <p>Example: {@code parentDirectory("2019-09-13/2019-09-13T23_12_21.610147Z/node_0.0.7.rcs_sig")}
     * returns {@code "2019-09-13/2019-09-13T23_12_21.610147Z/"}.
     *
     * @param entryName the TAR entry path as stored in the archive
     * @return the parent directory with trailing slash, or {@code "/"} when there is no parent
     */
    private static String parentDirectory(String entryName) {
        int idx = entryName.lastIndexOf('/');
        if (idx <= 0) return "/"; // root or no slash
        return entryName.substring(0, idx + 1); // include trailing slash to make it clear it's a directory
    }

    /**
     * Extract the canonical base key (timestamp portion) from record or signature filenames.
     *
     * <p>Examples:
     * <ul>
     *   <li>{@code 2019-09-13T22_48_30.277013Z.rcd} -> {@code 2019-09-13T22_48_30.277013Z}</li>
     *   <li>{@code 2019-09-13T22_48_30.277013Z_1.rcd} -> {@code 2019-09-13T22_48_30.277013Z}</li>
     *   <li>{@code node_0.0.7.rcs_sig} -> {@code node_0.0.7}</li>
     * </ul>
     *
     * <p>The method strips known extensions (".rcd", ".rcs_sig" and the combined
     * ".rcd.rcs_sig" case) and then removes {@code _node_<n>} suffixes and final index suffixes
     * like {@code _1} so that sidecars and node-specific files normalize to the underlying
     * timestamp base key.
     *
     * @param filename the filename to normalize (not the full path)
     * @return the extracted base key
     */
    private static String extractBaseKey(String filename) {
        // remove known extensions without regex to avoid overhead
        String noExt = filename;
        if (noExt.endsWith(".rcd.rcs_sig")) {
            noExt = noExt.substring(0, noExt.length() - ".rcd.rcs_sig".length());
        } else if (noExt.endsWith(".rcs_sig")) {
            noExt = noExt.substring(0, noExt.length() - ".rcs_sig".length());
        } else if (noExt.endsWith(".rcd")) {
            noExt = noExt.substring(0, noExt.length() - ".rcd".length());
        }

        int end = noExt.length();

        // Strip trailing _node_sidecar (for other sidecar files)
        if (noExt.endsWith("_node_sidecar")) {
            end = noExt.length() - "_node_sidecar".length();
        }

        // Strip trailing _node_<id> where <id> is digits and '.'
        int nodeIdx = noExt.lastIndexOf("_node_", end - 1);
        if (nodeIdx >= 0 && nodeIdx < end) {
            boolean ok = true;
            for (int i = nodeIdx + 6; i < end; i++) {
                char c = noExt.charAt(i);
                if (((c - '0') | ('9' - c)) < 0 && c != '.') { // fast digits-or-dot check
                    ok = false;
                    break;
                }
            }
            if (ok) end = nodeIdx;
        }

        // Strip trailing _<digits>
        int us = noExt.lastIndexOf('_', end - 1);
        if (us >= 0) {
            boolean digits = us + 1 < end;
            for (int i = us + 1; i < end; i++) {
                char c = noExt.charAt(i);
                if (((c - '0') | ('9' - c)) < 0) { // not a digit
                    digits = false;
                    break;
                }
            }
            if (digits) end = us;
        }
        return noExt.substring(0, end);
    }

    // Determine whether a noExt name represents a sidecar for baseKey
    private static boolean isSidecarName(String noExt, String baseKey) {
        int kind = classifySidecar(noExt, baseKey);
        return kind > 0 || kind == -2;
    }

    // Classify sidecar: return >0 for primary sidecar index, -2 for node-suffixed sidecar, -1 for not a sidecar
    private static int classifySidecar(String noExt, String baseKey) {
        if (!noExt.startsWith(baseKey)) return -1;
        int pos = baseKey.length();
        if (noExt.length() <= pos + 1 || noExt.charAt(pos) != '_') return -1;
        int i = pos + 1;
        int startDigits = i;
        int idx = 0;
        while (i < noExt.length()) {
            char c = noExt.charAt(i);
            if (c < '0' || c > '9') break;
            idx = (idx * 10) + (c - '0');
            i++;
        }
        if (i == startDigits) return -1; // no digits -> not sidecar
        if (i == noExt.length()) return Math.max(1, idx); // primary sidecar with index
        // Check for node suffix
        if (noExt.startsWith("_node_", i)) {
            int j = i + 6;
            if (j >= noExt.length()) return -1; // empty node id -> not expected
            // Check for _node_sidecar pattern
            if (noExt.substring(j).equals("sidecar")) {
                return -2; // other sidecar with _node_sidecar suffix
            }
            // Check for _node_<digits-and-dots> pattern
            for (; j < noExt.length(); j++) {
                char c = noExt.charAt(j);
                if (((c - '0') | ('9' - c)) < 0 && c != '.') return -1; // invalid char in node id
            }
            return -2; // other sidecar with node suffix
        }
        return -1;
    }

    /**
     * Extract the sidecar index from a sidecar filename (without extension).
     *
     * <p>This method is used to handle edge cases where primary sidecar files (without node suffix)
     * are missing, but "other" sidecar files (with node suffix) exist. By extracting the index from
     * these other sidecar files, we can promote one of them to be the primary sidecar for that index.</p>
     *
     * <p>Examples:
     * <ul>
     *   <li>{@code 2019-09-13T22_48_30.277013Z_1_node_sidecar} -> 1</li>
     *   <li>{@code 2019-09-13T22_48_30.277013Z_01_node_sidecar} -> 1</li>
     *   <li>{@code 2019-09-13T22_48_30.277013Z_12_node_21} -> 12</li>
     *   <li>{@code 2024-06-17T18_21_08.511792844Z_01_node_sidecar} -> 1</li>
     * </ul>
     *
     * @param noExt the filename without extension
     * @param baseKey the base timestamp key
     * @return the sidecar index, or -1 if not a valid sidecar filename
     */
    private static int extractSidecarIndex(String noExt, String baseKey) {
        if (!noExt.startsWith(baseKey)) return -1;
        int pos = baseKey.length();
        if (noExt.length() <= pos + 1 || noExt.charAt(pos) != '_') return -1;
        int i = pos + 1;
        int startDigits = i;
        int idx = 0;
        while (i < noExt.length()) {
            char c = noExt.charAt(i);
            if (c < '0' || c > '9') break;
            idx = (idx * 10) + (c - '0');
            i++;
        }
        if (i == startDigits) return -1; // no digits -> not a valid sidecar
        return Math.max(1, idx); // return the parsed index (minimum 1)
    }

    /**
     * Parse an {@link Instant} from a base key that uses underscores in the time component
     * instead of colons.
     *
     * <p>Example: {@code 2019-09-13T22_48_30.277013Z} is converted to
     * {@code 2019-09-13T22:48:30.277013Z} and parsed with {@link Instant#parse}.
     *
     * @param baseKey the timestamp-like base key with underscores in the time portion
     * @return the parsed {@link Instant}
     * @throws IllegalArgumentException if the {@code baseKey} does not contain a 'T' separator or
     *         cannot be parsed by {@link Instant#parse}
     */
    private static Instant parseInstantFromBaseKey(String baseKey) {
        // baseKey expected like 2019-09-13T22_48_30.277013Z (underscores instead of colons in time)
        // Convert underscores in the time portion (after 'T') to colons and parse
        // e.g. 2019-09-13T22_48_30.277013Z -> 2019-09-13T22:48:30.277013Z

        int tIndex = baseKey.indexOf('T');
        if (tIndex < 0) throw new IllegalArgumentException("Invalid baseKey timestamp: " + baseKey);
        String date = baseKey.substring(0, tIndex + 1);
        String time = baseKey.substring(tIndex + 1);
        // Replace underscores with colons only in the time part up to the 'Z'
        time = time.replace('_', ':');
        String iso = date + time;
        return Instant.parse(iso);
    }
}
