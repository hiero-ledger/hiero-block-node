// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.commands.history;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.hiero.block.tools.commands.history.model.InMemoryFile;
import org.hiero.block.tools.commands.history.model.InMemoryRecordFileSet;

/**
 * Utility to read and group record files from a compressed daily tar archive compressed with zstd.
 *
 * <p>This class invokes the system {@code zstd} binary to stream-decompress a {@code .tar.zstd}
 * archive and then reads the contained TAR entries with Apache Commons Compress. Files are
 * grouped by their parent directory (typically a per-timestamp subdirectory) and assembled into
 * {@link InMemoryRecordFileSet} instances which contain the primary record file, any alternate
 * record files, signature files and sidecars.</p>
 *
 * <p>Filename conventions understood by this reader:
 * <ul>
 *   <li>Record files end with {@code .rcd}. The primary record file name is the timestamp only,
 *       for example {@code 2019-09-13T22_48_30.277013Z.rcd}.</li>
 *   <li>Other record files produced by individual nodes use a {@code _node_<id>} suffix, e.g.
 *       {@code 2019-09-13T22_48_30.277013Z_node_21.rcd}.</li>
 *   <li>Primary sidecar files are indexed like {@code 2019-09-13T22_48_30.277013Z_1.rcd} (index
 *       starts at 1). Node-specific sidecars append a {@code _node_<id>} token,
 *       e.g. {@code 2019-09-13T22_48_30.277013Z_1_node_21.rcd}.</li>
 *   <li>Signature files end with {@code .rcs_sig} and are often named as {@code node_<address>.rcs_sig}
 *       and colocated inside the timestamp directory.</li>
 * </ul>
 *
 * <p>Important notes:
 * <ul>
 *   <li>This implementation reads each TAR entry fully into memory (see
 *       {@link #readEntryFully}). Large archives will consume memory proportional to the largest
 *       entry read concurrently. If you need a streaming/lower-memory alternative, convert the
 *       reader to yield sets lazily as a Spliterator that processes entries incrementally.</li>
 *   <li>The class executes an external {@code zstd} process; ensure the utility is available on
 *       the PATH on the host that runs this code. The command used is {@code zstd -d -c <file>}.
 *       Any failure of the external process is surfaced as a runtime exception.</li>
 * </ul>
 */
public class TarZstdDayReader {

    /**
     * Decompresses the given {@code .tar.zstd} file and returns a stream of
     * {@link InMemoryRecordFileSet} grouped by the per-timestamp directory structure in the
     * archive.
     *
     * <p>Behavioral summary:
     * <ul>
     *   <li>Spawns {@code zstd -d -c <zstdFile>} as a subprocess and pipes the decompressed TAR
     *       bytes into an {@link TarArchiveInputStream}.</li>
     *   <li>Reads TAR entries and groups files by parent directory; when a directory boundary is
     *       observed the accumulated files are processed into one or more
     *       {@link InMemoryRecordFileSet} instances.</li>
     *   <li>Ensures the zstd process exits and throws a {@link RuntimeException} if it terminates
     *       with a non-zero exit code or if reading the stream fails.</li>
     * </ul>
     *
     * @param zstdFile the path to a .tar.zstd archive; must not be {@code null}
     * @return a {@link Stream} of {@link InMemoryRecordFileSet} representing grouped record files
     *         found in the archive. The caller should consume or close the stream promptly.
     * @throws IllegalArgumentException if {@code zstdFile} is {@code null}
     * @throws RuntimeException if launching or reading from the zstd process fails, or if the
     *         zstd process returns a non-zero exit code
     *
     * @apiNote the returned Stream is built from an in-memory list collected while reading the
     * archive inside this method. If you need true lazy streaming (emitting sets while still
     * reading the archive), consider refactoring to a streaming/Spliterator approach.
     */
    public static Stream<InMemoryRecordFileSet> streamTarZstd(Path zstdFile) {
        if (zstdFile == null) throw new IllegalArgumentException("zstdFile is null");

        // Build safe shell command that ensures zstd exists and writes decompressed tar to stdout
        String cmd = String.join(
                " ", "set -euo pipefail;", "command -v zstd >/dev/null 2>&1;", "zstd -d -c", shq(zstdFile.toString()));

        ProcessBuilder pb = new ProcessBuilder("bash", "-lc", cmd);
        Process p;
        try {
            p = pb.start();
        } catch (IOException ioe) {
            throw new RuntimeException("IOException starting zstd process for file: " + zstdFile, ioe);
        }

        List<InMemoryRecordFileSet> results = new ArrayList<>();

        try (InputStream procOut = p.getInputStream();
                TarArchiveInputStream tar = new TarArchiveInputStream(procOut)) {

            TarArchiveEntry entry;
            String currentDir = null;
            List<InMemoryFile> currentFiles = new ArrayList<>();

            while ((entry = tar.getNextEntry()) != null) {
                if (entry.isDirectory()) continue; // skip directory entries

                String entryName = entry.getName();
                String parentDir = parentDirectory(entryName);

                // Detect directory boundary change (works for tar archives where entries for a directory are grouped)
                if (currentDir == null) {
                    currentDir = parentDir;
                } else if (!currentDir.equals(parentDir)) {
                    // process previous directory batch
                    processDirectoryFiles(currentDir, currentFiles, results);
                    currentFiles.clear();
                    currentDir = parentDir;
                }

                // Read entry content into memory (streamed)
                byte[] data = readEntryFully(tar, entry.getSize());
                currentFiles.add(new InMemoryFile(Path.of(entryName), data));
            }

            // process remaining files
            if (currentDir != null && !currentFiles.isEmpty()) {
                processDirectoryFiles(currentDir, currentFiles, results);
            }

        } catch (IOException ioe) {
            // If we get an IOException here, the process may still be running; ensure we destroy it
            p.destroyForcibly();
            throw new RuntimeException("IOException processing tar.zstd file: " + zstdFile, ioe);
        }
        // After closing streams, ensure process exit and check return code (avoid throws inside finally)
        int rc;
        try {
            rc = p.waitFor();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted waiting for zstd process", ie);
        }
        if (rc != 0) {
            throw new RuntimeException("zstd process exited with code " + rc);
        }

        return results.stream();
    }

    /**
     * Process a batch of files that belong to the same parent directory and append the resulting
     * {@link InMemoryRecordFileSet} objects to {@code results}.
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
     *       {@link InMemoryRecordFileSet} in index order.</li>
     * </ul>
     *
     * @param currentDir the parent directory path (as a string ending with '/'), may be {@code "/"}
     *                   when entries are at the archive root; used to infer a directory-level base
     *                   key for signatures that do not include timestamps in their names
     * @param currentFiles files read from the TAR that share the same parent directory; may include
     *                     {@code .rcd} and {@code .rcs_sig} files
     * @param results the list to append created {@link InMemoryRecordFileSet} instances to
     */
    @SuppressWarnings("ReplaceNullCheck")
    private static void processDirectoryFiles(
            String currentDir, List<InMemoryFile> currentFiles, List<InMemoryRecordFileSet> results) {
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
        Map<String, List<InMemoryFile>> byBase = new HashMap<>();
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

            // find primary record file: exact match baseKey + ".rcd"
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
                throw new RuntimeException(
                        "Primary record file not found for baseKey='" + baseKey + "' in dir='" + currentDir + "'");
            }

            // There must be at least one signature file for the group; enforce invariant
            if (signatureFiles.isEmpty()) {
                System.err.println("Missing signature files for baseKey='" + baseKey + "' in dir='" + currentDir + "'");
                throw new RuntimeException(
                        "No signature files found for baseKey='" + baseKey + "' in dir='" + currentDir + "'");
            }

            // classify other record files (exclude the primary)
            for (InMemoryFile f : rcdFiles) {
                if (f == primaryRecord) continue;
                String name = f.path().getFileName().toString();
                String noExt = name.substring(0, name.length() - 4);
                // classify sidecars vs other record files: non-sidecars are treated as otherRecordFiles
                if (!noExt.matches(Pattern.quote(baseKey) + "_\\d+(_node_\\d+)?$")) {
                    otherRecordFiles.add(f);
                }
            }

            // sidecars: collect primary sidecars (no node suffix) per index and other sidecars (with node suffix)
            Map<Integer, InMemoryFile> primarySidecarMap = new HashMap<>();
            List<InMemoryFile> otherSidecarFiles = new ArrayList<>();

            Pattern primarySidecarPattern = Pattern.compile(Pattern.quote(baseKey) + "_(\\d+)$");
            Pattern otherSidecarPattern = Pattern.compile(Pattern.quote(baseKey) + "_(\\d+)_node_\\d+$");

            for (InMemoryFile f : rcdFiles) {
                String name = f.path().getFileName().toString();
                String noExt = name.substring(0, name.length() - 4);
                Matcher mPrimary = primarySidecarPattern.matcher(noExt);
                Matcher mOther = otherSidecarPattern.matcher(noExt);
                if (mPrimary.matches()) {
                    int idx = Integer.parseInt(mPrimary.group(1));
                    primarySidecarMap.put(idx, f);
                } else if (mOther.matches()) {
                    otherSidecarFiles.add(f);
                }
            }

            // Build ordered list of primary sidecars by index (1..max)
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

            InMemoryRecordFileSet set = new InMemoryRecordFileSet(
                    recordTime, primaryRecord, otherRecordFiles, signatureFiles, primarySidecars, otherSidecarFiles);

            results.add(set);
        }
    }

    /**
     * Read the full contents of the current TAR entry from the provided {@link InputStream} and
     * return its bytes.
     *
     * <p>If {@code sizeHint} is positive and reasonable it is used to pre-size the byte buffer.
     * Otherwise the method reads until the entry stream EOF. This method blocks until the entry
     * has been fully consumed or an {@link IOException} occurs.
     *
     * @param in the input stream positioned at the start of the TAR entry payload
     * @param sizeHint the size of the entry if known, or a non-positive value if unknown
     * @return the entry's bytes
     * @throws IOException if an I/O error occurs while reading the entry
     */
    private static byte[] readEntryFully(InputStream in, long sizeHint) throws IOException {
        ByteArrayOutputStream baos =
                new ByteArrayOutputStream(sizeHint > 0 && sizeHint <= Integer.MAX_VALUE ? (int) sizeHint : 8192);
        byte[] buffer = new byte[8192];
        long remaining = sizeHint;

        if (sizeHint > 0) {
            while (remaining > 0) {
                int toRead = (int) Math.min(buffer.length, remaining);
                int r = in.read(buffer, 0, toRead);
                if (r == -1) break;
                baos.write(buffer, 0, r);
                remaining -= r;
            }
        } else {
            // size unknown: read until EOF for this entry
            int r;
            while ((r = in.read(buffer)) != -1) {
                baos.write(buffer, 0, r);
            }
        }
        return baos.toByteArray();
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
     * Shell-quote a string for safe single-quoted use inside a bash command line.
     *
     * <p>This helper produces a single-quoted shell token that is safe to insert into a command
     * passed to {@code bash -lc}. It escapes existing single quotes in the input by closing the
     * single-quoted region, inserting {@code '"'"'} and reopening it.
     *
     * @param s the raw string to quote
     * @return a safely quoted string suitable for inclusion in a bash single-quoted token
     */
    private static String shq(String s) {
        return "'" + s.replace("'", "'\"'\"'") + "'";
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
        // remove known extensions
        String noExt = filename;
        // Some signature files are named like <base>.rcd.rcs_sig — strip both suffixes in that order.
        if (noExt.endsWith(".rcd.rcs_sig")) {
            noExt = noExt.substring(0, noExt.length() - ".rcd.rcs_sig".length());
        } else if (noExt.endsWith(".rcs_sig")) {
            noExt = noExt.substring(0, noExt.length() - ".rcs_sig".length());
        } else if (noExt.endsWith(".rcd")) {
            noExt = noExt.substring(0, noExt.length() - ".rcd".length());
        }

        // strip node suffixes like _node_21 and sidecar indexes like _1
        // We want the pure timestamp prefix like 2019-09-13T22_48_30.277013Z
        noExt = noExt.replaceAll("_node_\\d+$", "");
        noExt = noExt.replaceAll("_(\\d+)$", "");
        return noExt;
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
