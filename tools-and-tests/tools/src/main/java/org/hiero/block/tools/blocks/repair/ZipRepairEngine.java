// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.repair;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32;
import java.util.zip.Deflater;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;
import org.hiero.block.tools.blocks.repair.RepairResult.RepairStatus;
import org.hiero.block.tools.utils.PrettyPrint;
import picocli.CommandLine.Help.Ansi;

/**
 * Phase 1 engine: scans a directory of zip archives for corrupt Central Directories (CEN) and
 * repairs them by rebuilding the CEN from the intact local-file entries.
 *
 * <h2>Why zips can be corrupt</h2>
 *
 * <p>A zip file's Central Directory is written at the very end when {@link ZipOutputStream#close()}
 * is called. If the {@code wrap} command was killed (SIGKILL, OOM, power loss) before
 * {@code close()} completed, the zip is left without a valid CEN. Additionally, the APPEND-mode
 * {@link ZipOutputStream} bug causes wrong CEN offsets whenever the stream's internal byte counter
 * starts at zero but the underlying file already contained data. Both problems are fixed here.</p>
 *
 * <h2>Performance</h2>
 *
 * <ul>
 *   <li><b>Scan</b> uses a two-seek check per file (~26 bytes read), parallelised across
 *       {@code scanThreads} threads to saturate drive throughput.</li>
 *   <li><b>Repair</b> runs {@code repairThreads} tasks in parallel. For files that fit within
 *       {@link #BUFFER_SIZE}, the entire zip is read into a per-thread {@link ThreadLocal} buffer,
 *       processed in RAM, and written back via a single {@link FileChannel} call — avoiding GC
 *       pressure from repeated large allocations. Files larger than {@link #BUFFER_SIZE} fall back
 *       to a streaming path.</li>
 * </ul>
 */
public class ZipRepairEngine {

    // ── Zip-format constants ──────────────────────────────────────────────────────────────────────

    /** EOCD (End of Central Directory) signature, little-endian. */
    private static final int EOCD_SIGNATURE = 0x06054b50;

    /** CEN (Central Directory Entry) signature, little-endian. */
    private static final int CEN_SIGNATURE = 0x02014b50;

    /** Minimum EOCD record size in bytes (no zip comment). */
    private static final int EOCD_SIZE = 22;

    /** Byte offset of the CEN offset field within the EOCD record. */
    private static final int EOCD_CEN_OFFSET_POS = 16;

    // ── Buffer tuning ─────────────────────────────────────────────────────────────────────────────

    /**
     * Per-thread I/O buffer size (400 MiB).
     *
     * <p>Chosen to accommodate the average corrupt zip (~150 MiB) entirely in RAM. Files larger
     * than this limit fall back to streaming. With {@code repairThreads} threads, total committed
     * memory is approximately {@code 2 × BUFFER_SIZE × repairThreads}.</p>
     */
    public static final int BUFFER_SIZE = 1024 * 1024 * 400;

    /**
     * {@link ByteArrayOutputStream} subclass that exposes its internal buffer so repaired zip bytes
     * can be written to a {@link FileChannel} without an intermediate copy.
     *
     * <p>Calling {@link #reset()} resets the write pointer without releasing the backing array, so
     * the same instance can be reused across repairs on the same thread.</p>
     */
    private static final class RecyclableOutputStream extends ByteArrayOutputStream {

        RecyclableOutputStream(final int initialCapacity) {
            super(initialCapacity);
        }

        /**
         * Write all accumulated bytes to {@code channel}, looping until fully written.
         *
         * @param channel target file channel (must be open for writing)
         * @throws IOException if the channel write fails
         */
        void writeTo(final FileChannel channel) throws IOException {
            final ByteBuffer bb = ByteBuffer.wrap(buf, 0, count);
            while (bb.hasRemaining()) {
                channel.write(bb);
            }
        }
    }

    /**
     * Per-thread read buffer, lazily allocated to {@link #BUFFER_SIZE} bytes.
     *
     * <p>Reused across repair tasks on the same thread. For zip files larger than
     * {@code BUFFER_SIZE} the streaming fallback is used instead.</p>
     */
    private static final ThreadLocal<byte[]> THREAD_READ_BUF = ThreadLocal.withInitial(() -> new byte[BUFFER_SIZE]);

    /**
     * Per-thread write buffer, lazily allocated with initial capacity {@link #BUFFER_SIZE}.
     *
     * <p>{@link RecyclableOutputStream#reset()} resets the write pointer without releasing the
     * backing array, avoiding repeated large allocations after the first repair.</p>
     */
    private static final ThreadLocal<RecyclableOutputStream> THREAD_WRITE_BUF =
            ThreadLocal.withInitial(() -> new RecyclableOutputStream(BUFFER_SIZE));

    // ── State ─────────────────────────────────────────────────────────────────────────────────────

    private final int scanThreads;
    private final int repairThreads;

    /**
     * Construct a new engine with the given parallelism settings.
     *
     * @param scanThreads   threads for the parallel validity scan
     * @param repairThreads threads for the parallel repair phase
     */
    public ZipRepairEngine(final int scanThreads, final int repairThreads) {
        this.scanThreads = scanThreads;
        this.repairThreads = repairThreads;
    }

    // ── Public entry point ────────────────────────────────────────────────────────────────────────

    /**
     * Scan all zip files under {@code dir} for corrupt CENs and repair them in parallel.
     *
     * <p>Prints progress and a summary banner to {@link System#out}.</p>
     *
     * @param dir the directory to scan (searched recursively)
     * @return {@code 0} if all corrupt zips were repaired, {@code 1} if any were unrecoverable
     */
    public int runScanAndRepair(final Path dir) {
        PrettyPrint.printBanner("ZIP REPAIR — PHASE 1: CEN REPAIR");
        System.out.println(Ansi.AUTO.string("@|yellow Directory:|@      " + dir.toAbsolutePath()));
        System.out.println(Ansi.AUTO.string("@|yellow Scan threads:|@   " + scanThreads));
        System.out.println(Ansi.AUTO.string("@|yellow Repair threads:|@ " + repairThreads));
        System.out.println(Ansi.AUTO.string(
                "@|yellow Buffer size:|@    " + (BUFFER_SIZE / (1024 * 1024)) + " MiB per thread (in + out)"));
        System.out.println();

        System.out.print("Collecting zip file paths … ");
        System.out.flush();
        final List<Path> allZips = collectZipFiles(dir);
        System.out.println(allZips.size() + " found.");
        System.out.println();

        if (allZips.isEmpty()) {
            System.out.println(Ansi.AUTO.string("@|yellow No zip files found.|@"));
            return 0;
        }

        System.out.println("Scanning " + allZips.size() + " zip files …");
        final long scanStart = System.nanoTime();
        final List<Path> corruptZips = findCorruptZipsParallel(allZips);
        final long scanSecs = (System.nanoTime() - scanStart) / 1_000_000_000L;
        System.out.printf(
                "  Done in %ds — %d corrupt file(s) out of %d.%n%n", scanSecs, corruptZips.size(), allZips.size());

        if (corruptZips.isEmpty()) {
            PrettyPrint.printBanner("PHASE 1 RESULT");
            System.out.println(Ansi.AUTO.string(
                    "@|bold,green All " + allZips.size() + " zip files have valid CENs. No repairs needed.|@"));
            return 0;
        }

        System.out.println(Ansi.AUTO.string("@|yellow Corrupt zip files:|@"));
        for (final Path corrupt : corruptZips) {
            System.out.println(Ansi.AUTO.string("  @|red •|@ " + corrupt));
        }
        System.out.println();

        final Map<Path, RepairResult> resultMap = repairAllParallel(corruptZips);

        // Print per-file results in original (sorted) order
        System.out.println();
        int repaired = 0;
        int unrecoverable = 0;
        for (final Path zipPath : corruptZips) {
            final RepairResult result = resultMap.get(zipPath);
            if (result == null) {
                continue;
            }
            switch (result.status()) {
                case REPAIRED -> {
                    repaired++;
                    System.out.println(Ansi.AUTO.string("  @|green OK|@      " + zipPath + "  ("
                            + result.entriesRecovered() + " blocks recovered)"));
                }
                case PARTIAL -> {
                    repaired++;
                    System.out.println(Ansi.AUTO.string("  @|yellow PARTIAL|@ " + zipPath + "  ("
                            + result.entriesRecovered() + " blocks recovered; last entry was truncated: "
                            + result.detail() + ")"));
                }
                case UNRECOVERABLE -> {
                    unrecoverable++;
                    System.out.println(Ansi.AUTO.string("  @|red FAILED|@  " + zipPath + "  — " + result.detail()));
                }
            }
        }

        System.out.println();
        PrettyPrint.printBanner("PHASE 1 SUMMARY");
        System.out.println(Ansi.AUTO.string("@|yellow Zip files checked:|@   " + allZips.size()));
        System.out.println(Ansi.AUTO.string("@|yellow Corrupt found:|@        " + corruptZips.size()));
        System.out.println(Ansi.AUTO.string("@|yellow Successfully repaired:|@ " + repaired));
        System.out.println(Ansi.AUTO.string("@|yellow Unrecoverable:|@         " + unrecoverable));
        System.out.println();

        if (unrecoverable == 0) {
            System.out.println(Ansi.AUTO.string("@|bold,green All corrupt zip files have been repaired.|@"));
            return 0;
        } else {
            System.out.println(Ansi.AUTO.string("@|bold,red " + unrecoverable
                    + " zip file(s) could not be recovered.|@ "
                    + "The blocks they contained will be missing from validation."));
            return 1;
        }
    }

    // ── Scan helpers ──────────────────────────────────────────────────────────────────────────────

    /**
     * Walk {@code dir} recursively and return all {@code .zip} file paths, sorted.
     *
     * @param dir root directory
     * @return sorted list of zip paths
     */
    private static List<Path> collectZipFiles(final Path dir) {
        final List<Path> result = new ArrayList<>();
        try {
            Files.walk(dir)
                    .filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().endsWith(".zip"))
                    .sorted()
                    .forEach(result::add);
        } catch (IOException e) {
            System.err.println("Error scanning directory: " + e.getMessage());
        }
        return result;
    }

    /**
     * Scan all zips in parallel using a fixed thread pool.
     *
     * <p>Each file check is a fast two-seek operation reading only ~26 bytes.</p>
     *
     * @param allZips list of all zip paths to check
     * @return sorted list of paths that failed the validity check
     */
    private List<Path> findCorruptZipsParallel(final List<Path> allZips) {
        final int total = allZips.size();
        final AtomicInteger checked = new AtomicInteger(0);
        final List<Path> corrupt = Collections.synchronizedList(new ArrayList<>());
        final long startNanos = System.nanoTime();

        try (ExecutorService pool = Executors.newFixedThreadPool(scanThreads)) {
            final List<Future<?>> futures = new ArrayList<>(total);
            for (final Path zip : allZips) {
                futures.add(pool.submit(() -> {
                    if (!isZipValid(zip)) {
                        corrupt.add(zip);
                    }
                    final int done = checked.incrementAndGet();
                    if (done % 1000 == 0 || done == total) {
                        final long elapsedSec = (System.nanoTime() - startNanos) / 1_000_000_000L;
                        System.out.printf(
                                "  Checked %,d / %,d  (%d corrupt so far)  [%ds]%n",
                                done, total, corrupt.size(), elapsedSec);
                    }
                }));
            }
            for (final Future<?> f : futures) {
                try {
                    f.get();
                } catch (Exception e) {
                    System.err.println("Scan task error: " + e.getMessage());
                }
            }
        }
        corrupt.sort(Path::compareTo);
        return corrupt;
    }

    /**
     * Fast zip validity check using two file-channel seeks, reading only ~26 bytes total.
     *
     * <p>A valid zip has an EOCD record at {@code fileSize - 22} with the correct signature, and
     * the first CEN entry at the declared offset also carries the correct signature. This catches
     * both a missing EOCD and an EOCD with wrong CEN offset (APPEND-mode bug).</p>
     *
     * @param zipPath path to the zip file to check
     * @return {@code true} if the zip appears valid
     */
    private static boolean isZipValid(final Path zipPath) {
        try {
            final long fileSize = Files.size(zipPath);
            if (fileSize < EOCD_SIZE) {
                return false;
            }
            try (SeekableByteChannel ch = Files.newByteChannel(zipPath)) {
                final ByteBuffer eocd = ByteBuffer.allocate(EOCD_SIZE).order(ByteOrder.LITTLE_ENDIAN);
                ch.position(fileSize - EOCD_SIZE);
                if (ch.read(eocd) < EOCD_SIZE) {
                    return false;
                }
                eocd.flip();
                if (eocd.getInt(0) != EOCD_SIGNATURE) {
                    return false;
                }
                final long cenOffset = Integer.toUnsignedLong(eocd.getInt(EOCD_CEN_OFFSET_POS));
                if (cenOffset >= fileSize) {
                    return false;
                }
                final ByteBuffer cenSig = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
                ch.position(cenOffset);
                if (ch.read(cenSig) < 4) {
                    return false;
                }
                cenSig.flip();
                return cenSig.getInt(0) == CEN_SIGNATURE;
            }
        } catch (IOException e) {
            return false;
        }
    }

    // ── Repair helpers ────────────────────────────────────────────────────────────────────────────

    /**
     * Repair all corrupt zips in parallel, collecting results into a map keyed by zip path.
     *
     * @param corruptZips list of corrupt zip paths (will be processed in parallel, arbitrary order)
     * @return map from zip path to its {@link RepairResult}
     */
    private Map<Path, RepairResult> repairAllParallel(final List<Path> corruptZips) {
        final Map<Path, RepairResult> resultMap = new ConcurrentHashMap<>(corruptZips.size());
        final AtomicInteger unrecoverableCount = new AtomicInteger(0);
        final AtomicInteger doneCount = new AtomicInteger(0);
        final int reportEvery = Math.max(1, corruptZips.size() / 20);

        System.out.println("Repairing " + corruptZips.size() + " file(s) using " + repairThreads + " thread(s) …");
        final long repairStart = System.nanoTime();

        try (ExecutorService pool = Executors.newFixedThreadPool(repairThreads)) {
            final List<Future<?>> futures = new ArrayList<>(corruptZips.size());
            for (final Path zipPath : corruptZips) {
                futures.add(pool.submit(() -> {
                    final RepairResult result = repairZip(zipPath);
                    resultMap.put(zipPath, result);
                    if (result.status() == RepairStatus.UNRECOVERABLE) {
                        unrecoverableCount.incrementAndGet();
                    }
                    final int done = doneCount.incrementAndGet();
                    if (done % reportEvery == 0 || done == corruptZips.size()) {
                        final long elapsedSec = (System.nanoTime() - repairStart) / 1_000_000_000L;
                        System.out.printf(
                                "  Repaired %,d / %,d  (%d failed so far)  [%ds]%n",
                                done, corruptZips.size(), unrecoverableCount.get(), elapsedSec);
                    }
                }));
            }
            for (final Future<?> f : futures) {
                try {
                    f.get();
                } catch (Exception e) {
                    System.err.println("Repair task error: " + e.getMessage());
                }
            }
        }
        return resultMap;
    }

    /**
     * Repair a corrupt zip, using an in-memory strategy when the file fits within
     * {@link #BUFFER_SIZE}, or falling back to streaming for larger files.
     *
     * <p><b>In-memory path</b> (file ≤ {@code BUFFER_SIZE}): reads the entire zip into the
     * calling thread's {@link #THREAD_READ_BUF}, processes entries via {@link ZipInputStream}
     * over a {@link ByteArrayInputStream}, and accumulates the repaired zip into the calling
     * thread's {@link #THREAD_WRITE_BUF}. Both buffers are reused across repairs on the same
     * thread, so no large allocation occurs after the initial warm-up.</p>
     *
     * <p><b>Streaming fallback</b> (file > {@code BUFFER_SIZE}): delegates to
     * {@link #repairZipStreaming}.</p>
     *
     * @param zipPath path to the corrupt zip file
     * @return result describing outcome
     */
    private static RepairResult repairZip(final Path zipPath) {
        final Path tempFile = zipPath.resolveSibling(zipPath.getFileName() + ".repairtmp");

        final long fileSize;
        try {
            fileSize = Files.size(zipPath);
        } catch (IOException e) {
            return RepairResult.unrecoverable("Cannot read file size: " + e.getMessage());
        }

        if (fileSize > BUFFER_SIZE) {
            return repairZipStreaming(zipPath, tempFile);
        }

        // ── In-memory path ──────────────────────────────────────────────────────────────────────

        final int readSize = (int) fileSize;
        final byte[] readBuf = THREAD_READ_BUF.get();
        try (FileChannel readCh = FileChannel.open(zipPath, StandardOpenOption.READ)) {
            final ByteBuffer bb = ByteBuffer.wrap(readBuf, 0, readSize);
            while (bb.hasRemaining()) {
                if (readCh.read(bb) < 0) {
                    break;
                }
            }
        } catch (IOException e) {
            return RepairResult.unrecoverable("Failed to read file into buffer: " + e.getMessage());
        }

        final RecyclableOutputStream writeBuf = THREAD_WRITE_BUF.get();
        writeBuf.reset();

        int entriesRecovered = 0;
        String truncationDetail = null;
        boolean streamOpenedOk = false;
        try (ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(readBuf, 0, readSize));
                ZipOutputStream zos = new ZipOutputStream(writeBuf)) {
            streamOpenedOk = true;
            zos.setMethod(ZipOutputStream.STORED);
            zos.setLevel(Deflater.NO_COMPRESSION);
            ZipEntry inEntry;
            while ((inEntry = zis.getNextEntry()) != null) {
                final String name = inEntry.getName();
                try {
                    writeEntry(zis, zos, inEntry);
                    entriesRecovered++;
                } catch (IOException e) {
                    truncationDetail = "entry '" + name + "' truncated: " + e.getMessage();
                    break;
                }
                zis.closeEntry();
            }
        } catch (IOException e) {
            if (!streamOpenedOk || entriesRecovered == 0) {
                return RepairResult.unrecoverable(
                        "ZipInputStream failed before reading any entries: " + e.getMessage());
            }
            truncationDetail = "stream error after " + entriesRecovered + " entries: " + e.getMessage();
        }

        if (entriesRecovered == 0) {
            return RepairResult.unrecoverable("No recoverable entries found in zip");
        }

        try (FileChannel outCh = FileChannel.open(
                tempFile, StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            writeBuf.writeTo(outCh);
        } catch (IOException e) {
            cleanupTemp(tempFile);
            return RepairResult.unrecoverable("Failed to write repaired file: " + e.getMessage());
        }
        try {
            Files.move(tempFile, zipPath, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            cleanupTemp(tempFile);
            return RepairResult.unrecoverable("Failed to replace original with repaired file: " + e.getMessage());
        }

        return truncationDetail != null
                ? RepairResult.partial(entriesRecovered, truncationDetail)
                : RepairResult.repaired(entriesRecovered);
    }

    /**
     * Streaming fallback for files larger than {@link #BUFFER_SIZE}.
     *
     * <p>Uses a {@link java.io.BufferedInputStream}/{@link java.io.BufferedOutputStream} pair so
     * that I/O still happens in large chunks without holding the whole file in memory. Avoids OOM
     * for rare very large (e.g. 15 GiB) zip files.</p>
     *
     * @param zipPath  path to the corrupt zip file
     * @param tempFile sibling temp file path already computed by the caller
     * @return result describing outcome
     */
    private static RepairResult repairZipStreaming(final Path zipPath, final Path tempFile) {
        int entriesRecovered = 0;
        String truncationDetail = null;
        boolean streamOpenedOk = false;

        try (ZipInputStream zis = new ZipInputStream(
                        new java.io.BufferedInputStream(Files.newInputStream(zipPath), BUFFER_SIZE));
                ZipOutputStream zos = new ZipOutputStream(
                        new java.io.BufferedOutputStream(Files.newOutputStream(tempFile), BUFFER_SIZE))) {
            streamOpenedOk = true;
            zos.setMethod(ZipOutputStream.STORED);
            zos.setLevel(Deflater.NO_COMPRESSION);
            ZipEntry inEntry;
            while ((inEntry = zis.getNextEntry()) != null) {
                final String name = inEntry.getName();
                try {
                    writeEntry(zis, zos, inEntry);
                    entriesRecovered++;
                } catch (IOException e) {
                    truncationDetail = "entry '" + name + "' truncated: " + e.getMessage();
                    break;
                }
                zis.closeEntry();
            }
        } catch (IOException e) {
            cleanupTemp(tempFile);
            if (!streamOpenedOk || entriesRecovered == 0) {
                return RepairResult.unrecoverable(
                        "ZipInputStream failed before reading any entries: " + e.getMessage());
            }
            truncationDetail = "stream error after " + entriesRecovered + " entries: " + e.getMessage();
        }

        if (entriesRecovered == 0) {
            cleanupTemp(tempFile);
            return RepairResult.unrecoverable("No recoverable entries found in zip");
        }

        try {
            Files.move(tempFile, zipPath, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            cleanupTemp(tempFile);
            return RepairResult.unrecoverable("Failed to replace original with repaired file: " + e.getMessage());
        }

        return truncationDetail != null
                ? RepairResult.partial(entriesRecovered, truncationDetail)
                : RepairResult.repaired(entriesRecovered);
    }

    /**
     * Write one entry from {@code zis} to {@code zos}.
     *
     * <p>For STORED entries with size and CRC already set in the local header, uses streaming
     * transfer. For entries without pre-set metadata the bytes are buffered to compute the CRC.</p>
     *
     * @param zis     source stream, positioned at the start of the entry body
     * @param zos     destination stream
     * @param inEntry metadata from the source entry's local file header
     * @throws IOException if reading or writing fails
     */
    static void writeEntry(final ZipInputStream zis, final ZipOutputStream zos, final ZipEntry inEntry)
            throws IOException {
        if (inEntry.getSize() >= 0 && inEntry.getCrc() != -1) {
            final ZipEntry outEntry = new ZipEntry(inEntry.getName());
            outEntry.setSize(inEntry.getSize());
            outEntry.setCompressedSize(inEntry.getCompressedSize());
            outEntry.setCrc(inEntry.getCrc());
            zos.putNextEntry(outEntry);
            zis.transferTo(zos);
            zos.closeEntry();
        } else {
            final byte[] bytes = zis.readAllBytes();
            final CRC32 crc = new CRC32();
            crc.update(bytes);
            final ZipEntry outEntry = new ZipEntry(inEntry.getName());
            outEntry.setSize(bytes.length);
            outEntry.setCompressedSize(bytes.length);
            outEntry.setCrc(crc.getValue());
            zos.putNextEntry(outEntry);
            zos.write(bytes);
            zos.closeEntry();
        }
    }

    /** Delete {@code tempFile} if it exists, swallowing any {@link IOException}. */
    static void cleanupTemp(final Path tempFile) {
        try {
            Files.deleteIfExists(tempFile);
        } catch (IOException ignored) {
        }
    }
}
