// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.repair;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;
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
 *   <li><b>In-memory repair</b> (files ≤ {@link #bufferSize}): reads the entire zip into a
 *       per-thread {@link ThreadLocal} buffer, processes entries via {@link ZipInputStream}, and
 *       writes back via a single {@link FileChannel} call — avoiding GC pressure. Buffer size is
 *       auto-computed from available heap and thread count, or set explicitly via
 *       {@link #ZipRepairEngine(int, int, int)}.</li>
 *   <li><b>Streaming repair</b> (files &gt; {@link #bufferSize}): scans Local File Headers (LFH)
 *       directly using positional {@link FileChannel} reads, then copies each entry verbatim using
 *       {@link FileChannel#transferTo} (zero-copy on Linux via {@code sendfile} / {@code splice}).
 *       This eliminates both the {@link ZipOutputStream} CRC recomputation and the
 *       {@link ZipInputStream} / {@link java.io.BufferedInputStream} overhead visible in CPU
 *       profiles of large-file repairs. ZIP64 (files / entries &gt; 4 GiB) is fully supported.</li>
 * </ul>
 */
public class ZipRepairEngine {

    // ── Zip-format constants ──────────────────────────────────────────────────────────────────────

    /** EOCD (End of Central Directory) signature, little-endian. */
    private static final int EOCD_SIGNATURE = 0x06054b50;

    /** CEN (Central Directory Entry) signature, little-endian. */
    private static final int CEN_SIGNATURE = 0x02014b50;

    /** LFH (Local File Header) signature, little-endian. */
    private static final int LFH_SIGNATURE = 0x04034b50;

    /** ZIP64 extended-info extra-field tag. */
    private static final int ZIP64_EXTRA_TAG = 0x0001;

    /** Minimum EOCD record size in bytes (no zip comment). */
    private static final int EOCD_SIZE = 22;

    /** Byte offset of the CEN offset field within the EOCD record. */
    private static final int EOCD_CEN_OFFSET_POS = 16;

    // ── Buffer tuning ─────────────────────────────────────────────────────────────────────────────

    /** Minimum wall-clock interval between progress-bar updates (500 ms in nanoseconds). */
    private static final long PROGRESS_INTERVAL_NS = 500_000_000L;

    /** Minimum per-thread buffer size (64 MiB). */
    private static final int MIN_BUFFER_SIZE = 64 * 1024 * 1024;

    /**
     * {@code true} when the system {@code zip} command is present and responds to
     * {@code zip --version} with exit code 0. Checked once at class load.
     *
     * <p>When available, the streaming repair path delegates to
     * {@code zip -FF <input> --out <temp>} which is typically 2–3× faster than the pure-Java
     * {@link FileChannel#transferTo} path (no JVM overhead; native I/O throughput).</p>
     */
    static final boolean ZIP_FF_AVAILABLE = checkZipFfAvailable();

    private static boolean checkZipFfAvailable() {
        try {
            final Process p = new ProcessBuilder("zip", "--version")
                    .redirectErrorStream(true)
                    .start();
            p.getInputStream().transferTo(OutputStream.nullOutputStream());
            return p.waitFor() == 0;
        } catch (Exception e) {
            return false;
        }
    }

    // ── Inner types ───────────────────────────────────────────────────────────────────────────────

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
     * Metadata captured while scanning a Local File Header during the streaming repair path.
     * Used to build the new Central Directory without re-reading entry headers.
     */
    private record RawCenEntry(
            byte[] nameBytes,
            short flags,
            short method,
            int modTime,
            int modDate,
            long crc32,
            long compSize,
            long uncompSize,
            long lfhOffset) {}

    // ── State ─────────────────────────────────────────────────────────────────────────────────────

    private final int scanThreads;
    private final int repairThreads;

    /**
     * Per-thread I/O buffer size in bytes.
     *
     * <p>Files whose on-disk size is ≤ this value are processed entirely in RAM (in-memory path).
     * Larger files fall through to the streaming LFH-scan path. Buffer size is either provided
     * explicitly or auto-computed from available heap by {@link #computeBufferSize(int)}.</p>
     */
    final int bufferSize;

    /**
     * Per-thread read buffer, lazily allocated to {@link #bufferSize} bytes.
     *
     * <p>Reused across repair tasks on the same thread — no GC pressure after warm-up.</p>
     */
    private final ThreadLocal<byte[]> threadReadBuf;

    /**
     * Per-thread write buffer, lazily allocated with initial capacity {@link #bufferSize}.
     *
     * <p>{@link RecyclableOutputStream#reset()} resets the write pointer without releasing the
     * backing array, avoiding repeated large allocations after the first repair.</p>
     */
    private final ThreadLocal<RecyclableOutputStream> threadWriteBuf;

    /**
     * Construct a new engine, auto-computing the per-thread buffer size from available heap.
     *
     * @param scanThreads   threads for the parallel validity scan
     * @param repairThreads threads for the parallel repair phase
     */
    public ZipRepairEngine(final int scanThreads, final int repairThreads) {
        this(scanThreads, repairThreads, 0);
    }

    /**
     * Construct a new engine with an explicit buffer size.
     *
     * @param scanThreads       threads for the parallel validity scan
     * @param repairThreads     threads for the parallel repair phase
     * @param explicitBufferMiB per-thread buffer size in MiB, or {@code 0} to auto-compute
     */
    public ZipRepairEngine(final int scanThreads, final int repairThreads, final int explicitBufferMiB) {
        this.scanThreads = scanThreads;
        this.repairThreads = repairThreads;
        this.bufferSize = explicitBufferMiB > 0 ? explicitBufferMiB * 1024 * 1024 : computeBufferSize(repairThreads);
        this.threadReadBuf = ThreadLocal.withInitial(() -> new byte[this.bufferSize]);
        this.threadWriteBuf = ThreadLocal.withInitial(() -> new RecyclableOutputStream(this.bufferSize));
    }

    /**
     * Estimate the optimal per-thread buffer size from the JVM's max heap.
     *
     * <p>Each repair thread needs up to 2 × bufferSize (one read buffer + one write buffer).
     * We reserve 25 % of max heap for JVM overhead, GC, and other live objects.</p>
     *
     * @param repairThreads number of parallel repair threads
     * @return buffer size in bytes, clamped to [{@link #MIN_BUFFER_SIZE}, {@code Integer.MAX_VALUE - 8}]
     */
    static int computeBufferSize(final int repairThreads) {
        final long maxHeap = Runtime.getRuntime().maxMemory();
        final long perThread = (long) (maxHeap * 0.75) / Math.max(1L, (long) repairThreads * 2);
        return (int) Math.max(MIN_BUFFER_SIZE, Math.min(perThread, (long) Integer.MAX_VALUE - 8));
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
        System.out.println(Ansi.AUTO.string("@|yellow Buffer size:|@    " + (bufferSize / (1024 * 1024))
                + " MiB per thread" + " (files ≤ this are repaired in RAM; larger files use streaming)"));
        System.out.println(Ansi.AUTO.string("@|yellow Streaming mode:|@ "
                + (ZIP_FF_AVAILABLE
                        ? "@|green zip -FF|@ (native; ~2-3× faster than Java fallback)"
                        : "@|yellow Java LFH scan|@ (install zip for faster repairs)")));
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
        final AtomicLong lastPrintNanos = new AtomicLong(startNanos);

        try (ExecutorService pool = Executors.newFixedThreadPool(scanThreads)) {
            final List<Future<?>> futures = new ArrayList<>(total);
            for (final Path zip : allZips) {
                futures.add(pool.submit(() -> {
                    if (!isZipValid(zip)) {
                        corrupt.add(zip);
                    }
                    final int done = checked.incrementAndGet();
                    final long now = System.nanoTime();
                    final long last = lastPrintNanos.get();
                    if (done == total
                            || (now - last >= PROGRESS_INTERVAL_NS && lastPrintNanos.compareAndSet(last, now))) {
                        final long elapsedMillis = (now - startNanos) / 1_000_000L;
                        final double percent = (double) done / total * 100.0;
                        final long remaining = PrettyPrint.computeRemainingMilliseconds(done, total, elapsedMillis);
                        PrettyPrint.printProgressWithEta(
                                percent,
                                String.format("Scanning  %,d / %,d  (%,d corrupt)", done, total, corrupt.size()),
                                remaining);
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
        PrettyPrint.clearProgress();
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
     * <p>Progress is reported in bytes rather than file count so the bar advances smoothly even
     * when individual zips are large. The background progress thread redraws the bar every
     * {@link #PROGRESS_INTERVAL_NS} regardless of how long individual repair tasks take.</p>
     *
     * @param corruptZips list of corrupt zip paths (will be processed in parallel, arbitrary order)
     * @return map from zip path to its {@link RepairResult}
     */
    private Map<Path, RepairResult> repairAllParallel(final List<Path> corruptZips) {
        final Map<Path, RepairResult> resultMap = new ConcurrentHashMap<>(corruptZips.size());
        final AtomicInteger unrecoverableCount = new AtomicInteger(0);
        final AtomicInteger doneCount = new AtomicInteger(0);
        final AtomicLong bytesProcessed = new AtomicLong(0);
        final int total = corruptZips.size();

        // Pre-compute per-file sizes so we can show byte-based progress.
        long totalBytesSum = 0;
        final long[] fileSizes = new long[total];
        for (int i = 0; i < total; i++) {
            try {
                fileSizes[i] = Files.size(corruptZips.get(i));
            } catch (IOException e) {
                fileSizes[i] = 0;
            }
            totalBytesSum += fileSizes[i];
        }
        final long totalBytes = totalBytesSum;

        System.out.printf(
                "Repairing %,d file(s) (%,.1f GiB) using %d thread(s) …%n",
                total, totalBytes / (1024.0 * 1024.0 * 1024.0), repairThreads);
        final long repairStart = System.nanoTime();

        // Dedicated progress thread: redraws the bar every PROGRESS_INTERVAL_NS regardless of how
        // long individual repair tasks take, so the bar stays alive even when tasks are slow.
        final Thread progressThread = Thread.ofVirtual().start(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                printRepairProgress(
                        doneCount.get(),
                        total,
                        unrecoverableCount.get(),
                        bytesProcessed.get(),
                        totalBytes,
                        repairStart);
                try {
                    Thread.sleep(PROGRESS_INTERVAL_NS / 1_000_000L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });

        try (ExecutorService pool = Executors.newFixedThreadPool(repairThreads)) {
            final List<Future<?>> futures = new ArrayList<>(total);
            for (int i = 0; i < total; i++) {
                final Path zipPath = corruptZips.get(i);
                futures.add(pool.submit(() -> {
                    final RepairResult result = repairZip(zipPath, bytesProcessed::addAndGet);
                    resultMap.put(zipPath, result);
                    if (result.status() == RepairStatus.UNRECOVERABLE) {
                        unrecoverableCount.incrementAndGet();
                    }
                    doneCount.incrementAndGet();
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

        progressThread.interrupt();
        try {
            progressThread.join(1_000);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
        // Final 100 % bar before clearing so the user sees completion before results print.
        printRepairProgress(total, total, unrecoverableCount.get(), totalBytes, totalBytes, repairStart);
        PrettyPrint.clearProgress();
        return resultMap;
    }

    /**
     * Print one repair progress bar update, showing byte-based percentage and throughput.
     *
     * @param done             files repaired so far
     * @param total            total files to repair
     * @param unrecoverable    files that could not be repaired
     * @param bytesProcessed   total bytes of completed repairs so far
     * @param totalBytes       total bytes of all files to repair
     * @param repairStartNanos start time from {@link System#nanoTime()}
     */
    private static void printRepairProgress(
            final int done,
            final int total,
            final int unrecoverable,
            final long bytesProcessed,
            final long totalBytes,
            final long repairStartNanos) {
        final long elapsedMillis = (System.nanoTime() - repairStartNanos) / 1_000_000L;
        final double percent = totalBytes == 0 ? 100.0 : (double) bytesProcessed / totalBytes * 100.0;
        final long remaining = PrettyPrint.computeRemainingMilliseconds(bytesProcessed, totalBytes, elapsedMillis);
        final double mibProcessed = bytesProcessed / (1024.0 * 1024.0);
        final double mibTotal = totalBytes / (1024.0 * 1024.0);
        final double mibPerSec = elapsedMillis > 0 ? mibProcessed / (elapsedMillis / 1000.0) : 0.0;
        PrettyPrint.printProgressWithEta(
                percent,
                String.format(
                        "Repairing %,d / %,d files (%,d failed)  %.0f / %.0f MiB  @ %.0f MiB/s",
                        done, total, unrecoverable, mibProcessed, mibTotal, mibPerSec),
                remaining);
    }

    /**
     * Repair a corrupt zip, using an in-memory strategy when the file fits within
     * {@link #bufferSize}, or falling back to the zero-copy streaming path for larger files.
     *
     * <p><b>In-memory path</b> (file ≤ {@code bufferSize}): reads the entire zip into the
     * calling thread's {@link #threadReadBuf}, processes entries via {@link ZipInputStream}
     * over a {@link ByteArrayInputStream}, and accumulates the repaired zip into the calling
     * thread's {@link #threadWriteBuf}. Both buffers are reused across repairs on the same
     * thread, so no large allocation occurs after the initial warm-up.</p>
     *
     * <p><b>Streaming path</b> (file &gt; {@code bufferSize}): scans LFH signatures directly
     * using {@link #repairZipStreaming} — see that method for details.</p>
     *
     * @param zipPath       path to the corrupt zip file
     * @param bytesCallback called with the number of bytes processed; may be called once at
     *                      completion (in-memory / Java streaming) or incrementally per entry
     *                      ({@code zip -FF} path) for smooth progress reporting
     * @return result describing outcome
     */
    private RepairResult repairZip(final Path zipPath, final LongConsumer bytesCallback) {
        final Path tempFile = zipPath.resolveSibling(zipPath.getFileName() + ".repairtmp");

        final long fileSize;
        try {
            fileSize = Files.size(zipPath);
        } catch (IOException e) {
            return RepairResult.unrecoverable("Cannot read file size: " + e.getMessage());
        }

        if (fileSize > bufferSize) {
            return repairZipStreaming(zipPath, tempFile, fileSize, bytesCallback);
        }

        // ── In-memory path ──────────────────────────────────────────────────────────────────────

        final int readSize = (int) fileSize;
        final byte[] readBuf = threadReadBuf.get();
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

        final RecyclableOutputStream writeBuf = threadWriteBuf.get();
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

        bytesCallback.accept(fileSize);
        return truncationDetail != null
                ? RepairResult.partial(entriesRecovered, truncationDetail)
                : RepairResult.repaired(entriesRecovered);
    }

    /**
     * Streaming repair dispatcher for files larger than {@link #bufferSize}.
     *
     * <p>When {@link #ZIP_FF_AVAILABLE} is {@code true}, delegates to
     * {@link #repairZipWithZipFf} (native {@code zip -FF} subprocess). If that fails, or
     * when the system {@code zip} command is unavailable, falls back to the pure-Java
     * {@link #repairZipStreamingJava} implementation.</p>
     *
     * @param zipPath       path to the corrupt zip file
     * @param tempFile      sibling temp file path already computed by the caller
     * @param fileSize      on-disk size of the zip, used as the callback value for the Java path
     * @param bytesCallback called with bytes processed; invoked per entry by zip -FF, or once
     *                      with {@code fileSize} by the Java fallback
     * @return result describing outcome
     */
    private RepairResult repairZipStreaming(
            final Path zipPath, final Path tempFile, final long fileSize, final LongConsumer bytesCallback) {
        if (ZIP_FF_AVAILABLE) {
            final RepairResult result = repairZipWithZipFf(zipPath, tempFile, bytesCallback);
            if (result != null) {
                return result;
            }
            // zip -FF failed (process error or no entries) — clean up and fall through.
            cleanupTemp(tempFile);
        }
        return repairZipStreamingJava(zipPath, tempFile, fileSize, bytesCallback);
    }

    /**
     * Repair using the system {@code zip -FF} command.
     *
     * <p>Parses stdout lines of the form {@code " copying: FILENAME  (NNN bytes)"} to call
     * {@code bytesCallback} incrementally, giving the progress bar sub-file granularity for
     * large zips.</p>
     *
     * @param zipPath       input corrupt zip
     * @param tempFile      output path for the repaired zip
     * @param bytesCallback called with each entry's byte count as it is copied
     * @return result on success, or {@code null} if the process failed (caller should fall back)
     */
    private RepairResult repairZipWithZipFf(final Path zipPath, final Path tempFile, final LongConsumer bytesCallback) {
        try {
            final Process process = new ProcessBuilder("zip", "-FF", zipPath.toString(), "--out", tempFile.toString())
                    .redirectErrorStream(false)
                    .start();

            // Drain stderr in background to prevent the process from blocking on a full pipe.
            final Thread stderrDrainer = Thread.ofVirtual().start(() -> {
                try {
                    process.getErrorStream().transferTo(OutputStream.nullOutputStream());
                } catch (IOException ignored) {
                }
            });

            int entriesRecovered = 0;
            try (BufferedReader stdout = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = stdout.readLine()) != null) {
                    final String trimmed = line.trim();
                    if (!trimmed.startsWith("copying:")) {
                        continue;
                    }
                    entriesRecovered++;
                    // Parse "copying: FILENAME  (NNN bytes)" for the byte count.
                    final int parenOpen = trimmed.lastIndexOf('(');
                    final int parenClose = trimmed.lastIndexOf(')');
                    if (parenOpen >= 0 && parenClose > parenOpen) {
                        // Substring is e.g. "909465 bytes"
                        final String inside = trimmed.substring(parenOpen + 1, parenClose);
                        final int space = inside.indexOf(' ');
                        if (space > 0) {
                            try {
                                bytesCallback.accept(Long.parseLong(inside.substring(0, space)));
                            } catch (NumberFormatException ignored) {
                            }
                        }
                    }
                }
            }

            stderrDrainer.join(5_000);
            final int exitCode = process.waitFor();
            if (exitCode != 0 || entriesRecovered == 0) {
                return null;
            }

            Files.move(tempFile, zipPath, StandardCopyOption.REPLACE_EXISTING);
            return RepairResult.repaired(entriesRecovered);

        } catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            return null;
        }
    }

    /**
     * Pure-Java streaming repair for files larger than {@link #bufferSize}.
     *
     * <p>Rather than routing bytes through {@link ZipInputStream} / {@link ZipOutputStream}
     * (which recomputes CRC32 for every byte written and adds heap-buffer copies), this method
     * scans the file directly at the {@code PK\x03\x04} Local File Header level using positional
     * {@link FileChannel} reads and copies each entry verbatim to the output via
     * {@link FileChannel#transferTo} — a single system call that can bypass userspace on Linux
     * ({@code sendfile} / {@code splice}). A new Central Directory and EOCD are then written from
     * the metadata collected during the scan. ZIP64 (offsets / sizes &gt; 4 GiB) is fully
     * supported.</p>
     *
     * @param zipPath       path to the corrupt zip file
     * @param tempFile      sibling temp file path already computed by the caller
     * @param fileSize      on-disk size of the zip; passed to {@code bytesCallback} on success
     * @param bytesCallback called once with {@code fileSize} when repair completes
     * @return result describing outcome
     */
    private RepairResult repairZipStreamingJava(
            final Path zipPath, final Path tempFile, final long fileSize, final LongConsumer bytesCallback) {
        final List<RawCenEntry> entries = new ArrayList<>();
        String truncationDetail = null;

        try (FileChannel inCh = FileChannel.open(zipPath, StandardOpenOption.READ);
                FileChannel outCh = FileChannel.open(
                        tempFile,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.TRUNCATE_EXISTING)) {

            final long fileSizeFromChannel = inCh.size();
            final ByteBuffer hdr = ByteBuffer.allocate(30).order(ByteOrder.LITTLE_ENDIAN);
            long inPos = 0;

            // ── Scan and copy Local File Headers ──────────────────────────────────────────────

            while (inPos + 30 <= fileSizeFromChannel) {
                // Read the 30-byte fixed part of the LFH.
                readFully(inCh, hdr, inPos);
                if (hdr.remaining() < 30) {
                    break; // I/O truncation
                }

                final int sig = hdr.getInt(0);
                if (sig == CEN_SIGNATURE || sig == EOCD_SIGNATURE) {
                    break; // reached an existing (possibly bad) CEN / EOCD — stop here
                }
                if (sig != LFH_SIGNATURE) {
                    truncationDetail = String.format(
                            "unexpected signature 0x%08X at offset %,d after %,d entries", sig, inPos, entries.size());
                    break;
                }

                final short flags = hdr.getShort(6);
                final short method = hdr.getShort(8);
                final int modTime = hdr.getShort(10) & 0xFFFF;
                final int modDate = hdr.getShort(12) & 0xFFFF;
                final long crc32 = Integer.toUnsignedLong(hdr.getInt(14));
                long compSize = Integer.toUnsignedLong(hdr.getInt(18));
                long uncompSize = Integer.toUnsignedLong(hdr.getInt(22));
                final int nameLen = hdr.getShort(26) & 0xFFFF;
                final int extraLen = hdr.getShort(28) & 0xFFFF;

                // Data-descriptor flag (bit 3): sizes come after the data — we cannot seek ahead.
                // This should not occur for STORED block files written by BlockWriter.
                if ((flags & 0x08) != 0) {
                    truncationDetail = "entry at offset " + inPos
                            + " uses data descriptor (bit 3 set); cannot repair without decompressor";
                    break;
                }

                // Read file name for error messages and CEN construction.
                final byte[] nameBytes = readBytes(inCh, inPos + 30, nameLen);

                // Parse the extra field for ZIP64 extended info when sizes are 0xFFFF_FFFF.
                if (extraLen > 0 && (compSize == 0xFFFFFFFFL || uncompSize == 0xFFFFFFFFL)) {
                    final byte[] extra = readBytes(inCh, inPos + 30 + nameLen, extraLen);
                    final ByteBuffer eb = ByteBuffer.wrap(extra).order(ByteOrder.LITTLE_ENDIAN);
                    while (eb.remaining() >= 4) {
                        final int tagId = eb.getShort() & 0xFFFF;
                        final int tagSz = eb.getShort() & 0xFFFF;
                        if (tagId == ZIP64_EXTRA_TAG) {
                            // Fields appear in order, only when the 32-bit counterpart is 0xFFFFFFFF.
                            if (uncompSize == 0xFFFFFFFFL && eb.remaining() >= 8) {
                                uncompSize = eb.getLong();
                            }
                            if (compSize == 0xFFFFFFFFL && eb.remaining() >= 8) {
                                compSize = eb.getLong();
                            }
                            break;
                        }
                        final int skip = Math.min(tagSz, eb.remaining());
                        eb.position(eb.position() + skip);
                    }
                }

                // Bounds check: does the full entry fit in the input file?
                final long entryLen = 30L + nameLen + extraLen + compSize;
                if (inPos + entryLen > fileSizeFromChannel) {
                    final String name = new String(nameBytes, StandardCharsets.UTF_8);
                    truncationDetail = "entry '" + name + "' truncated at offset " + inPos;
                    break;
                }

                // Record the offset in the OUTPUT file before copying.
                final long lfhOutOffset = outCh.position();

                // Copy the full entry (LFH + name + extra + data) verbatim.
                // transferTo may transfer less than requested on some OSes — loop until done.
                long remaining = entryLen;
                long from = inPos;
                while (remaining > 0) {
                    final long n = inCh.transferTo(from, remaining, outCh);
                    if (n <= 0) {
                        throw new IOException("transferTo stalled at input offset " + from);
                    }
                    from += n;
                    remaining -= n;
                }

                inPos += entryLen;
                entries.add(new RawCenEntry(
                        nameBytes, flags, method, modTime, modDate, crc32, compSize, uncompSize, lfhOutOffset));
            }

            if (entries.isEmpty()) {
                cleanupTemp(tempFile);
                return RepairResult.unrecoverable("No recoverable entries found");
            }

            // ── Write new Central Directory + EOCD ────────────────────────────────────────────
            writeCentralDirectory(outCh, entries);

        } catch (IOException e) {
            cleanupTemp(tempFile);
            return RepairResult.unrecoverable("I/O error during repair: " + e.getMessage());
        }

        try {
            Files.move(tempFile, zipPath, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            cleanupTemp(tempFile);
            return RepairResult.unrecoverable("Failed to replace original with repaired file: " + e.getMessage());
        }

        bytesCallback.accept(fileSize);
        return truncationDetail != null
                ? RepairResult.partial(entries.size(), truncationDetail)
                : RepairResult.repaired(entries.size());
    }

    // ── I/O helpers ───────────────────────────────────────────────────────────────────────────────

    /**
     * Fill {@code buf} from {@code ch} starting at file position {@code pos}.
     * Uses absolute positional reads so the channel's current position is not affected.
     *
     * @param ch  source channel
     * @param buf buffer to fill (cleared before reading)
     * @param pos file position of the first byte to read
     * @throws IOException if the channel read fails
     */
    private static void readFully(final FileChannel ch, final ByteBuffer buf, final long pos) throws IOException {
        buf.clear();
        long filePos = pos;
        while (buf.hasRemaining()) {
            final int n = ch.read(buf, filePos);
            if (n < 0) {
                break;
            }
            filePos += n;
        }
        buf.flip();
    }

    /**
     * Read exactly {@code len} bytes from {@code ch} starting at file position {@code pos}.
     *
     * @param ch  source channel
     * @param pos file position of the first byte to read
     * @param len number of bytes to read
     * @return byte array of length {@code len} (may be shorter if the file is truncated)
     * @throws IOException if the channel read fails
     */
    private static byte[] readBytes(final FileChannel ch, final long pos, final int len) throws IOException {
        final byte[] bytes = new byte[len];
        final ByteBuffer bb = ByteBuffer.wrap(bytes);
        long filePos = pos;
        while (bb.hasRemaining()) {
            final int n = ch.read(bb, filePos);
            if (n < 0) {
                break;
            }
            filePos += n;
        }
        return bytes;
    }

    /**
     * Write a fresh Central Directory and EOCD record to {@code outCh} at its current position,
     * using the LFH metadata collected during the streaming scan.
     *
     * <p>ZIP64 extended-info extra fields and the ZIP64 EOCD record/locator are written
     * automatically whenever an entry offset or the CEN start position exceeds 4 GiB, or the
     * entry count exceeds 65 535.</p>
     *
     * @param outCh   the output file channel, positioned just after the last copied entry
     * @param entries metadata for every recovered entry, in order
     * @throws IOException if writing fails
     */
    private static void writeCentralDirectory(final FileChannel outCh, final List<RawCenEntry> entries)
            throws IOException {
        final long cenStart = outCh.position();

        for (final RawCenEntry e : entries) {
            final boolean z64size = e.compSize() > 0xFFFFFFFFL || e.uncompSize() > 0xFFFFFFFFL;
            final boolean z64offset = e.lfhOffset() > 0xFFFFFFFFL;
            // Extra field: 4-byte tag+size header + 16 bytes for sizes + 8 bytes for offset.
            final int extraSize = (z64size || z64offset) ? (4 + (z64size ? 16 : 0) + (z64offset ? 8 : 0)) : 0;

            final ByteBuffer cen =
                    ByteBuffer.allocate(46 + e.nameBytes().length + extraSize).order(ByteOrder.LITTLE_ENDIAN);
            cen.putInt(CEN_SIGNATURE);
            cen.putShort((short) (extraSize > 0 ? 45 : 20)); // version made by (4.5 if ZIP64)
            cen.putShort((short) (extraSize > 0 ? 45 : 20)); // version needed
            cen.putShort(e.flags());
            cen.putShort(e.method());
            cen.putShort((short) e.modTime());
            cen.putShort((short) e.modDate());
            cen.putInt((int) e.crc32());
            cen.putInt(z64size ? 0xFFFFFFFF : (int) e.compSize());
            cen.putInt(z64size ? 0xFFFFFFFF : (int) e.uncompSize());
            cen.putShort((short) e.nameBytes().length);
            cen.putShort((short) extraSize);
            cen.putShort((short) 0); // comment length
            cen.putShort((short) 0); // disk number start
            cen.putShort((short) 0); // internal attributes
            cen.putInt(0); // external attributes
            cen.putInt(z64offset ? 0xFFFFFFFF : (int) e.lfhOffset());
            cen.put(e.nameBytes());
            if (extraSize > 0) {
                cen.putShort((short) ZIP64_EXTRA_TAG);
                cen.putShort((short) (extraSize - 4));
                if (z64size) {
                    cen.putLong(e.uncompSize());
                    cen.putLong(e.compSize());
                }
                if (z64offset) {
                    cen.putLong(e.lfhOffset());
                }
            }
            cen.flip();
            while (cen.hasRemaining()) {
                outCh.write(cen);
            }
        }

        final long cenSize = outCh.position() - cenStart;
        final int entryCount = entries.size();
        final boolean needZip64 = cenStart > 0xFFFFFFFFL || cenSize > 0xFFFFFFFFL || entryCount > 0xFFFF;

        if (needZip64) {
            // ZIP64 end of central directory record (56 bytes).
            final long z64eocdPos = outCh.position();
            final ByteBuffer z64eocd = ByteBuffer.allocate(56).order(ByteOrder.LITTLE_ENDIAN);
            z64eocd.putInt(0x06064b50); // ZIP64 EOCD signature
            z64eocd.putLong(44); // size of remaining record (56 - 12)
            z64eocd.putShort((short) 45); // version made by
            z64eocd.putShort((short) 45); // version needed
            z64eocd.putInt(0); // disk number
            z64eocd.putInt(0); // disk with CEN start
            z64eocd.putLong(entryCount); // entries on this disk
            z64eocd.putLong(entryCount); // total entries
            z64eocd.putLong(cenSize); // CEN size
            z64eocd.putLong(cenStart); // CEN offset
            z64eocd.flip();
            while (z64eocd.hasRemaining()) {
                outCh.write(z64eocd);
            }

            // ZIP64 end of central directory locator (20 bytes).
            final ByteBuffer z64loc = ByteBuffer.allocate(20).order(ByteOrder.LITTLE_ENDIAN);
            z64loc.putInt(0x07064b50); // ZIP64 EOCD locator signature
            z64loc.putInt(0); // disk with ZIP64 EOCD
            z64loc.putLong(z64eocdPos); // offset of ZIP64 EOCD record
            z64loc.putInt(1); // total disks
            z64loc.flip();
            while (z64loc.hasRemaining()) {
                outCh.write(z64loc);
            }
        }

        // Regular EOCD — always present; fields set to 0xFFFF / 0xFFFFFFFF when ZIP64 is active.
        final ByteBuffer eocd = ByteBuffer.allocate(22).order(ByteOrder.LITTLE_ENDIAN);
        eocd.putInt(EOCD_SIGNATURE);
        eocd.putShort((short) 0); // disk number
        eocd.putShort((short) 0); // disk with CEN start
        eocd.putShort((short) (needZip64 ? 0xFFFF : entryCount));
        eocd.putShort((short) (needZip64 ? 0xFFFF : entryCount));
        eocd.putInt(needZip64 ? 0xFFFFFFFF : (int) cenSize);
        eocd.putInt(needZip64 ? 0xFFFFFFFF : (int) cenStart);
        eocd.putShort((short) 0); // comment length
        eocd.flip();
        while (eocd.hasRemaining()) {
            outCh.write(eocd);
        }
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
