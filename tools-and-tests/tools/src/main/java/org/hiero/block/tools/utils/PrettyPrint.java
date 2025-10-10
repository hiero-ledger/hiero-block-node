// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.utils;

public class PrettyPrint {
    // track whether we've printed progress before so we know when to move the cursor up
    private static boolean printedBefore = false;
    private static final Object PRINT_LOCK = new Object();

    // ANSI color codes
    private static final String ANSI_GREEN = "\033[32m";
    private static final String ANSI_BLUE = "\033[34m";
    private static final String ANSI_RESET = "\033[0m";

    // whether to emit color (disable if NO_COLOR env set or no console)
    private static final boolean USE_COLOR = (System.getenv("NO_COLOR") == null) && (System.console() != null);

    // Spinner characters and index (rotates each print to show activity)
    private static final char[] SPINNER = {'|', '/', '-', '\\'};
    private static int spinnerIndex = 0;

    /**
     * Prints a simple progress bar to the console.
     *
     * @param current the current progress value
     * @param total the total value for completion
     */
    public static void printProgress(long current, long total) {
        printProgress(current, total, "");
    }

    /**
     * Prints a single-line progress bar with an additional message. Subsequent calls overwrite the same line.
     *
     * @param current the current progress value
     * @param total the total value for completion
     * @param progressString an additional string to print after the bar
     */
    public static void printProgress(long current, long total, String progressString) {
        int width = 50;
        double percent = (total == 0) ? 100d : (current * 100d / total);
        // clamp percent/ progress to sane bounds
        if (percent < 0d) percent = 0d;
        if (percent > 100d) percent = 100d;
        int progress = (total == 0) ? width : (int) (width * current / (double) Math.max(1L, total));
        if (progress < 0) progress = 0;
        if (progress > width) progress = width;

        StringBuilder bar = new StringBuilder("[");
        for (int i = 0; i < width; i++) {
            bar.append(i < progress ? "=" : " ");
        }
        bar.append("] ").append(String.format("%.2f", percent)).append("% ");

        String barLine = bar.toString();

        synchronized (PRINT_LOCK) {
            // advance spinner and build spinner string
            char spinnerChar = SPINNER[spinnerIndex];
            spinnerIndex = (spinnerIndex + 1) % SPINNER.length;
            String spinner = " " + spinnerChar;

            // apply colors if enabled (spinner colored same as bar)
            final String coloredBarWithSpinner =
                    USE_COLOR ? (ANSI_GREEN + barLine + spinner + ANSI_RESET) : (barLine + spinner);
            final String coloredProgressString = USE_COLOR ? (ANSI_BLUE + progressString + ANSI_RESET) : progressString;

            StringBuilder out = new StringBuilder();
            if (!printedBefore) {
                // ensure we start on a fresh new line so we don't corrupt previous output
                out.append("\n");
            }
            // Clear current line and carriage return then print bar + message
            out.append("\033[2K\r");
            out.append(coloredBarWithSpinner).append(" ").append(coloredProgressString);

            System.out.print(out.toString());
            System.out.flush();
            printedBefore = true;
        }
    }

    /**
     * New: print a percentage with two decimals and an ETA. Percent is 0.0-100.0.
     * @param percent the percent complete (0.0 - 100.0)
     * @param progressString additional message to print
     * @param remainingMillis estimated remaining milliseconds (may be negative or Long.MAX_VALUE if unknown)
     */
    public static void printProgressWithEta(double percent, String progressString, long remainingMillis) {
        int width = 50;
        double clamped = percent;
        if (clamped < 0d) clamped = 0d;
        if (clamped > 100d) clamped = 100d;
        int progress = (int) (width * clamped / 100.0);
        if (progress < 0) progress = 0;
        if (progress > width) progress = width;

        StringBuilder bar = new StringBuilder("[");
        for (int i = 0; i < width; i++) {
            bar.append(i < progress ? "=" : " ");
        }
        bar.append("] ").append(String.format("%.2f", clamped)).append("% ");

        String etaStr = "ETA: ?";
        if (remainingMillis >= 0 && remainingMillis < Long.MAX_VALUE) {
            etaStr = "ETA: " + formatMillis(remainingMillis);
        }

        String barLine = bar.toString();

        synchronized (PRINT_LOCK) {
            char spinnerChar = SPINNER[spinnerIndex];
            spinnerIndex = (spinnerIndex + 1) % SPINNER.length;
            String spinner = " " + spinnerChar;

            final String coloredBarWithSpinner =
                    USE_COLOR ? (ANSI_GREEN + barLine + spinner + ANSI_RESET) : (barLine + spinner);
            final String coloredProgressString = USE_COLOR
                    ? (ANSI_BLUE + progressString + " " + etaStr + ANSI_RESET)
                    : (progressString + " " + etaStr);

            StringBuilder out = new StringBuilder();
            if (!printedBefore) {
                out.append("\n");
            }
            out.append("\033[2K\r");
            out.append(coloredBarWithSpinner).append(" ").append(coloredProgressString);

            System.out.print(out.toString());
            System.out.flush();
            printedBefore = true;
        }
    }

    private static String formatMillis(long millis) {
        if (millis <= 0) return "0s";
        long seconds = millis / 1000;
        long days = seconds / 86400;
        seconds %= 86400;
        long hours = seconds / 3600;
        seconds %= 3600;
        long minutes = seconds / 60;
        seconds %= 60;
        StringBuilder sb = new StringBuilder();
        if (days > 0) sb.append(days).append("d");
        if (hours > 0) {
            if (sb.length() > 0) sb.append(" ");
            sb.append(hours).append("h");
        }
        if (minutes > 0) {
            if (sb.length() > 0) sb.append(" ");
            sb.append(minutes).append("m");
        }
        if (seconds > 0) {
            if (sb.length() > 0) sb.append(" ");
            sb.append(seconds).append("s");
        }
        return sb.toString();
    }

    /**
     * If a progress line is active, clear it and move the cursor to the next line so subsequent output prints cleanly.
     */
    public static void clearProgress() {
        synchronized (PRINT_LOCK) {
            if (!printedBefore) return;
            // clear current line and move to the next line
            System.out.print("\033[2K\r\n");
            System.out.flush();
            printedBefore = false;
        }
    }

    /**
     * Converts a file size in bytes to a human-readable string with appropriate units (B, KB, MB, GB, TB).
     *
     * @param sizeInBytes the file size in bytes
     * @return a human-readable string representation of the file size
     */
    public static String prettyPrintFileSize(long sizeInBytes) {
        if (sizeInBytes < 1024) {
            return sizeInBytes + " B";
        }
        int exp = (int) (Math.log(sizeInBytes) / Math.log(1024));
        char unit = "KMGTPE".charAt(exp - 1);
        return String.format("%.1f %sB", sizeInBytes / Math.pow(1024, exp), unit);
    }
}
