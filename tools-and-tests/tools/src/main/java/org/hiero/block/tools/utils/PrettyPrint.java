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
     * Prints a simple progress bar and an additional progressString on the next line.
     * Subsequent calls overwrite the same two lines in the console.
     *
     * @param current the current progress value
     * @param total the total value for completion
     * @param progressString an additional string to print on the line below the bar
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
            if (printedBefore) {
                // Move cursor up 2 lines to overwrite previous bar and progressString
                out.append("\033[2A");
            }
            // Clear line and print colored bar + spinner
            out.append("\033[2K").append(coloredBarWithSpinner).append("\n");
            // Clear next line and print colored progressString
            out.append("\033[2K").append(coloredProgressString);
            System.out.print(out.toString());
            System.out.flush();
            printedBefore = true;
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
