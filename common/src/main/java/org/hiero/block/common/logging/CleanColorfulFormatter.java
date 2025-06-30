// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.common.logging;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Objects;
import java.util.logging.ConsoleHandler;
import java.util.logging.Formatter;
import java.util.logging.LogManager;
import java.util.logging.LogRecord;

/**
 * A custom {@link Formatter} that adds color to log messages based on their severity level.
 */
@SuppressWarnings("unused")
public class CleanColorfulFormatter extends Formatter {
    private static final boolean colorfulLogFormatterEnabled = LogManager.getLogManager()
            .getProperty("java.util.logging.ConsoleHandler.formatter")
            .contains(CleanColorfulFormatter.class.getName());

    public static final String RESET = CleanColorfulFormatter.colorfulLogFormatterEnabled ? "\u001B[0m" : "";
    public static final String RED = CleanColorfulFormatter.colorfulLogFormatterEnabled ? "\u001B[31m" : "";
    public static final String GREEN = CleanColorfulFormatter.colorfulLogFormatterEnabled ? "\u001B[32m" : "";
    public static final String LIGHT_GREEN = CleanColorfulFormatter.colorfulLogFormatterEnabled ? "\u001B[92m" : "";
    public static final String YELLOW = CleanColorfulFormatter.colorfulLogFormatterEnabled ? "\u001B[33m" : "";
    public static final String BLUE = CleanColorfulFormatter.colorfulLogFormatterEnabled ? "\u001B[34m" : "";
    public static final String MAGENTA = CleanColorfulFormatter.colorfulLogFormatterEnabled ? "\u001B[35m" : "";
    public static final String CYAN = CleanColorfulFormatter.colorfulLogFormatterEnabled ? "\u001B[36m" : "";
    public static final String GREY = CleanColorfulFormatter.colorfulLogFormatterEnabled ? "\u001B[37m" : "";
    public static final String WHITE = CleanColorfulFormatter.colorfulLogFormatterEnabled ? "\u001B[97m" : "";
    public static final String FORMAT_PROPERTY = "org.hiero.block.node.app.logging.CleanColorfulFormatter.format";
    public static final String DEFAULT_FORMAT_STRING = "%TF %<TT.%<TL%<Tz %4$-7s %2$-40s %5$s%6$s%n";
    /** The string format of the log message */
    private final String format;

    /**
     * Make the logging colorful by wrapping the default console handler with a {@link CleanColorfulFormatter}.
     * This method should be called at the start of the application.
     */
    public static void makeLoggingColorful() {
        java.util.logging.Logger rootLogger = java.util.logging.Logger.getLogger("");
        Arrays.stream(rootLogger.getHandlers())
                .filter(handler -> handler instanceof ConsoleHandler)
                .forEach(handler -> {
                    if (!(handler.getFormatter() instanceof CleanColorfulFormatter)) {
                        handler.setFormatter(new CleanColorfulFormatter());
                    }
                });
    }

    /**
     * Creates a new {@link CleanColorfulFormatter} instance.
     */
    public CleanColorfulFormatter() {
        final String configuredFormat = LogManager.getLogManager().getProperty(FORMAT_PROPERTY);
        format = Objects.requireNonNullElse(configuredFormat, DEFAULT_FORMAT_STRING);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String format(LogRecord record) {
        final String color =
                switch (record.getLevel().getName()) {
                    case "SEVERE" -> RED;
                    case "WARNING" -> YELLOW;
                    case "INFO" -> WHITE;
                    case "CONFIG" -> GREY;
                    case "FINE", "FINER", "FINEST" -> CYAN;
                    default -> RESET;
                };
        ZonedDateTime zdt = ZonedDateTime.ofInstant(record.getInstant(), ZoneId.systemDefault());
        String source;
        if (record.getSourceClassName() != null) {
            source = record.getSourceClassName();
            source = source.substring(source.lastIndexOf('.') + 1);
            if (record.getSourceMethodName() != null) {
                source += "#" + record.getSourceMethodName();
            }
        } else {
            source = record.getLoggerName();
        }
        String message =
                formatMessage(record).replaceAll("\n", "\n        " + color); // indent and color multiple line messages
        String throwable = "";
        if (record.getThrown() != null) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            pw.println();
            record.getThrown().printStackTrace(pw);
            pw.close();
            throwable = sw.toString();
        }
        return color
                + String.format(
                        format,
                        zdt,
                        source,
                        record.getLoggerName(),
                        record.getLevel().getLocalizedName(),
                        message,
                        throwable)
                + RESET;
    }
}
