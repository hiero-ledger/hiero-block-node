package org.hiero.block.tools.utils;

import static java.time.ZoneOffset.UTC;

import com.hedera.hapi.node.base.Timestamp;
import java.time.Instant;
import org.hiero.block.tools.records.RecordFileUtils;

/**
 * Utility class for converting between Timestamp and Instant.
 */
public class TimeUtils {
    /** Genesis instant for special case handling */
    public static final Instant GENESIS_INSTANT = RecordFileUtils.extractRecordFileTime("2019-09-13T21_53_51.396440Z.rcd")
        .atOffset(UTC).toInstant();
    /** Genesis timestamp for special case handling */
    public static final Timestamp GENESIS_TIMESTAMP = toTimestamp(GENESIS_INSTANT);

    /**
     * Converts a Timestamp to an Instant.
     *
     * @param timestamp the Timestamp to convert
     * @return the corresponding Instant
     */
    public static Instant toInstant(Timestamp timestamp) {
        return Instant.ofEpochSecond(timestamp.seconds(), timestamp.nanos());
    }

    /**
     * Converts an Instant to a Timestamp.
     *
     * @param instant the Instant to convert
     * @return the corresponding Timestamp
     */
    public static Timestamp toTimestamp(Instant instant) {
        return new Timestamp(instant.getEpochSecond(), instant.getNano());
    }
}
