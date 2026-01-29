package org.hiero.block.tools.states;

import java.time.Instant;

public class TimeConverted {
    public static void main(String[] args) {
        String timeStamp = "1568412919.286477002";
        long epochSecond = Long.parseLong(timeStamp.substring(0, timeStamp.indexOf('.')));
        long nano = Long.parseLong(timeStamp.substring(timeStamp.indexOf('.') + 1));
        Instant time = Instant.ofEpochSecond(//
                epochSecond, // from getEpochSecond()
                nano); // from getNano()
        System.out.println("Converted time: " + time);

        String utcTime = "2019-09-13T21_54_30.872035001Z";
        Instant utcInstant = Instant.parse(utcTime.replace("_", ":"));
        System.out.println("Converted UTC time: " + utcInstant + " -> "+
                utcInstant.getEpochSecond() + "." + utcInstant.getNano());

    }
}
