package org.apache.hadoop.hdds.util;

/**
 * Pretty duration string representation.
 */
public final class DurationUtil {

  private DurationUtil() {
  }

  public static String getPrettyDuration(long durationSeconds) {
    String prettyDuration;
    if (durationSeconds >= 0 && durationSeconds < 60) {
      prettyDuration = durationSeconds + "s";
    } else if (durationSeconds >= 60 && durationSeconds < 3600) {
      prettyDuration = (durationSeconds / 60 + "m " + durationSeconds % 60 + "s");
    } else if (durationSeconds >= 3600) {
      prettyDuration =
          (durationSeconds / 60 / 60 + "h " + durationSeconds / 60 % 60 + "m " + durationSeconds % 60 + "s");
    } else {
      throw new IllegalStateException("Incorrect duration exception" + durationSeconds);
    }
    return prettyDuration;
  }
}
