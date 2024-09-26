package org.apache.hadoop.ozone.recon.logs;

import java.io.IOException;

public interface LogFetcher {
  /**
   * Return the log lines from timestamp in the provided direction
   *
   * @param timestamp  The timestamp at which we need to read logs
   * @param direction  The direction towards which we fetch
   *                   log data
   * @param lines      The number of log lines to fetch
   * @return A JSON formatted string of log events
   */
  String getLogs(String timestamp, int direction, int lines);
  /**
   * Return the log lines from offset in the provided direction
   *
   * @param offset     The offset of the log line
   * @param direction  The direction towards which we fetch
   *                   log data
   * @param lines      The number of log lines to fetch
   * @return A JSON formatted string of log events
   */
  String getLogs(long offset, int direction, int lines);

  /**
   * Search through the logs for log events.
   *
   * @param filters The filters to use while searching.
   * @param accumulator Accumulate the search results.
   * @throws java.io.IOException if something goes wrong.
   * @return A JSON string of the search results
   */
//  String searchLogs(LogFilters filters, LogEventAccumulator accumulator)  throws IOException;
}
