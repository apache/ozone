package org.apache.hadoop.ozone.recon.logging;

import org.apache.hadoop.ozone.recon.logging.LogModels.LoggerResponse;

import java.io.IOException;
import java.text.ParseException;

public interface LogFetcher {

  enum Direction {
    FORWARD,
    REVERSE,
    NEUTRAL
  }

  /**
   * Return the log lines from offset in the provided direction
   *
   * @param offset     The offset of the log line
   * @param direction  The direction towards which we fetch
   *                   log data
   * @param events     The number of log events to fetch
   * @return A JSON formatted string of log events
   *
   * @throws IOException in case something goes wrong during file I/O operations
   * @throws ParseException in case of error during parsing of event timestamp
   */
  LoggerResponse.Builder getLogs(long offset, Direction direction, int events)
    throws IOException, ParseException, LogFileEmptyException;

  /**
   * TODO:
   * Return the log lines from timestamp in the provided direction
   *
   * @param timestamp  The timestamp at which we need to read logs
   * @param direction  The direction towards which we fetch
   *                   log data
   * @param lines      The number of log lines to fetch
   * @return A JSON formatted string of log events
   */
//  LoggerResponse.Builder getLogs(String timestamp, Direction direction, int lines);

  /**
   * TODO:
   * Search through the logs for log events.
   *
   * @param filters The filters to use while searching.
   * @param accumulator Accumulate the search results.
   * @throws java.io.IOException if something goes wrong.
   * @return A JSON string of the search results
   */
//  LoggerResponse.Builder searchLogs(LogFilters filters, LogEventAccumulator accumulator)  throws IOException;
}
