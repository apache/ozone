package org.apache.hadoop.ozone.recon.logging.LogParsers;

import org.apache.hadoop.ozone.recon.logging.LogModels.LogEvent;

import java.text.ParseException;


/**
 * Interface to allow later implementations to various logging libraries
 * Currently implements the Log4J log parser
 */
public interface LogParser {
  /**
   * This method will parse a log line and return the event
   * @param line Stores the line from the log file to parse
   * @return The event type
   */
  LogEvent parseEvent(String line) throws IllegalStateException, ParseException;
}
