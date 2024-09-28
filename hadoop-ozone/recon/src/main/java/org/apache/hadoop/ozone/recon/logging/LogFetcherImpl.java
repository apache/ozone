package org.apache.hadoop.ozone.recon.logging;


import org.apache.hadoop.ozone.recon.logging.LogModels.LogEvent;
import org.apache.hadoop.ozone.recon.logging.LogModels.LoggerResponse;
import org.apache.hadoop.ozone.recon.logging.LogReaders.LogEventReader;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;

/**
 * This class will be used to implement the API call actions to /log endpoint
 */
public class LogFetcherImpl implements LogFetcher {

  private final LogEventReader logEventReader;

  String logDir = System.getProperty("hadoop.log.dir");
  String logFile = "ozone-recon.log";
  String RECON_LOG_FILE_LOC = logDir + "/" + logFile;

  public LogFetcherImpl() throws FileNotFoundException {
    logEventReader = new LogEventReader(RECON_LOG_FILE_LOC);
  }

  /**
   * Get the logs from a given offset
   * @param offset     The offset of the log line
   * @param direction  The direction towards which we fetch
   *                   log data
   * @param events     The number of log events to fetch
   * @return {@link LoggerResponse} instance of the events
   * @throws IOException in case of error in I/O operations
   * @throws ParseException in case of error while parsing log event timestamp
   */
  @Override
  public LoggerResponse.Builder getLogs(long offset, Direction direction, int events)
      throws IOException, ParseException {
    // Fetch the events
    Deque<LogEvent> logEventDeque = new LinkedList<>();
    // Fetch the event at offset
    logEventDeque.add(logEventReader.getEventAt(offset));

    for (int idx = 1; idx < events; idx++) {
      LogEvent event = null;
      if (Direction.FORWARD == direction) {
        event = logEventReader.getNextEvent();
        // Did not find any event so assume end of events
        if (null == event) {
          break;
        }
        logEventDeque.add(event);
      }

      if (Direction.REVERSE == direction) {
        event = logEventReader.getPrevEvent();
        // Did not find any event so assume end of events
        if (null == event) {
          break;
        }
        logEventDeque.addFirst(event);
      }
    }
    long firstEventOffset = logEventDeque.getFirst().getOffset();
    long lastEventOffset = logEventDeque.getLast().getOffset();
    return LoggerResponse.newBuilder()
      .setLogs(new ArrayList<>(logEventDeque))
      .setFirstOffset(firstEventOffset)
      .setLastOffset(lastEventOffset);
  }

  /**
   * Get the last events number of events
   * This is the default implementation for initial fetch of data
   * We will start from the end of the logfile for the most recent event
   * @param events Stores the number of events to get
   * @return The events from the end
   */
  public LoggerResponse.Builder getLogs(int events)
      throws IOException, ParseException {

    Deque<LogEvent> logEventDeque = new LinkedList<>();
    logEventDeque.add(logEventReader.getLastEvent());

    for (int idx = 1; idx < events; idx++) {
      LogEvent event = logEventReader.getPrevEvent();

      //Did not find any event so assume end of events
      if (null == event) {
        break;
      }
      // Since we are reading in reverse we need to add the events before current event
      logEventDeque.addFirst(event);
    }
    long firstEventOffset = logEventDeque.getFirst().getOffset();
    long lastEventOffset = logEventDeque.getLast().getOffset();
    return LoggerResponse.newBuilder()
      .setLogs(new ArrayList<>(logEventDeque))
      .setFirstOffset(firstEventOffset)
      .setLastOffset(lastEventOffset);
  }

  public void close() throws IOException {
    logEventReader.close();
  }

}
