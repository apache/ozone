package org.apache.hadoop.ozone.recon.logging.LogReaders;

import org.apache.hadoop.ozone.recon.logging.LogFileEmptyException;
import org.apache.hadoop.ozone.recon.logging.LogModels.LogEvent;
import org.apache.hadoop.ozone.recon.logging.LogParsers.Log4JParser;
import org.apache.hadoop.ozone.recon.logging.LogParsers.LogParser;
import org.jooq.meta.derby.sys.Sys;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

/**
 * This class is encapsulating the LogReader in order to add more event related
 * parsing functionality on top of it.
 * While LogReader deals with general reading of the file in chunks
 * this will provide more LOG related functionality
 *
 * An Event is the line in logfile with the following format:
 * [date] [time] [log level] [source] [message]
 * where the message might span multiple lines.
 * Essentially it is the result of a LOG.[level](...) statement
 */
public class LogEventReader {

  private final LogReader lr = new LogReader();
  private final LogParser lp = new Log4JParser();

  // Stores the next event in the log that is available
  private LogEvent nextEvent = null;

  public LogEventReader() { }

  public void initializeReader(String path) throws IOException, LogFileEmptyException {
    File file = new File(path);
    lr.initializeReader(file, "r");
  }

  /**
   * Utility function to find the occurrence of the next event in the log
   * @return {@link LogEvent} instance of the next event
   */
  private LogEvent findNextEvent()
      throws IOException, IllegalStateException, ParseException {
    LogEvent event = null;
    // Store the lines we read while trying to get the next event occurrence
    List<String> lines = new ArrayList<>();

    // Find the next event
    for (;;) {
      String logLine = lr.getNextLine();
      // We were not able to get the next line, reached EOF
      if (null == logLine) {
        break;
      }

      event = lp.parseEvent(logLine);
      if (null != event) {
        // Add all the lines encountered until now to the current event
        event.setPrevLines(lines);
        event.setOffset(lr.getCurrentOffset());
        break;
      }
      lines.add(logLine);
    }

    // If no event was found
    if (null == event) {
      //Return an event with blank information
      return new LogEvent();
    }
    return event;
  }

  /**
   * Utility function to find occurrence of the previous event in the log
   * @return {@link LogEvent} instance of the previous event
   */
  private LogEvent findPrevEvent()
      throws IOException, IllegalStateException, ParseException {

    LogEvent event = null;
    for(;;) {
      String logLine = lr.getPrevLine();
      // We reached file beginning
      if (null == logLine) {
        break;
      }

      event = lp.parseEvent(logLine);
      if (null != event) {
        event.setOffset(lr.getCurrentOffset());
        break;
      }
    }

    // If no event was found
    if (null == event) {
      // Return blank event
      return new LogEvent();
    }
    return event;
  }

  /**
   * Get the next event present in the logfile
   * @return the next event in the logfile
   * @throws IOException if something goes wrong while trying to read the logfile
   * @throws IllegalStateException if it is unable to find regex match in the log line
   * @throws ParseException if the log line is having incorrect date format
   */
  public LogEvent getNextEvent()
      throws IOException, IllegalStateException, ParseException {

    LogEvent event = null;

    // We already have the next event ready
    if (null != nextEvent) {
      event = nextEvent;
      event.addLinesToMessage(nextEvent.getPrevLines());
      nextEvent = null;
    } else {
      event = findNextEvent();
    }
    // we have found the event, so find the next event to this event as lookahead
    if (null != event) {
      nextEvent = findNextEvent();
      event.addLinesToMessage(nextEvent.getPrevLines());
    }
    return event;
  }

  /**
   * Get the previous event present in the logfile
   * @return the previous event in the logfile
   * @throws IOException if something goes wrong while trying to read the logfile
   * @throws IllegalStateException if it is unable to find regex match in the log line
   * @throws ParseException if the log line is having incorrect date format
   */
  public LogEvent getPrevEvent()
      throws IOException, IllegalStateException, ParseException {

    LogEvent event = null;
    if (null != nextEvent) {
      nextEvent = null;
    }

    event = findPrevEvent();
    // If we found the previous event, find the next event and
    // get its prev lines field. The next event's previous lines are the current event's lines
    if (null != event) {
      nextEvent = findNextEvent();
      event.addLinesToMessage(nextEvent.getPrevLines());
      // Go back to the current prev event
      // in order to bring file pointer to correct position
      findPrevEvent();
    }

    return event;
  }


  /**
   * Get the first event in the current logfile
   * @return {@link LogEvent} instance of the first event
   */
  public LogEvent getFirstEvent()
      throws IOException, IllegalStateException, ParseException {
    lr.goToFileStart();
    nextEvent = null;
    return getNextEvent();
  }

  /**
   * Get the last event in the current logfile
   * @return {@link LogEvent} instance of the last event
   */
  public LogEvent getLastEvent()
      throws IOException, IllegalStateException, ParseException {
    lr.goToFileEnd();
    nextEvent = null;
    return getPrevEvent();
  }

  /**
   * Get the event that is present near the provided offset position
   * @param offset  Stores the offset at which we want to find the event
   * @return {@link LogEvent} instance at the provided offset
   */
  public LogEvent getEventAt(long offset)
    throws IOException, ParseException {

    lr.goToPosition(offset);
    // Find the last event from this offset as provided offset might not be at an event
    LogEvent event = getPrevEvent();

    // If the current event is after the provided offset,
    // we need to set the file pointer at the proper event offset
    if (null != event && event.getOffset() > offset) {
      lr.goToPosition(event.getOffset());
    } else {
      // If the previous event is null then we might be too early in the file
      // In that case we need to get the next event
      event = getNextEvent();
      // If event is not null we might be at the end of file so get the minimum
      // of the event offset or the file size
      if (null != event) {
        lr.goToPosition(Math.min(event.getOffset(), lr.getFileSize()));
      }

    }
    return event;
  }

  public void close() throws IOException {
    lr.close();
  }
}
