package org.apache.hadoop.ozone.recon.logs.LogParsers;


import org.apache.hadoop.ozone.recon.logs.LogModels.LogLine;
import org.apache.hadoop.ozone.recon.logs.LogParsers.LogParser;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class if a collection of utils to help parse log lines
 * written by Log4J-esque libraries.
 * Each LOG line is of the Regex following pattern
 * YYYY-MM-DD HH:mm:ss,sss LOG_LEVEL Source: Message
 */
public class Log4JParser implements LogParser {
  private final Pattern log4jpattern;

  Log4JParser() {
    final String date_regex = "(?<year>\\d{4})-(?<month>\\d{2})-(?<day>\\d{2})";
    final String time_regex = "(?<hour>\\d{2}):(?<minute>\\d{2}):(?<second>\\d{2})[,.](?<ms>\\d{3})";
    final String log_level_regex = "\\w+";
    // Match any non-whitespace character but exclude trailing colon
    final String source_regex = "\\S*[^ \\t\\n\\r\\f\\v:]";
    final String message_regex = ".*\\n?";


    final String log4j_regex = "(" + date_regex +
      ")\\s+(" + time_regex +
      ")\\s+(?<level>" + log_level_regex +
      ")\\s+(?<source>" + source_regex +
      "):?\\s*(?<message>" + message_regex + ")";

    this.log4jpattern = Pattern.compile(log4j_regex);
  }

  public LogLine ParseEvent(String line)
    throws Exception{
    Matcher m = this.log4jpattern.matcher(line);
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS", Locale.ENGLISH);
    Date dt;
    if (m.matches()) {
      String time = m.group("year") + "-"
        + m.group("month") + "-"
        + m.group("day") + " "
        + m.group("hour") + ":"
        + m.group("minute") + ":"
        + m.group("second") + "."
        + m.group("ms");
      dt = sdf.parse(time);
      return new LogLine(dt, m.group("level"), m.group("source"), m.group("message"));
    }
    else {
      // We were not able to handle the string matching
      // Raise exception to handle further up during response building
      throw new Exception("Failed to match log line for " + line);
    }
  }
}
