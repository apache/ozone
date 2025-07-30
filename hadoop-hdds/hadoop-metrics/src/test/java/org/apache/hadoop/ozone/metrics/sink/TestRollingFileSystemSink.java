/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.metrics.sink;

import java.util.Calendar;
import org.apache.commons.configuration2.SubsetConfiguration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.ozone.metrics.MetricsException;
import org.apache.hadoop.ozone.metrics.impl.ConfigBuilder;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test that the init() method picks up all the configuration settings
 * correctly.
 */
public class TestRollingFileSystemSink {
  @Test
  public void testInit() {
    ConfigBuilder builder = new ConfigBuilder();
    SubsetConfiguration conf =
        builder.add("sink.roll-interval", "10m")
            .add("sink.roll-offset-interval-millis", "1")
            .add("sink.basepath", "path")
            .add("sink.ignore-error", "true")
            .add("sink.allow-append", "true")
            .add("sink.source", "src")
            .subset("sink");

    RollingFileSystemSink sink = new RollingFileSystemSink();

    sink.init(conf);

    assertEquals(sink.rollIntervalMillis, 600000,
        "The roll interval was not set correctly");
    assertEquals(sink.rollOffsetIntervalMillis, 1,
        "The roll offset interval was not set correctly");
    assertEquals(sink.basePath, new Path("path"),
        "The base path was not set correctly");
    assertEquals(sink.ignoreError, true, "ignore-error was not set correctly");
    assertEquals(sink.allowAppend, true, "allow-append was not set correctly");
    assertEquals(sink.source, "src", "The source was not set correctly");
  }

  /**
   * Test whether the initial roll interval is set correctly.
   */
  @Test
  public void testSetInitialFlushTime() {
    RollingFileSystemSink rfsSink = new RollingFileSystemSink(1000, 0);
    Calendar calendar = Calendar.getInstance();

    calendar.set(Calendar.MILLISECOND, 0);
    calendar.set(Calendar.SECOND, 0);
    calendar.set(Calendar.MINUTE, 0);
    calendar.set(Calendar.HOUR, 0);
    calendar.set(Calendar.DAY_OF_YEAR, 1);
    calendar.set(Calendar.YEAR, 2016);

    assertNull(
        rfsSink.nextFlush, "Last flush time should have been null prior to calling init()");

    rfsSink.setInitialFlushTime(calendar.getTime());

    long diff =
        rfsSink.nextFlush.getTimeInMillis() - calendar.getTimeInMillis();

    assertEquals(0L, diff, "The initial flush time was calculated incorrectly");

    calendar.set(Calendar.MILLISECOND, 10);
    rfsSink.setInitialFlushTime(calendar.getTime());
    diff = rfsSink.nextFlush.getTimeInMillis() - calendar.getTimeInMillis();

    assertEquals(
        -10L, diff, "The initial flush time was calculated incorrectly");

    calendar.set(Calendar.SECOND, 1);
    calendar.set(Calendar.MILLISECOND, 10);
    rfsSink.setInitialFlushTime(calendar.getTime());
    diff = rfsSink.nextFlush.getTimeInMillis() - calendar.getTimeInMillis();

    assertEquals(
        -10L, diff, "The initial flush time was calculated incorrectly");

    // Try again with a random offset
    rfsSink = new RollingFileSystemSink(1000, 100);

    assertNull(
        rfsSink.nextFlush, "Last flush time should have been null prior to calling init()");

    calendar.set(Calendar.MILLISECOND, 0);
    calendar.set(Calendar.SECOND, 0);
    rfsSink.setInitialFlushTime(calendar.getTime());

    diff = rfsSink.nextFlush.getTimeInMillis() - calendar.getTimeInMillis();

    assertTrue((diff == 0L) || ((diff > -1000L) && (diff < -900L)),
        "The initial flush time was calculated incorrectly: " + diff);

    calendar.set(Calendar.MILLISECOND, 10);
    rfsSink.setInitialFlushTime(calendar.getTime());
    diff = rfsSink.nextFlush.getTimeInMillis() - calendar.getTimeInMillis();

    assertTrue((diff >= -10L) && (diff <= 0L) || ((diff > -1000L) && (diff < -910L)),
        "The initial flush time was calculated incorrectly: " + diff);

    calendar.set(Calendar.SECOND, 1);
    calendar.set(Calendar.MILLISECOND, 10);
    rfsSink.setInitialFlushTime(calendar.getTime());
    diff = rfsSink.nextFlush.getTimeInMillis() - calendar.getTimeInMillis();

    assertTrue((diff >= -10L) && (diff <= 0L) || ((diff > -1000L) && (diff < -910L)),
        "The initial flush time was calculated incorrectly: " + diff);

    // Now try pathological settings
    rfsSink = new RollingFileSystemSink(1000, 1000000);

    assertNull(rfsSink.nextFlush,
        "Last flush time should have been null prior to calling init()");

    calendar.set(Calendar.MILLISECOND, 1);
    calendar.set(Calendar.SECOND, 0);
    rfsSink.setInitialFlushTime(calendar.getTime());

    diff = rfsSink.nextFlush.getTimeInMillis() - calendar.getTimeInMillis();

    assertTrue((diff > -1000L) && (diff <= 0L),
        "The initial flush time was calculated incorrectly: " + diff);
  }

  /**
   * Test that the roll time updates correctly.
   */
  @Test
  public void testUpdateRollTime() {
    RollingFileSystemSink rfsSink = new RollingFileSystemSink(1000, 0);
    Calendar calendar = Calendar.getInstance();

    calendar.set(Calendar.MILLISECOND, 0);
    calendar.set(Calendar.SECOND, 0);
    calendar.set(Calendar.MINUTE, 0);
    calendar.set(Calendar.HOUR, 0);
    calendar.set(Calendar.DAY_OF_YEAR, 1);
    calendar.set(Calendar.YEAR, 2016);

    rfsSink.nextFlush = Calendar.getInstance();
    rfsSink.nextFlush.setTime(calendar.getTime());
    rfsSink.updateFlushTime(calendar.getTime());

    assertEquals(calendar.getTimeInMillis() + 1000,
        rfsSink.nextFlush.getTimeInMillis(),
        "The next roll time should have been 1 second in the future");

    rfsSink.nextFlush.setTime(calendar.getTime());
    calendar.add(Calendar.MILLISECOND, 10);
    rfsSink.updateFlushTime(calendar.getTime());

    assertEquals(calendar.getTimeInMillis() + 990,
        rfsSink.nextFlush.getTimeInMillis(),
        "The next roll time should have been 990 ms in the future");

    rfsSink.nextFlush.setTime(calendar.getTime());
    calendar.add(Calendar.SECOND, 2);
    calendar.add(Calendar.MILLISECOND, 10);
    rfsSink.updateFlushTime(calendar.getTime());

    assertEquals(calendar.getTimeInMillis() + 990,
        rfsSink.nextFlush.getTimeInMillis(),
        "The next roll time should have been 990 ms in the future");
  }

  /**
   * Test whether the roll interval is correctly calculated from the
   * configuration settings.
   */
  @Test
  public void testGetRollInterval() {
    doTestGetRollInterval(1, new String[] {"m", "min", "minute", "minutes"},
        60 * 1000L);
    doTestGetRollInterval(1, new String[] {"h", "hr", "hour", "hours"},
        60 * 60 * 1000L);
    doTestGetRollInterval(1, new String[] {"d", "day", "days"},
        24 * 60 * 60 * 1000L);

    ConfigBuilder builder = new ConfigBuilder();
    SubsetConfiguration conf =
        builder.add("sink.roll-interval", "1").subset("sink");
    // We can reuse the same sink evry time because we're setting the same
    // property every time.
    RollingFileSystemSink sink = new RollingFileSystemSink();

    sink.init(conf);

    assertEquals(3600000L, sink.getRollInterval());

    for (char c : "abcefgijklnopqrtuvwxyz".toCharArray()) {
      builder = new ConfigBuilder();
      conf = builder.add("sink.roll-interval", "90 " + c).subset("sink");

      try {
        sink.init(conf);
        sink.getRollInterval();
        fail("Allowed flush interval with bad units: " + c);
      } catch (MetricsException ex) {
        // Expected
      }
    }
  }

  /**
   * Test the basic unit conversions with the given unit name modifier applied.
   *
   * @param mod a unit name modifier
   */
  private void doTestGetRollInterval(int num, String[] units, long expected) {
    RollingFileSystemSink sink = new RollingFileSystemSink();
    ConfigBuilder builder = new ConfigBuilder();

    for (String unit : units) {
      sink.init(builder.add("sink.roll-interval", num + unit).subset("sink"));
      assertEquals(expected, sink.getRollInterval());

      sink.init(builder.add("sink.roll-interval",
          num + unit.toUpperCase()).subset("sink"));
      assertEquals(expected, sink.getRollInterval());

      sink.init(builder.add("sink.roll-interval",
          num + " " + unit).subset("sink"));
      assertEquals(expected, sink.getRollInterval());

      sink.init(builder.add("sink.roll-interval",
          num + " " + unit.toUpperCase()).subset("sink"));
      assertEquals(expected, sink.getRollInterval());
    }
  }
}
