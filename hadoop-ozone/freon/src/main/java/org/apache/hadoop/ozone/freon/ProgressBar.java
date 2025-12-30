/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.freon;

import java.io.PrintStream;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates and runs a ProgressBar in new Thread which gets printed on
 * the provided PrintStream.
 */
public class ProgressBar {

  private static final Logger LOG = LoggerFactory.getLogger(ProgressBar.class);

  private final long maxValue;
  private final LongSupplier currentValue;
  private final Thread thread;

  private volatile boolean running;

  private volatile long startTime;

  /**
   * True if the progress bar is used from an interactive environment (shell).
   */
  private boolean interactive;
  private Supplier<String> supplier;

  /**
   * Creates a new ProgressBar instance which prints the progress on the given
   * PrintStream when started.
   *
   * @param stream to display the progress
   * @param maxValue Maximum value of the progress
   * @param currentValue Supplier that provides the current value
   */
  public ProgressBar(final PrintStream stream, final long maxValue,
                     final LongSupplier currentValue) {
    this(stream, maxValue, currentValue, System.console() != null, () -> "");
  }

  /**
   * Creates a new ProgressBar instance which prints the progress on the given
   * PrintStream when started.
   *
   * @param stream       to display the progress
   * @param maxValue     Maximum value of the progress
   * @param currentValue Supplier that provides the current progressbar value
   * @param interactive  Print progressbar for interactive environments.
   * @param supplier     Supplier that provides the real time message
   */
  public ProgressBar(final PrintStream stream, final long maxValue,
      final LongSupplier currentValue, boolean interactive,
      final Supplier<String> supplier) {
    this.maxValue = maxValue;
    this.currentValue = currentValue;
    this.thread = new Thread(getProgressBar(stream));
    this.interactive = interactive;
    this.supplier = supplier;
  }

  /**
   * Starts the ProgressBar in a new Thread.
   * This is a non blocking call.
   */
  public synchronized void start() {
    if (!running) {
      running = true;
      startTime = System.nanoTime();
      thread.start();
    }
  }

  /**
   * Graceful shutdown, waits for the progress bar to complete.
   * This is a blocking call.
   */
  public synchronized void shutdown() {
    if (running) {
      try {
        thread.join();
        running = false;
      } catch (InterruptedException e) {
        LOG.warn("Got interrupted while waiting for the progress bar to " +
                "complete.");
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Terminates the progress bar. This doesn't wait for the progress bar
   * to complete.
   */
  public synchronized void terminate() {
    if (running) {
      try {
        running = false;
        thread.join();
      } catch (InterruptedException e) {
        LOG.warn("Got interrupted while waiting for the progress bar to " +
                "complete.");
        Thread.currentThread().interrupt();
      }
    }
  }

  private Runnable getProgressBar(final PrintStream stream) {
    return () -> {
      println(stream);
      while (running && currentValue.getAsLong() < maxValue) {
        print(stream, currentValue.getAsLong());
        try {
          Thread.sleep(1000L);
        } catch (InterruptedException e) {
          LOG.warn("ProgressBar was interrupted.");
          Thread.currentThread().interrupt();
        }
      }
      print(stream, currentValue.getAsLong());
      println(stream);
      running = false;
    };
  }

  /**
   * Given current value prints the progress bar.
   *
   * @param value current progress position
   */
  public void print(final PrintStream stream, final long value) {
    if (interactive) {
      printProgressBar(stream, value);
    } else {
      logProgressBar(stream, value);
    }
  }

  private void println(PrintStream stream) {
    if (interactive) {
      stream.println();
    }
  }

  private void logProgressBar(PrintStream stream, long value) {
    double percent = 100.0 * value / maxValue;
    LOG.info(String
        .format("Progress: %.2f %% (%d out of %d)", percent, value, maxValue));
  }

  private void printProgressBar(PrintStream stream, long value) {
    stream.print('\r');
    double percent = 100.0 * value / maxValue;
    StringBuilder sb = new StringBuilder();
    String realTimeMessage = supplier.get();
    int shrinkTimes = 1;
    if (!realTimeMessage.isEmpty()) {
      shrinkTimes = 3;
    }
    sb.append(' ').append(String.format("%.2f", percent)).append("% |");
    for (int i = 0; i <= percent / shrinkTimes; i++) {
      sb.append('█');
    }
    for (int j = 0; j < (100 - percent) / shrinkTimes; j++) {
      sb.append(' ');
    }
    sb.append("|  ");
    sb.append(value).append('/').append(maxValue);
    long timeInSec = TimeUnit.SECONDS.convert(
        System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
    String timeToPrint = String.format("%d:%02d:%02d", timeInSec / 3600,
        (timeInSec % 3600) / 60, timeInSec % 60);
    sb.append(" Time: ").append(timeToPrint);
    sb.append("|  ");
    sb.append(realTimeMessage);
    stream.print(sb.toString());
  }
}
