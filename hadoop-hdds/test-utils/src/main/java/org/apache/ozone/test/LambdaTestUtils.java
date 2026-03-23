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

package org.apache.ozone.test;

import com.google.common.base.Preconditions;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to make the most of Lambda expressions in Ozone tests.
 *
 * The code has been designed from the outset to be Java-8 friendly, but
 * to still be usable in Java 7.
 *
 * The code is modelled on {@code GenericTestUtils#waitFor(Supplier, int, int)},
 * but also lifts concepts from Scalatest's {@code awaitResult} and
 * its notion of pluggable retry logic (simple, backoff, maybe even things
 * with jitter: test author gets to choose).
 */
public final class LambdaTestUtils {

  private static final Logger LOG = LoggerFactory.getLogger(LambdaTestUtils.class);

  public static final String NULL_RESULT = "(null)";

  private LambdaTestUtils() {
  }

  /**
   * Interface to implement for converting a timeout into some form
   * of exception to raise.
   */
  @FunctionalInterface
  public interface TimeoutHandler {

    /**
     * Create an exception (or throw one, if desired).
     * @param timeoutMillis timeout which has arisen
     * @param caught any exception which was caught; may be null
     * @return an exception which will then be thrown
     * @throws Exception if the handler wishes to raise an exception
     * that way.
     */
    Throwable evaluate(int timeoutMillis, Throwable caught) throws Throwable;
  }

  /**
   * Wait for a condition to be met, with a retry policy returning the
   * sleep time before the next attempt is made. If, at the end
   * of the timeout period, the condition is still false (or failing with
   * an exception), the timeout handler is invoked, passing in the timeout
   * and any exception raised in the last invocation. The exception returned
   * by this timeout handler is then rethrown.
   * <p>
   * Example: Wait 30s for a condition to be met, with a sleep of 30s
   * between each probe.
   * If the operation is failing, then, after 30s, the timeout handler
   * is called. This returns the exception passed in (if any),
   * or generates a new one.
   * <pre>
   * {@code
   * await(
   *   30 * 1000,
   *   () -> { return 0 == filesystem.listFiles(new Path("/")).length); },
   *   () -> 500),
   *   (timeout, ex) -> ex != null ? ex : new TimeoutException("timeout"));
   * }
   * </pre>
   *
   * @param timeoutMillis timeout in milliseconds.
   * Can be zero, in which case only one attempt is made.
   * @param check predicate to evaluate
   * @param retry retry escalation logic
   * @param timeoutHandler handler invoked on timeout;
   * the returned exception will be thrown
   * @return the number of iterations before the condition was satisfied
   * @throws Exception the exception returned by {@code timeoutHandler} on
   * timeout
   * @throws FailFastException immediately if the evaluated operation raises it
   * @throws InterruptedException if interrupted.
   */
  public static int await(int timeoutMillis,
      Callable<Boolean> check,
      Callable<Integer> retry,
      TimeoutHandler timeoutHandler)
      throws Exception {
    Preconditions.checkArgument(timeoutMillis >= 0,
        "timeoutMillis must be >= 0");
    Objects.requireNonNull(timeoutHandler, "timeoutHandler == null");

    final long endTime = Time.monotonicNow() + timeoutMillis;
    Throwable ex = null;
    boolean running = true;
    int iterations = 0;
    while (running) {
      iterations++;
      try {
        if (check.call()) {
          return iterations;
        }
        // the probe failed but did not raise an exception. Reset any
        // exception raised by a previous probe failure.
        ex = null;
      } catch (InterruptedException
          | FailFastException
          | VirtualMachineError e) {
        throw e;
      } catch (Throwable e) {
        LOG.debug("await() iteration {}", iterations, e);
        ex = e;
      }
      running = Time.monotonicNow() < endTime;
      if (running) {
        int sleeptime = retry.call();
        if (sleeptime >= 0) {
          Thread.sleep(sleeptime);
        } else {
          running = false;
        }
      }
    }
    // timeout
    Throwable evaluate;
    try {
      evaluate = timeoutHandler.evaluate(timeoutMillis, ex);
      if (evaluate == null) {
        // bad timeout handler logic; fall back to GenerateTimeout so the
        // underlying problem isn't lost.
        LOG.error("timeout handler {} did not throw an exception ",
            timeoutHandler);
        evaluate = new GenerateTimeout().evaluate(timeoutMillis, ex);
      }
    } catch (Throwable throwable) {
      evaluate = throwable;
    }
    return raise(evaluate);
  }

  /**
   * Simplified {@link #await(int, Callable, Callable, TimeoutHandler)}
   * operation with a fixed interval
   * and {@link GenerateTimeout} handler to generate a {@code TimeoutException}.
   * <p>
   * Example: await for probe to succeed:
   * <pre>
   * {@code
   * await(
   *   30 * 1000, 500,
   *   () -> { return 0 == filesystem.listFiles(new Path("/")).length); });
   * }
   * </pre>
   *
   * @param timeoutMillis timeout in milliseconds.
   * Can be zero, in which case only one attempt is made.
   * @param intervalMillis interval in milliseconds between checks
   * @param check predicate to evaluate
   * @return the number of iterations before the condition was satisfied
   * @throws Exception returned by {@code failure} on timeout
   * @throws FailFastException immediately if the evaluated operation raises it
   * @throws InterruptedException if interrupted.
   */
  public static int await(int timeoutMillis,
      int intervalMillis,
      Callable<Boolean> check) throws Exception {
    return await(timeoutMillis, check,
        new FixedRetryInterval(intervalMillis),
        new GenerateTimeout());
  }

  /**
   * Take the throwable and raise it as an exception or an error, depending
   * upon its type. This allows callers to declare that they only throw
   * Exception (i.e. can be invoked by Callable) yet still rethrow a
   * previously caught Throwable.
   * @param throwable Throwable to rethrow
   * @param <T> expected return type
   * @return never
   * @throws Exception if throwable is an Exception
   * @throws Error if throwable is not an Exception
   */
  private static <T> T raise(Throwable throwable) throws Exception {
    if (throwable instanceof Exception) {
      throw (Exception) throwable;
    } else {
      throw (Error) throwable;
    }
  }

  /**
   * Robust string converter for exception messages; if the {@code toString()}
   * method throws an exception then that exception is caught and logged,
   * then a simple string of the classname logged.
   * This stops a {@code toString()} failure hiding underlying problems.
   * @param o object to stringify
   * @return a string for exception messages
   */
  private static String robustToString(Object o) {
    if (o == null) {
      return NULL_RESULT;
    } else {
      try {
        return o.toString();
      } catch (Exception e) {
        LOG.info("Exception calling toString()", e);
        return o.getClass().toString();
      }
    }
  }

  /**
   * Returns {@code TimeoutException} on a timeout. If
   * there was a inner class passed in, includes it as the
   * inner failure.
   */
  public static class GenerateTimeout implements TimeoutHandler {
    private final String message;

    public GenerateTimeout(String message) {
      this.message = message;
    }

    public GenerateTimeout() {
      this("timeout");
    }

    /**
     * Evaluate operation creates a new {@code TimeoutException}.
     * @param timeoutMillis timeout in millis
     * @param caught optional caught exception
     * @return TimeoutException
     */
    @Override
    public Throwable evaluate(int timeoutMillis, Throwable caught) {
      String s = String.format("%s: after %d millis", message,
          timeoutMillis);
      String caughtText = caught != null
          ? ("; " + robustToString(caught)) : "";

      return (new TimeoutException(s + caughtText)
                                     .initCause(caught));
    }
  }

  /**
   * Retry at a fixed time period between calls.
   */
  public static class FixedRetryInterval implements Callable<Integer> {
    private final int intervalMillis;
    private int invocationCount = 0;

    public FixedRetryInterval(int intervalMillis) {
      Preconditions.checkArgument(intervalMillis > 0);
      this.intervalMillis = intervalMillis;
    }

    @Override
    public Integer call() throws Exception {
      invocationCount++;
      return intervalMillis;
    }

    public int getInvocationCount() {
      return invocationCount;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder(
          "FixedRetryInterval{");
      sb.append("interval=").append(intervalMillis);
      sb.append(", invocationCount=").append(invocationCount);
      sb.append('}');
      return sb.toString();
    }
  }

  /**
   * An exception which triggers a fast exist from the
   * {@link #await(int, Callable, Callable, TimeoutHandler)} loops.
   */
  public static class FailFastException extends Exception {

    public FailFastException(String detailMessage) {
      super(detailMessage);
    }

    public FailFastException(String message, Throwable cause) {
      super(message, cause);
    }

    /**
     * Instantiate from a format string.
     * @param format format string
     * @param args arguments to format
     * @return an instance with the message string constructed.
     */
    public static FailFastException newInstance(String format, Object...args) {
      return new FailFastException(String.format(format, args));
    }
  }

  /**
   * A simple interface for lambdas, which returns nothing; this exists
   * to simplify lambda tests on operations with no return value.
   */
  @FunctionalInterface
  public interface VoidCallable {
    void call() throws Exception;
  }

}
