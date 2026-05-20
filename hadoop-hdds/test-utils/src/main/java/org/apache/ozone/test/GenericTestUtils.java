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

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.base.Preconditions;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.CharSequenceInputStream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Layout;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.ratis.util.function.CheckedSupplier;
import org.junit.jupiter.api.Assertions;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;

/**
 * Provides some very generic helpers which might be used across the tests.
 */
public abstract class GenericTestUtils {

  /**
   * Error string used in
   * {@link GenericTestUtils#waitFor(BooleanSupplier, int, int)}.
   */
  public static final String ERROR_MISSING_ARGUMENT =
      "Input supplier interface should be initialized";
  public static final String ERROR_INVALID_ARGUMENT =
      "Total wait time should be greater than check interval time";

  private static final long NANOSECONDS_PER_MILLISECOND = 1_000_000;

  /**
   * Return current time in millis as an {@code Instant}.  This may be
   * before {@link Instant#now()}, since the latter includes nanoseconds, too.
   * This is needed for some tests that verify volume/bucket creation time,
   * which also uses {@link Instant#ofEpochMilli(long)}.
   *
   * @return current time as {@code Instant};
   */
  public static Instant getTestStartTime() {
    return Instant.ofEpochMilli(System.currentTimeMillis());
  }

  /**
   * Waits for a condition specified by the given {@code check} to return {@code true}.
   * If the condition throws an exception, the operation would be retried assuming the condition didn't get satisfied.
   * The condition will be checked initially and then at intervals specified by
   * {@code checkEveryMillis}, until the total time exceeds {@code waitForMillis}.
   * If the condition is not satisfied within the allowed time, a {@link TimeoutException}
   * is thrown. If interrupted while waiting, an {@link InterruptedException} is thrown.
   */
  public static <E extends Exception> void waitFor(CheckedSupplier<Boolean, E> check, int checkEveryMillis,
      int waitForMillis) throws InterruptedException, TimeoutException {
    waitFor((BooleanSupplier) () -> {
      try {
        return check.get();
      } catch (Exception e) {
        return false;
      }
    }, checkEveryMillis, waitForMillis);
  }

  /**
   * Wait for the specified test to return true. The test will be performed
   * initially and then every {@code checkEveryMillis} until at least
   * {@code waitForMillis} time has expired. If {@code check} is null or
   * {@code waitForMillis} is less than {@code checkEveryMillis} this method
   * will throw an {@link IllegalArgumentException}.
   *
   * @param check            the test to perform
   * @param checkEveryMillis how often to perform the test
   * @param waitForMillis    the amount of time after which no more tests
   *                         will be
   *                         performed
   * @throws TimeoutException     if the test does not return true in the
   *                              allotted
   *                              time
   * @throws InterruptedException if the method is interrupted while waiting
   */
  public static void waitFor(BooleanSupplier check, int checkEveryMillis,
      int waitForMillis) throws TimeoutException, InterruptedException {
    Objects.requireNonNull(check, ERROR_MISSING_ARGUMENT);
    Preconditions.checkArgument(waitForMillis >= checkEveryMillis,
        ERROR_INVALID_ARGUMENT);

    long st = monotonicNow();
    boolean result = check.getAsBoolean();

    while (!result && (monotonicNow() - st < waitForMillis)) {
      Thread.sleep(checkEveryMillis);
      result = check.getAsBoolean();
    }

    if (!result) {
      throw new TimeoutException("Timed out waiting for condition. " +
          "Thread diagnostics:\n" +
          TimedOutTestsListener.buildThreadDiagnosticString());
    }
  }

  public static <T extends Throwable> T assertThrows(
      Class<T> expectedType,
      Callable<? extends AutoCloseable> func) {
    return Assertions.assertThrows(expectedType, () -> {
      final AutoCloseable closeable = func.call();
      try {
        if (closeable != null) {
          closeable.close();
        }
      } catch (Exception ignored) {
      }
    });
  }

  /**
   * @deprecated use sl4fj based version
   */
  @Deprecated
  public static void setLogLevel(Logger logger, Level level) {
    logger.setLevel(level);
  }

  public static void setLogLevel(org.slf4j.Logger logger,
      org.slf4j.event.Level level) {
    setLogLevel(toLog4j(logger), Level.toLevel(level.toString()));
  }

  public static void setLogLevel(Class<?> clazz, org.slf4j.event.Level level) {
    setLogLevel(LoggerFactory.getLogger(clazz), level);
  }

  public static void withLogDisabled(Class<?> clazz, Runnable task) {
    org.slf4j.Logger logger = LoggerFactory.getLogger(clazz);
    final Logger log4j = toLog4j(logger);
    final Level level = log4j.getLevel();
    setLogLevel(log4j, Level.OFF);
    try {
      task.run();
    } finally {
      setLogLevel(log4j, level);
    }
  }

  public static <T> T mockFieldReflection(Object object, String fieldName)
          throws NoSuchFieldException, IllegalAccessException {
    Field field = object.getClass().getDeclaredField(fieldName);
    boolean isAccessible = field.isAccessible();

    field.setAccessible(true);
    Field modifiersField = ReflectionUtils.getModifiersField();
    boolean modifierFieldAccessible = modifiersField.isAccessible();
    modifiersField.setAccessible(true);
    int modifierVal = modifiersField.getInt(field);
    modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
    T value = (T) field.get(object);
    value = Mockito.spy(value);
    field.set(object, value);
    modifiersField.setInt(field, modifierVal);
    modifiersField.setAccessible(modifierFieldAccessible);
    field.setAccessible(isAccessible);
    return value;
  }

  public static <T> T getFieldReflection(Object object, String fieldName)
          throws NoSuchFieldException, IllegalAccessException {
    Field field = object.getClass().getDeclaredField(fieldName);
    boolean isAccessible = field.isAccessible();

    field.setAccessible(true);
    Field modifiersField = ReflectionUtils.getModifiersField();
    boolean modifierFieldAccessible = modifiersField.isAccessible();
    modifiersField.setAccessible(true);
    int modifierVal = modifiersField.getInt(field);
    modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
    T value = (T) field.get(object);
    modifiersField.setInt(field, modifierVal);
    modifiersField.setAccessible(modifierFieldAccessible);
    field.setAccessible(isAccessible);
    return value;
  }

  public static <K, V> Map<V, K> getReverseMap(Map<K, List<V>> map) {
    return map.entrySet().stream().flatMap(entry -> entry.getValue().stream()
            .map(v -> Pair.of(v, entry.getKey())))
            .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
  }

  /**
   * Class to capture logs for doing assertions.
   */
  public abstract static class LogCapturer {
    private final StringWriter sw = new StringWriter();

    public static LogCapturer captureLogs(Logger logger) {
      return new Log4j1Capturer(logger);
    }

    public static LogCapturer captureLogs(Logger logger, Layout layout) {
      return new Log4j1Capturer(logger, layout);
    }

    public static LogCapturer captureLogs(Class<?> clazz) {
      return captureLogs(LoggerFactory.getLogger(clazz));
    }

    public static LogCapturer captureLogs(org.slf4j.Logger logger) {
      return new Log4j1Capturer(toLog4j(logger));
    }

    // TODO: let Log4j2Capturer capture only specific logger's logs
    public static LogCapturer log4j2(String ignoredLoggerName) {
      return Log4j2Capturer.getInstance();
    }

    public String getOutput() {
      return writer().toString();
    }

    public abstract void stopCapturing();

    protected StringWriter writer() {
      return sw;
    }

    public void clearOutput() {
      writer().getBuffer().setLength(0);
    }
  }

  @Deprecated
  public static Logger toLog4j(org.slf4j.Logger logger) {
    return LogManager.getLogger(logger.getName());
  }

  private static long monotonicNow() {
    return System.nanoTime() / NANOSECONDS_PER_MILLISECOND;
  }

  public static PrintStreamCapturer captureOut() {
    return new SystemOutCapturer();
  }

  public static PrintStreamCapturer captureErr() {
    return new SystemErrCapturer();
  }

  /** Capture contents of a {@code PrintStream}, until {@code close()}d. */
  public abstract static class PrintStreamCapturer implements AutoCloseable, Supplier<String> {
    private final ByteArrayOutputStream bytes;
    private final PrintStream bytesPrintStream;
    private final PrintStream old;
    private final Consumer<PrintStream> restore;

    protected PrintStreamCapturer(PrintStream out, Consumer<PrintStream> install) {
      old = out;
      bytes = new ByteArrayOutputStream();
      try {
        bytesPrintStream = new PrintStream(bytes, false, UTF_8.name());
        install.accept(new TeePrintStream(out, bytesPrintStream));
        restore = install;
      } catch (UnsupportedEncodingException e) {
        throw new IllegalStateException(e);
      }
    }

    @Override
    public String get() {
      return getOutput();
    }

    public String getOutput() {
      try {
        return bytes.toString(UTF_8.name());
      } catch (UnsupportedEncodingException e) {
        throw new IllegalStateException(e);
      }
    }

    public void reset() {
      bytes.reset();
    }

    @Override
    public void close() throws Exception {
      IOUtils.closeQuietly(bytesPrintStream);
      restore.accept(old);
    }
  }

  /**
   * Capture output printed to {@link System#err}.
   * <p>
   * Usage:
   * <pre>
   *   try (PrintStreamCapturer capture = captureErr()) {
   *     ...
   *     // Call capture.getOutput() to get the output string
   *   }
   * </pre>
   */
  public static class SystemErrCapturer extends PrintStreamCapturer {
    public SystemErrCapturer() {
      super(System.err, System::setErr);
    }
  }

  /**
   * Capture output printed to {@link System#out}.
   * <p>
   * Usage:
   * <pre>
   *   try (PrintStreamCapturer capture = captureOut()) {
   *     ...
   *     // Call capture.getOutput() to get the output string
   *   }
   * </pre>
   */
  public static class SystemOutCapturer extends PrintStreamCapturer {
    public SystemOutCapturer() {
      super(System.out, System::setOut);
    }
  }

  /**
   * Replaces {@link System#in} with a stream that provides {@code lines} as input.
   * @return an {@code AutoCloseable} to restore the original {@link System#in} stream
   */
  public static AutoCloseable supplyOnSystemIn(String... lines) {
    final InputStream original = System.in;
    final InputStream in = CharSequenceInputStream.builder()
        .setCharSequence(String.join("\n", lines))
        .get();
    System.setIn(in);
    return () -> System.setIn(original);
  }

  /**
   * Prints output to one {@link PrintStream} while copying to the other.
   * <p>
   * Closing the main {@link PrintStream} will NOT close the other.
   */
  public static class TeePrintStream extends PrintStream {
    private final PrintStream other;

    public TeePrintStream(OutputStream main, PrintStream other)
        throws UnsupportedEncodingException {
      super(main, false, UTF_8.name());
      this.other = other;
    }

    @Override
    public void flush() {
      super.flush();
      other.flush();
    }

    @Override
    public void write(byte[] buf, int off, int len) {
      super.write(buf, off, len);
      other.write(buf, off, len);
    }
  }

  /**
   * Helper class to get free port avoiding randomness.
   */
  public static final class PortAllocator {

    public static final String HOSTNAME = "localhost";
    public static final String HOST_ADDRESS = "127.0.0.1";
    public static final int MIN_PORT = 15000;
    public static final int MAX_PORT = 32000;
    public static final AtomicInteger NEXT_PORT = new AtomicInteger(MIN_PORT);

    private PortAllocator() {
      // no instances
    }

    public static synchronized int getFreePort() {
      int port = NEXT_PORT.getAndIncrement();
      if (port > MAX_PORT) {
        NEXT_PORT.set(MIN_PORT);
        port = NEXT_PORT.getAndIncrement();
      }
      return port;
    }

    public static String localhostWithFreePort() {
      return HOST_ADDRESS + ":" + getFreePort();
    }

    public static String anyHostWithFreePort() {
      return "0.0.0.0:" + getFreePort();
    }
  }

  /**
   * This class is a utility class for java reflection operations.
   */
  public static final class ReflectionUtils {

    /**
     * This method provides the modifiers field using reflection approach which is compatible
     * for both pre Java 9 and post java 9 versions.
     * @return modifiers field
     * @throws IllegalAccessException illegalAccessException,
     * @throws NoSuchFieldException noSuchFieldException.
     */
    public static Field getModifiersField() throws IllegalAccessException, NoSuchFieldException {
      Field modifiersField = null;
      try {
        modifiersField = Field.class.getDeclaredField("modifiers");
      } catch (NoSuchFieldException e) {
        try {
          Method getDeclaredFields0 = Class.class.getDeclaredMethod(
              "getDeclaredFields0", boolean.class);
          boolean accessibleBeforeSet = getDeclaredFields0.isAccessible();
          getDeclaredFields0.setAccessible(true);
          Field[] fields = (Field[]) getDeclaredFields0.invoke(Field.class, false);
          getDeclaredFields0.setAccessible(accessibleBeforeSet);
          for (Field field : fields) {
            if ("modifiers".equals(field.getName())) {
              modifiersField = field;
              break;
            }
          }
          if (modifiersField == null) {
            throw e;
          }
        } catch (InvocationTargetException | NoSuchMethodException ex) {
          e.addSuppressed(ex);
          throw e;
        }
      }
      return modifiersField;
    }
  }
}
