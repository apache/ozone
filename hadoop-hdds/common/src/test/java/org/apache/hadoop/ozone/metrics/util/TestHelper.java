package org.apache.hadoop.ozone.metrics.util;


import org.apache.hadoop.ozone.metrics.MetricsCollector;
import org.apache.hadoop.ozone.metrics.MetricsInfo;
import org.apache.hadoop.ozone.metrics.MetricsRecordBuilder;
import org.apache.hadoop.ozone.metrics.MetricsSource;
import org.apache.hadoop.ozone.metrics.lib.DefaultMetricsSystem;
import org.apache.hadoop.util.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Iterator;
import java.util.Objects;

import static org.apache.hadoop.ozone.metrics.lib.Interns.info;
import static org.apache.ozone.test.MetricsAsserts.anyInfo;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.atLeast;

/**
 * Contains methods from hadoop library which are necessary for metrics library.
 */
public class TestHelper {

  private static final double EPSILON = 1e-42;

  /**
   * Duplicate of the class {@link org.apache.ozone.test.MetricsAsserts} method due to incompatible types
   * Should be deleted when HDDS-12799 is in progress
   */
  public static MetricsRecordBuilder mockMetricsRecordBuilder() {
    final MetricsCollector mc = mock(MetricsCollector.class);
    MetricsRecordBuilder rb = mock(MetricsRecordBuilder.class, new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) {
        String methodName = invocation.getMethod().getName();
        return methodName.equals("parent") || methodName.equals("endRecord") ?
            mc : invocation.getMock();
      }
    });
    when(mc.addRecord(anyString())).thenReturn(rb);
    when(mc.addRecord((MetricsInfo) anyInfo())).thenReturn(rb);
    return rb;
  }

  /**
   * Call getMetrics on source and get a record builder mock to verify.
   * @param source  the metrics source
   * @param all     if true, return all metrics even if not changed
   * @return the record builder mock to verifyÏ
   */
  public static MetricsRecordBuilder getMetrics(MetricsSource source, boolean all) {
    MetricsRecordBuilder rb = mockMetricsRecordBuilder();
    MetricsCollector mc = rb.parent();
    source.getMetrics(mc, all);
    return rb;
  }

  public static MetricsRecordBuilder getMetrics(String name) {
    return getMetrics(DefaultMetricsSystem.instance().getSource(name));
  }

  public static MetricsRecordBuilder getMetrics(MetricsSource source) {
    return getMetrics(source, true);
  }


  /**
   * Assert an int counter metric as expected.
   * @param name  of the metric
   * @param expected  value of the metric
   * @param rb  the record builder mock used to getMetrics
   */
  public static void assertCounter(String name, int expected, MetricsRecordBuilder rb) {
    assertThat(getIntCounter(name, rb)).as(name)
        .isEqualTo(expected);
  }

  public static int getIntCounter(String name, MetricsRecordBuilder rb) {
    ArgumentCaptor<Integer> captor = ArgumentCaptor.forClass(
        Integer.class);
    verify(rb, atLeast(0)).addCounter(eqName(info(name, "")), captor.capture());
    checkCaptured(captor, name);
    return captor.getValue();
  }

  /**
   * Assert a long counter metric as expected.
   * @param name  of the metric
   * @param expected  value of the metric
   * @param rb  the record builder mock used to getMetrics
   */
  public static void assertCounter(String name, long expected, MetricsRecordBuilder rb) {
    assertThat(getLongCounter(name, rb)).as(name)
        .isEqualTo(expected);
  }

  public static long getLongCounter(String name, MetricsRecordBuilder rb) {
    ArgumentCaptor<Long> captor = ArgumentCaptor.forClass(Long.class);
    verify(rb, atLeast(0)).addCounter(eqName(info(name, "")), captor.capture());
    checkCaptured(captor, name);
    return captor.getValue();
  }

  public static double getDoubleGauge(String name, MetricsRecordBuilder rb) {
    ArgumentCaptor<Double> captor = ArgumentCaptor.forClass(Double.class);
    verify(rb, atLeast(0)).addGauge(eqName(info(name, "")), captor.capture());
    checkCaptured(captor, name);
    return captor.getValue();
  }

  /**
   * MetricInfo with the same name.
   * @param info to match
   * @return <code>null</code>
   */
  public static MetricsInfo eqName(MetricsInfo info) {
    return argThat(new TestHelper.InfoWithSameName(info));
  }

  private static class InfoWithSameName implements ArgumentMatcher<MetricsInfo> {
    private final String expected;

    InfoWithSameName(MetricsInfo info) {
      expected = Objects.requireNonNull(info.name(), "info name");
    }

    @Override
    public boolean matches(MetricsInfo info) {
      return expected.equals(info.name());
    }

    @Override
    public String toString() {
      return "Info with name=" + expected;
    }
  }

  /**
   * Check that this metric was captured exactly once.
   */
  private static void checkCaptured(ArgumentCaptor<?> captor, String name) {
    assertThat(captor.getAllValues()).as(name)
        .hasSize(1);
  }

  /**
   * Assert equivalence for array and iterable
   *
   * @param <T>      the type of the elements
   * @param s        the name/message for the collection
   * @param expected the expected array of elements
   * @param actual   the actual iterable of elements
   */
  public static <T> void assertEquals(String s, T[] expected,
                                      Iterable<T> actual) {
    Iterator<T> it = actual.iterator();
    int i = 0;
    for (; i < expected.length && it.hasNext(); ++i) {
      Assertions.assertEquals(expected[i], it.next(), "Element " + i + " for " + s);
    }
    Assertions.assertTrue(i == expected.length, "Expected more elements");
    Assertions.assertTrue(!it.hasNext(), "Expected less elements");
  }

  /**
   * Assert equality for two iterables
   *
   * @param <T>      the type of the elements
   * @param s
   * @param expected
   * @param actual
   */
  public static <T> void assertEquals(String s, Iterable<T> expected,
                                      Iterable<T> actual) {
    Iterator<T> ite = expected.iterator();
    Iterator<T> ita = actual.iterator();
    int i = 0;
    while (ite.hasNext() && ita.hasNext()) {
      Assertions.assertEquals(ite.next(), ita.next(), "Element " + i + " for " + s);
    }
    Assertions.assertTrue(!ite.hasNext(), "Expected more elements");
    Assertions.assertTrue(!ita.hasNext(), "Expected less elements");
  }


  static final String E_NULL_THROWABLE = "Null Throwable";
  static final String E_NULL_THROWABLE_STRING =
      "Null Throwable.toString() value";
  static final String E_UNEXPECTED_EXCEPTION = "but got unexpected exception";

  /**
   * Assert that an exception's <code>toString()</code> value
   * contained the expected text.
   * @param expectedText expected string
   * @param t thrown exception
   * @throws AssertionError if the expected string is not found
   */
  public static void assertExceptionContains(String expectedText, Throwable t) {
    assertExceptionContains(expectedText, t, "");
  }

  /**
   * Assert that an exception's <code>toString()</code> value
   * contained the expected text.
   * @param expectedText expected string
   * @param t thrown exception
   * @param message any extra text for the string
   * @throws AssertionError if the expected string is not found
   */
  public static void assertExceptionContains(String expectedText,
                                             Throwable t,
                                             String message) {
    String msg = t.toString();
    if (msg == null) {
      throw new AssertionError(E_NULL_THROWABLE_STRING, t);
    }
    if (expectedText != null && !msg.contains(expectedText)) {
      String prefix = org.apache.commons.lang3.StringUtils.isEmpty(message)
          ? "" : (message + ": ");
      throw new AssertionError(
          String.format("%s Expected to find '%s' %s: %s",
              prefix, expectedText, E_UNEXPECTED_EXCEPTION,
              StringUtils.stringifyException(t)),
          t);
    }
  }
}
