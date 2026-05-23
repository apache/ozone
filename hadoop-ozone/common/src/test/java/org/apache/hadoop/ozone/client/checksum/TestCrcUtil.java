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

package org.apache.hadoop.ozone.client.checksum;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Random;
import java.util.function.LongToIntFunction;
import org.apache.hadoop.util.DataChecksum;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Test {@link CrcUtil}. */
@Timeout(10)
public class TestCrcUtil {
  private static final Random RANDOM = new Random();

  /** Assert that the given throwable contains the given message. */
  public static void assertContains(Throwable t, String message) {
    assertTrue(t.getMessage().contains(message), () -> "Message \"" + message + "\" not found: " + t);
  }

  @Test
  public void testIntSerialization() {
    byte[] bytes = CrcUtil.intToBytes(0xCAFEBEEF);
    assertEquals(0xCAFEBEEF, CrcUtil.readInt(bytes, 0));

    bytes = new byte[8];
    CrcUtil.writeInt(bytes, 0, 0xCAFEBEEF);
    assertEquals(0xCAFEBEEF, CrcUtil.readInt(bytes, 0));
    CrcUtil.writeInt(bytes, 4, 0xABCDABCD);
    assertEquals(0xABCDABCD, CrcUtil.readInt(bytes, 4));

    // Assert big-endian format for general Java consistency.
    assertEquals(0xBEEFABCD, CrcUtil.readInt(bytes, 2));
  }

  @Test
  public void testToSingleCrcStringBadLength() {
    final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> CrcUtil.toSingleCrcString(new byte[8]));
    assertContains(e, "length");
  }

  @Test
  public void testToSingleCrcString() {
    byte[] buf = CrcUtil.intToBytes(0xcafebeef);
    assertEquals("0xcafebeef", CrcUtil.toSingleCrcString(buf));
  }

  @Test
  public void testToMultiCrcStringBadLength() {
    final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> CrcUtil.toMultiCrcString(new byte[6]));
    assertContains(e, "length");
  }

  @Test
  public void testToMultiCrcStringMultipleElements() {
    byte[] buf = new byte[12];
    CrcUtil.writeInt(buf, 0, 0xcafebeef);
    CrcUtil.writeInt(buf, 4, 0xababcccc);
    CrcUtil.writeInt(buf, 8, 0xddddefef);
    assertEquals("[0xcafebeef, 0xababcccc, 0xddddefef]", CrcUtil.toMultiCrcString(buf));
  }

  @Test
  public void testToMultiCrcStringSingleElement() {
    byte[] buf = new byte[4];
    CrcUtil.writeInt(buf, 0, 0xcafebeef);
    assertEquals("[0xcafebeef]", CrcUtil.toMultiCrcString(buf));
  }

  @Test
  public void testToMultiCrcStringNoElements() {
    assertEquals("[]", CrcUtil.toMultiCrcString(new byte[0]));
  }

  @Test
  public void testMultiplyMod() {
    runTestMultiplyMod(10_000_000, DataChecksum.Type.CRC32);
    runTestMultiplyMod(10_000_000, DataChecksum.Type.CRC32C);
  }

  private static long[] runTestMultiplyMod(int n, DataChecksum.Type type) {
    System.out.printf("Run %s with %d computations%n", type, n);
    final int polynomial = getCrcPolynomialForType(type);
    final LongToIntFunction mod = CrcComposer.getModFunction(type);

    final int[] p = new int[n];
    final int[] q = new int[n];
    for (int i = 0; i < n; i++) {
      p[i] = RANDOM.nextInt();
      q[i] = RANDOM.nextInt();
    }

    final int[] expected = new int[n];
    final long[] times = new long[2];
    final long t0 = System.currentTimeMillis();
    for (int i = 0; i < n; i++) {
      expected[i] = galoisFieldMultiply(p[i], q[i], polynomial);
    }
    times[0] = System.currentTimeMillis() - t0;
    final double ops0 = n * 1000.0 / times[0];
    System.out.printf("galoisFieldMultiply: %.3fs (%.2f ops)%n", times[0] / 1000.0, ops0);

    final int[] computed = new int[n];
    final long t1 = System.currentTimeMillis();
    for (int i = 0; i < n; i++) {
      computed[i] = CrcUtil.multiplyMod(p[i], q[i], mod);
    }
    times[1] = System.currentTimeMillis() - t1;
    final double ops1 = n * 1000.0 / times[1];
    System.out.printf("tableMultiply      : %.3fs (%.2f ops)%n", times[1] / 1000.0, ops1);
    System.out.printf("tableMultiply is %.2f%% faster%n", (ops1 - ops0) * 100.0 / ops0);

    for (int i = 0; i < n; i++) {
      if (expected[i] != computed[i]) {
        System.out.printf("expected %08X%n", expected[i]);
        System.out.printf("computed %08X%n", computed[i]);
        throw new IllegalStateException();
      }
    }
    return times;
  }

  /**
   * getCrcPolynomialForType.
   *
   * @param type type.
   * @return the int representation of the polynomial associated with the
   * CRC {@code type}, suitable for use with further CRC arithmetic.
   */
  private static int getCrcPolynomialForType(DataChecksum.Type type) {
    switch (type) {
    case CRC32:
      return org.apache.hadoop.util.CrcUtil.GZIP_POLYNOMIAL;
    case CRC32C:
      return org.apache.hadoop.util.CrcUtil.CASTAGNOLI_POLYNOMIAL;
    default:
      throw new IllegalArgumentException("Unexpected type: " + type);
    }
  }

  /**
   * Galois field multiplication of {@code p} and {@code q} with the
   * generator polynomial {@code m} as the modulus.
   *
   * @param m The little-endian polynomial to use as the modulus when
   *          multiplying p and q, with implicit "1" bit beyond the bottom bit.
   */
  private static int galoisFieldMultiply(int p, int q, int m) {
    int summation = 0;

    // Top bit is the x^0 place; each right-shift increments the degree of the
    // current term.
    int curTerm = org.apache.hadoop.util.CrcUtil.MULTIPLICATIVE_IDENTITY;

    // Iteratively multiply p by x mod m as we go to represent the q[i] term
    // (of degree x^i) times p.
    int px = p;

    while (curTerm != 0) {
      if ((q & curTerm) != 0) {
        summation ^= px;
      }

      // Bottom bit represents highest degree since we're little-endian; before
      // we multiply by "x" for the next term, check bottom bit to know whether
      // the resulting px will thus have a term matching the implicit "1" term
      // of "m" and thus will need to subtract "m" after mutiplying by "x".
      boolean hasMaxDegree = ((px & 1) != 0);
      px >>>= 1;
      if (hasMaxDegree) {
        px ^= m;
      }
      curTerm >>>= 1;
    }
    return summation;
  }

  /** For running benchmarks. */
  public static class Benchmark {
    /**
     * Usages: java {@link Benchmark} [m] [n] [type]
     *      m: the number of iterations
     *      n: the number of multiplication
     *   type: the CRC type, either CRC32 or CRC32C.
     */
    public static void main(String[] args) {
      final int m = args.length >= 1 ? Integer.parseInt(args[0]) : 10;
      final int n = args.length >= 2 ? Integer.parseInt(args[1]) : 100_000_000;
      final DataChecksum.Type type = args.length >= 3 ? DataChecksum.Type.valueOf(args[2])
          : DataChecksum.Type.CRC32;

      final int warmUpIterations = 2;
      System.out.printf("%nStart warming up with %d iterations ...%n", warmUpIterations);
      for (int i = 0; i < 2; i++) {
        runTestMultiplyMod(n, type);
      }

      System.out.printf("%nStart benchmark with %d iterations ...%n", m);
      final long[] times = new long[2];
      for (int i = 0; i < m; i++) {
        System.out.printf("%d) ", i);
        final long[] t = runTestMultiplyMod(n, type);
        times[0] += t[0];
        times[1] += t[1];
      }

      System.out.printf("%nResult) %d x %d computations:%n", m, n);
      final double ops0 = m * n * 1000.0 / times[0];
      System.out.printf("galoisFieldMultiply: %.3fs (%.2f ops)%n", times[0] / 1000.0, ops0);
      final double ops1 = m * n * 1000.0 / times[1];
      System.out.printf("tableMultiply      : %.3fs (%.2f ops)%n", times[1] / 1000.0, ops1);
      System.out.printf("tableMultiply is %.2f%% faster%n", (ops1 - ops0) * 100.0 / ops0);
    }
  }
}
