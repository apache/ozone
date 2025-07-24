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

package org.apache.ozone.erasurecode.rawcoder;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test native raw Reed-solomon encoding and decoding.
 */
public class TestNativeRSRawCoder extends TestRSRawCoderBase {

  public TestNativeRSRawCoder() {
    super(NativeRSRawErasureCoderFactory.class,
        NativeRSRawErasureCoderFactory.class);
  }

  @BeforeEach
  public void setup() {
    Assumptions.assumeTrue(ErasureCodeNative.isNativeCodeLoaded());
    setAllowDump(true);
  }

  @Test
  public void testCoding6x3ErasingAllD() {
    prepare(null, 6, 3, new int[]{0, 1, 2}, new int[0], true);
    testCodingDoMixAndTwice();
  }

  @Test
  public void testCoding6x3ErasingD0D2() {
    prepare(null, 6, 3, new int[] {0, 2}, new int[]{});
    testCodingDoMixAndTwice();
  }

  @Test
  public void testCoding6x3ErasingD0() {
    prepare(null, 6, 3, new int[]{0}, new int[0]);
    testCodingDoMixAndTwice();
  }

  @Test
  public void testCoding6x3ErasingD2() {
    prepare(null, 6, 3, new int[]{2}, new int[]{});
    testCodingDoMixAndTwice();
  }

  @Test
  public void testCoding6x3ErasingD0P0() {
    prepare(null, 6, 3, new int[]{0}, new int[]{0});
    testCodingDoMixAndTwice();
  }

  @Test
  public void testCoding6x3ErasingAllP() {
    prepare(null, 6, 3, new int[0], new int[]{0, 1, 2});
    testCodingDoMixAndTwice();
  }

  @Test
  public void testCoding6x3ErasingP0() {
    prepare(null, 6, 3, new int[0], new int[]{0});
    testCodingDoMixAndTwice();
  }

  @Test
  public void testCoding6x3ErasingP2() {
    prepare(null, 6, 3, new int[0], new int[]{2});
    testCodingDoMixAndTwice();
  }

  @Test
  public void testCoding6x3ErasureP0P2() {
    prepare(null, 6, 3, new int[0], new int[]{0, 2});
    testCodingDoMixAndTwice();
  }

  @Test
  public void testCoding6x3ErasingD0P0P1() {
    prepare(null, 6, 3, new int[]{0}, new int[]{0, 1});
    testCodingDoMixAndTwice();
  }

  @Test
  public void testCoding6x3ErasingD0D2P2() {
    prepare(null, 6, 3, new int[]{0, 2}, new int[]{2});
    testCodingDoMixAndTwice();
  }

  @Test
  public void testCodingNegative6x3ErasingD2D4() {
    prepare(null, 6, 3, new int[]{2, 4}, new int[0]);
    testCodingDoMixAndTwice();
  }

  @Test
  public void testCodingNegative6x3ErasingTooMany() {
    prepare(null, 6, 3, new int[]{2, 4}, new int[]{0, 1});
    testCodingWithErasingTooMany();
  }

  @Override
  @Test
  public void testCoding10x4ErasingD0P0() {
    prepare(null, 10, 4, new int[] {0}, new int[] {0});
    testCodingDoMixAndTwice();
  }

  @Test
  public void testAfterRelease63() throws Exception {
    prepare(6, 3, null, null);
    testAfterRelease();
  }
}
