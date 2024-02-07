/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ozone.rocksdiff;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Class to test RocksDiffUtils.
 */
public class TestRocksDiffUtils {
  @Test
  public void testFilterFunction() {
    assertTrue(RocksDiffUtils.isKeyWithPrefixPresent(
        "/vol1/bucket1/",
        "/vol1/bucket1/key1",
        "/vol1/bucket1/key1"));
    assertTrue(RocksDiffUtils.isKeyWithPrefixPresent(
        "/vol1/bucket3/",
        "/vol1/bucket1/key1",
        "/vol1/bucket5/key1"));
    assertFalse(RocksDiffUtils.isKeyWithPrefixPresent(
        "/vol1/bucket5/",
        "/vol1/bucket1/key1",
        "/vol1/bucket4/key9"));
    assertFalse(RocksDiffUtils.isKeyWithPrefixPresent(
        "/vol1/bucket2/",
        "/vol1/bucket1/key1",
        "/vol1/bucket1/key1"));
    assertFalse(RocksDiffUtils.isKeyWithPrefixPresent(
        "/vol1/bucket/",
        "/vol1/bucket1/key1",
        "/vol1/bucket1/key1"));
    assertTrue(RocksDiffUtils.isKeyWithPrefixPresent(
        "/volume/bucket/",
        "/volume/bucket/key-1",
        "/volume/bucket2/key-97"));
  }
}
