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

package org.apache.hadoop.ozone.container.ozoneimpl;

import static org.apache.hadoop.ozone.container.common.ContainerTestUtils.getMetadataScanError;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.Test;

class TestMetadataScanResult {
  @Test
  void testFromEmptyErrors() {
    // No errors means the scan result is healthy.
    MetadataScanResult result = MetadataScanResult.fromErrors(Collections.emptyList());
    assertFalse(result.hasErrors());
    assertFalse(result.isDeleted());
    assertTrue(result.getErrors().isEmpty());
    assertTrue(result.toString().contains("0 errors"));
  }

  @Test
  void testFromErrors() {
    MetadataScanResult result =
        MetadataScanResult.fromErrors(Arrays.asList(getMetadataScanError(), getMetadataScanError()));
    assertTrue(result.hasErrors());
    assertFalse(result.isDeleted());
    assertEquals(2, result.getErrors().size());
    assertTrue(result.toString().contains("2 errors"));
  }

  @Test
  void testDeleted() {
    MetadataScanResult result = MetadataScanResult.deleted();
    assertFalse(result.hasErrors());
    assertTrue(result.isDeleted());
    assertTrue(result.getErrors().isEmpty());
    assertTrue(result.toString().contains("deleted"));
  }
}
