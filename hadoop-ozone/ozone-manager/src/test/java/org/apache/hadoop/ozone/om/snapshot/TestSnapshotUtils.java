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

package org.apache.hadoop.ozone.om.snapshot;

import org.apache.commons.lang3.tuple.Triple;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;

import static org.apache.hadoop.ozone.om.snapshot.SnapshotUtils.getSnapshotTableKeyFromString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for SnapshotUtils.
 */
public class TestSnapshotUtils {

  @Test
  void testGetSnapshotTableKeyFromString() throws IOException {
    String volume = "volumeName";
    String bucket = "bucketName";
    String snapshot = "snapshotName";

    Triple<String, String, String> triple = getSnapshotTableKeyFromString("/volumeName/bucketName/snapshotName");
    assertEquals(volume, triple.getLeft());
    assertEquals(bucket, triple.getMiddle());
    assertEquals(snapshot, triple.getRight());
  }

  @ValueSource(strings = {"", "/volume", "/volume/bucket", "/volume/bucket/dir/snapshot"})
  @ParameterizedTest
  void testGetSnapshotTableKeyFromStringFailure(String snapshotTableKey) {
    assertThrows(IOException.class, () -> getSnapshotTableKeyFromString(snapshotTableKey));
  }
}
