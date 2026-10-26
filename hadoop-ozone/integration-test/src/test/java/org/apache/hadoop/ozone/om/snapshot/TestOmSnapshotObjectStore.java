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

package org.apache.hadoop.ozone.om.snapshot;

import static org.apache.hadoop.ozone.om.helpers.BucketLayout.OBJECT_STORE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffReportOzone;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.Test;

/**
 * Test OmSnapshot for Object Store bucket type.
 */
public class TestOmSnapshotObjectStore extends OmSnapshotTests {

  public TestOmSnapshotObjectStore() throws Exception {
    super(OBJECT_STORE, false, false, false, false, true);
  }

  @Test
  public void testOptimizedObsDiff() throws Exception {
    String snap1 = "snap1";
    String snap2 = "snap2";
    String key1 = "key1";
    String key2 = "key2";
    String renamedKey = "key3";
    OzoneBucket bucket = getOzoneBucket();
    createFileKey(bucket, key1);
    createFileKey(bucket, key2);
    createSnapshot(getVolumeName(), getBucketName(), snap1);
    bucket.renameKey(key2, renamedKey);
    GenericTestUtils.waitFor(() -> {
      try {
        bucket.getKey(renamedKey);
        return true;
      } catch (IOException e) {
        return false;
      }
    }, 300, 10000);
    bucket.deleteKey(key1);
    createSnapshot(getVolumeName(), getBucketName(), snap2);

    SnapshotDiffResponse response = waitForDoneReportAfterOrderedSubmit(
        getVolumeName(), getBucketName(), snap1, snap2, false);
    assertEquals("Report is dependency ordered.", response.getReason());
    assertThat(response.getSnapshotDiffReport().getDiffList()).containsExactlyInAnyOrder(
        SnapshotDiffReportOzone.getDiffReportEntry(SnapshotDiffReport.DiffType.DELETE, key1),
        SnapshotDiffReportOzone.getDiffReportEntry(SnapshotDiffReport.DiffType.RENAME, key2, renamedKey));
  }
}
