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

import static org.apache.hadoop.hdds.utils.NativeConstants.ROCKS_TOOLS_NATIVE_PROPERTY;
import static org.apache.hadoop.ozone.om.helpers.BucketLayout.FILE_SYSTEM_OPTIMIZED;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffReportEntry;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffReportOzone;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * Test OmSnapshot for FSO bucket type when native lib is enabled.
 */
@EnabledIfSystemProperty(named = ROCKS_TOOLS_NATIVE_PROPERTY, matches = "true")
class TestOmSnapshotFsoWithNativeLib extends OmSnapshotTests {
  TestOmSnapshotFsoWithNativeLib() throws Exception {
    super(FILE_SYSTEM_OPTIMIZED, false, false, false, false, true);
  }

  @Test
  void testOptimizedDiffUnorderedThenOrdered() throws Exception {
    String snap1 = "snap1";
    String snap2 = "snap2";
    String rootPath = String.format("%s://%s.%s/",
        OzoneConsts.OZONE_URI_SCHEME, getBucketName(), getVolumeName());
    try (FileSystem fs = FileSystem.get(new URI(rootPath), getCluster().getConf())) {
      createFileKey(fs, "/dir1/dir2/key1");
      createFileKey(fs, "/dir1/dir3/key1");
      createSnapshot(getVolumeName(), getBucketName(), snap1);
      fs.rename(new Path("/dir1/dir2/key1"), new Path("/dir1/dir3/renamed_key"));
      fs.delete(new Path("/dir1/dir2"), true);
      createSnapshot(getVolumeName(), getBucketName(), snap2);
    }

    List<DiffReportEntry> expectedUnordered = Arrays.asList(
        SnapshotDiffReportOzone.getDiffReportEntry(SnapshotDiffReport.DiffType.DELETE, "dir1/dir2"),
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.RENAME, "dir1/dir2/key1", "dir1/dir3/renamed_key"));

    List<DiffReportEntry> expectedOrdered = Arrays.asList(
        SnapshotDiffReportOzone.getDiffReportEntry(
            SnapshotDiffReport.DiffType.RENAME, "dir1/dir2/key1", "dir1/dir3/renamed_key"),
        SnapshotDiffReportOzone.getDiffReportEntry(SnapshotDiffReport.DiffType.DELETE, "dir1/dir2"));

    SnapshotDiffResponse unordered = waitForDoneReportAfterOrderedSubmit(
        getVolumeName(), getBucketName(), snap1, snap2, false);
    assertEquals("Report is dependency unordered.", unordered.getReason());
    assertEquals(expectedUnordered, unordered.getSnapshotDiffReport().getDiffList());

    SnapshotDiffResponse ordered = waitForDoneReportAfterOrderedSubmit(
        getVolumeName(), getBucketName(), snap1, snap2, true);
    assertEquals("Report is dependency ordered.", ordered.getReason());
    assertEquals(expectedOrdered, ordered.getSnapshotDiffReport().getDiffList());
  }
}
