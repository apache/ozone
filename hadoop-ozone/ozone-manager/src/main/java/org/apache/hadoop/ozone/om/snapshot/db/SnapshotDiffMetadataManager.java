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

package org.apache.hadoop.ozone.om.snapshot.db;

import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffReportEntry;
import org.apache.hadoop.ozone.om.helpers.SnapshotDiffJob;
import org.apache.hadoop.ozone.om.snapshot.diff.helper.SnapshotDiffObjectInfo;

/**
 * Interface representing the metadata manager for snapshot difference operations.
 * This interface provides methods to access various tables storing metadata
 * related to snapshot diff jobs, diff reports, purged jobs, and object information
 * for snapshots.
 *
 * Implementations of this interface should handle the setup, maintenance, and
 * querying of the relevant metadata tables.
 *
 * The interface extends {@link AutoCloseable}, requiring implementations to
 * handle proper resource cleanup.
 */
public interface SnapshotDiffMetadataManager extends AutoCloseable {

  Table<String, SnapshotDiffJob> getSnapshotDiffJobTable();

  Table<String, DiffReportEntry> getSnapshotDiffReportTable();

  Table<String, Long> getSnapshotDiffPurgedJobTable();

  Table<String, SnapshotDiffObjectInfo> getSnapshotDiffFromSnapshotObjectInfoTable();

  Table<String, SnapshotDiffObjectInfo> getSnapshotDiffToSnapshotObjectInfoTable();

  Table<String, Boolean> getSnapshotDiffUniqueObjectIdsTable();

}
