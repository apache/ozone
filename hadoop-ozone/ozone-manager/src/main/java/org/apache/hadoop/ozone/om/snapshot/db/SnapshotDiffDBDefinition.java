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

import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_DIFF_DB_NAME;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DIFF_DB_DIR;
import static org.apache.hadoop.ozone.snapshot.SnapshotDiffReportOzone.getDiffReportEntryCodec;

import java.util.Map;
import org.apache.hadoop.hdds.utils.db.BooleanCodec;
import org.apache.hadoop.hdds.utils.db.DBColumnFamilyDefinition;
import org.apache.hadoop.hdds.utils.db.DBDefinition;
import org.apache.hadoop.hdds.utils.db.LongCodec;
import org.apache.hadoop.hdds.utils.db.StringCodec;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffReportEntry;
import org.apache.hadoop.ozone.om.helpers.SnapshotDiffJob;
import org.apache.hadoop.ozone.om.snapshot.diff.helper.SnapshotDiffObjectInfo;

/**
 * The SnapshotDiffDBDefinition class defines the schema for the snapshot
 * difference database tables used in snapshot diff operations. Each table
 * corresponds to a specific aspect of snapshot diff job processing and data
 * storage, ensuring proper organization and efficient access.
 */
public final class SnapshotDiffDBDefinition extends DBDefinition.WithMap {
  /**
   * Contains all the snap diff job which are either queued, in_progress or
   * done. This table is used to make sure that there is only single job for
   * requests with the same snapshot pair at any point of time.
   * |------------------------------------------------|
   * |  KEY                         |  VALUE          |
   * |------------------------------------------------|
   * |  fromSnapshotId-toSnapshotId | SnapshotDiffJob |
   * |------------------------------------------------|
   */
  public static final String SNAP_DIFF_JOB_TABLE_NAME = "snap-diff-job-table";

  public static final DBColumnFamilyDefinition<String, SnapshotDiffJob> SNAP_DIFF_JOB_TABLE_DEF
      = new DBColumnFamilyDefinition<>(SNAP_DIFF_JOB_TABLE_NAME, StringCodec.get(), SnapshotDiffJob.getCodec());
  /**
   * Global table to keep the diff report. Each key is prefixed by the jobId
   * to improve look up and clean up. JobId comes from snap-diff-job-table.
   * |--------------------------------|
   * |  KEY         |  VALUE          |
   * |--------------------------------|
   * |  jobId-index | DiffReportEntry |
   * |--------------------------------|
   */
  public static final String SNAP_DIFF_REPORT_TABLE_NAME = "snap-diff-report-table";
  public static final DBColumnFamilyDefinition<String, DiffReportEntry> SNAP_DIFF_REPORT_TABLE_NAME_DEF
      = new DBColumnFamilyDefinition<>(SNAP_DIFF_REPORT_TABLE_NAME, StringCodec.get(), getDiffReportEntryCodec());
  /**
   * Contains all the snap diff job which can be purged either due to max
   * allowed time is over, FAILED or REJECTED.
   * |-------------------------------------------|
   * |  KEY     |  VALUE                         |
   * |-------------------------------------------|
   * |  jobId   | numOfTotalEntriesInReportTable |
   * |-------------------------------------------|
   */
  public static final String SNAP_DIFF_PURGED_JOB_TABLE_NAME = "snap-diff-purged-job-table";
  public static final DBColumnFamilyDefinition<String, Long> SNAP_DIFF_PURGED_JOB_TABLE_DEF
      = new DBColumnFamilyDefinition<>(SNAP_DIFF_PURGED_JOB_TABLE_NAME, StringCodec.get(), LongCodec.get());
  /**
   * Contains all the snap diff job intermediate object output for from snapshot.
   * |----------------------------------------------------|
   * |  KEY               |  VALUE                        |
   * |----------------------------------------------------|
   * |  jobId-objectId   | SnapshotDiffObjectInfo         |
   * |----------------------------------------------------|
   */
  public static final String SNAP_DIFF_FROM_SNAP_OBJECT_TABLE_NAME = "-from-snap";
  public static final DBColumnFamilyDefinition<String, SnapshotDiffObjectInfo> SNAP_DIFF_FROM_SNAP_OBJECT_TABLE_DEF
      = new DBColumnFamilyDefinition<>(SNAP_DIFF_FROM_SNAP_OBJECT_TABLE_NAME, StringCodec.get(),
      SnapshotDiffObjectInfo.getCodec());

  /**
   * Contains all the snap diff job intermediate object output for to snapshot.
   * |----------------------------------------------------|
   * |  KEY               |  VALUE                        |
   * |----------------------------------------------------|
   * |  jobId-objectId   | SnapshotDiffObjectInfo         |
   * |----------------------------------------------------|
   */
  public static final String SNAP_DIFF_TO_SNAP_OBJECT_TABLE_NAME = "-to-snap";
  public static final DBColumnFamilyDefinition<String, SnapshotDiffObjectInfo> SNAP_DIFF_TO_SNAP_OBJECT_TABLE_DEF
      = new DBColumnFamilyDefinition<>(SNAP_DIFF_TO_SNAP_OBJECT_TABLE_NAME, StringCodec.get(),
      SnapshotDiffObjectInfo.getCodec());

  /**
   * Contains all the snap diff job intermediate object output for to snapshot.
   * |----------------------------------------------------|
   * |  KEY               |  VALUE                        |
   * |----------------------------------------------------|
   * |  jobId-objectId   | boolean                        |
   * |----------------------------------------------------|
   */
  public static final String SNAP_DIFF_UNIQUE_IDS_TABLE_NAME = "-unique-ids";
  public static final DBColumnFamilyDefinition<String, Boolean> SNAP_DIFF_UNIQUE_IDS_TABLE_DEF
      = new DBColumnFamilyDefinition<>(SNAP_DIFF_UNIQUE_IDS_TABLE_NAME, StringCodec.get(), BooleanCodec.get());

  //---------------------------------------------------------------------------//
  private static final Map<String, DBColumnFamilyDefinition<?, ?>> COLUMN_FAMILIES =
      DBColumnFamilyDefinition.newUnmodifiableMap(SNAP_DIFF_JOB_TABLE_DEF,
          SNAP_DIFF_REPORT_TABLE_NAME_DEF,
          SNAP_DIFF_PURGED_JOB_TABLE_DEF,
          SNAP_DIFF_FROM_SNAP_OBJECT_TABLE_DEF,
          SNAP_DIFF_TO_SNAP_OBJECT_TABLE_DEF,
          SNAP_DIFF_UNIQUE_IDS_TABLE_DEF);

  private static final SnapshotDiffDBDefinition INSTANCE = new SnapshotDiffDBDefinition();

  public static SnapshotDiffDBDefinition get() {
    return INSTANCE;
  }

  private SnapshotDiffDBDefinition() {
    super(COLUMN_FAMILIES);
  }

  @Override
  public String getName() {
    return OM_SNAPSHOT_DIFF_DB_NAME;
  }

  @Override
  public String getLocationConfigKey() {
    return OZONE_OM_SNAPSHOT_DIFF_DB_DIR;
  }
}
