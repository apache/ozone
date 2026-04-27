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

import static org.apache.hadoop.hdds.utils.db.DBStoreBuilder.DEFAULT_COLUMN_FAMILY_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.CodecRegistry;
import org.apache.hadoop.hdds.utils.db.managed.ManagedColumnFamilyOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedDBOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.SnapshotDiffJob;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

/**
 * Tests for SnapshotDiffManagerMXBean registration.
 */
public class TestSnapshotDiffManagerMXBean {

  private SnapshotDiffManager snapshotDiffManager;
  private ManagedRocksDB db;
  private PersistentMap<String, SnapshotDiffJob> snapDiffJobTable;
  private MBeanServer mbs;

  @BeforeEach
  public void setUp(@TempDir Path tempDir) throws IOException, RocksDBException {
    OzoneConfiguration conf = new OzoneConfiguration();
    ManagedDBOptions dbOptions = new ManagedDBOptions();
    dbOptions.setCreateIfMissing(true);
    ManagedColumnFamilyOptions columnFamilyOptions = new ManagedColumnFamilyOptions();

    List<ColumnFamilyDescriptor> columnFamilyDescriptors =
        Collections.singletonList(new ColumnFamilyDescriptor(
            DEFAULT_COLUMN_FAMILY_NAME.getBytes(StandardCharsets.UTF_8),
            columnFamilyOptions));

    List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

    db = ManagedRocksDB.open(dbOptions, tempDir.toAbsolutePath().toString(),
        columnFamilyDescriptors, columnFamilyHandles);

    CodecRegistry codecRegistry = CodecRegistry.newBuilder()
        .addCodec(SnapshotDiffJob.class, SnapshotDiffJob.codec())
        .build();

    ColumnFamilyHandle jobCFH = db.get().createColumnFamily(
        new ColumnFamilyDescriptor("jobTable".getBytes(StandardCharsets.UTF_8),
            columnFamilyOptions));
    ColumnFamilyHandle reportCFH = db.get().createColumnFamily(
        new ColumnFamilyDescriptor("reportTable".getBytes(StandardCharsets.UTF_8),
            columnFamilyOptions));
    ColumnFamilyHandle purgedJobCFH = db.get().createColumnFamily(
        new ColumnFamilyDescriptor("purgedJobTable".getBytes(StandardCharsets.UTF_8),
            columnFamilyOptions));

    snapDiffJobTable = new RocksDbPersistentMap<>(db, jobCFH,
        codecRegistry, String.class, SnapshotDiffJob.class);

    OzoneManager ozoneManager = mock(OzoneManager.class);
    when(ozoneManager.getConfiguration()).thenReturn(conf);
    when(ozoneManager.getMetrics()).thenReturn(mock(OMMetrics.class));
    OMMetadataManager omMetadataManager = mock(OMMetadataManager.class);
    org.apache.hadoop.hdds.utils.db.RDBStore rdbStore =
        mock(org.apache.hadoop.hdds.utils.db.RDBStore.class);
    when(rdbStore.getSnapshotMetadataDir())
        .thenReturn(tempDir.toAbsolutePath().toString());
    when(omMetadataManager.getStore()).thenReturn(rdbStore);
    when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);
    when(ozoneManager.getOmSnapshotManager()).thenReturn(mock(OmSnapshotManager.class));

    snapshotDiffManager = new SnapshotDiffManager(db, ozoneManager,
        jobCFH, reportCFH, purgedJobCFH, columnFamilyOptions, codecRegistry);

    mbs = ManagementFactory.getPlatformMBeanServer();
  }

  @AfterEach
  public void tearDown() {
    if (snapshotDiffManager != null) {
      snapshotDiffManager.close();
    }
    if (db != null) {
      db.close();
    }
  }

  @Test
  public void testMXBeanRegistration() throws Exception {
    ObjectName name = new ObjectName(
        "Hadoop:service=OzoneManager,name=SnapshotDiffManager");
    assertTrue(mbs.isRegistered(name), "SnapshotDiffManager MBean should be registered");

    // Initially 0 jobs
    Object jobsAttr = mbs.getAttribute(name, "SnapshotDiffJobs");
    assertTrue(jobsAttr instanceof CompositeData[]);
    assertEquals(0, ((CompositeData[]) jobsAttr).length);

    // Add a job directly to the table
    SnapshotDiffJob job = new SnapshotDiffJob(System.currentTimeMillis(), "job-1",
        org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus.QUEUED,
        "vol", "bucket", "snap1", "snap2",
        false, false, 0,
        org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.SubStatus.OBJECT_ID_MAP_GEN_FSO,
        0, null);
    snapDiffJobTable.put("job-1", job);

    // Verify MXBean method returns the job via JMX
    jobsAttr = mbs.getAttribute(name, "SnapshotDiffJobs");
    assertTrue(jobsAttr instanceof CompositeData[]);
    CompositeData[] jobs = (CompositeData[]) jobsAttr;
    assertEquals(1, jobs.length);
    assertEquals("job-1", jobs[0].get("jobId"));
    assertEquals("snap1", jobs[0].get("fromSnapshot"));
    assertEquals("snap2", jobs[0].get("toSnapshot"));
    assertEquals("OBJECT_ID_MAP_GEN_FSO", jobs[0].get("subStatus"));
  }
}
