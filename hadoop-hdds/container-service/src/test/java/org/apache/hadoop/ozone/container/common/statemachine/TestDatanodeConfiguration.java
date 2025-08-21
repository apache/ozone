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

package org.apache.hadoop.ozone.container.common.statemachine;

import static org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration.BLOCK_DELETE_COMMAND_WORKER_INTERVAL;
import static org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration.BLOCK_DELETE_COMMAND_WORKER_INTERVAL_DEFAULT;
import static org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration.CONTAINER_DELETE_THREADS_DEFAULT;
import static org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration.CONTAINER_DELETE_THREADS_MAX_KEY;
import static org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration.DISK_CHECK_MIN_GAP_DEFAULT;
import static org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration.DISK_CHECK_MIN_GAP_KEY;
import static org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration.DISK_CHECK_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration.DISK_CHECK_TIMEOUT_KEY;
import static org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration.FAILED_DATA_VOLUMES_TOLERATED_KEY;
import static org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration.FAILED_DB_VOLUMES_TOLERATED_KEY;
import static org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration.FAILED_METADATA_VOLUMES_TOLERATED_KEY;
import static org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration.FAILED_VOLUMES_TOLERATED_DEFAULT;
import static org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration.HDDS_DATANODE_VOLUME_MIN_FREE_SPACE_PERCENT_DEFAULT;
import static org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration.PERIODIC_DISK_CHECK_INTERVAL_MINUTES_DEFAULT;
import static org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration.PERIODIC_DISK_CHECK_INTERVAL_MINUTES_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.DatanodeRatisServerConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.container.common.ContainerTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.TimeDuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Test for {@link DatanodeConfiguration}.
 */
public class TestDatanodeConfiguration {

  private static final long[] CAPACITIES = {100_000, 1_000_000, 10_000_000};

  @Test
  public void acceptsValidValues() {
    // GIVEN
    int validDeleteThreads = 42;
    long validDiskCheckIntervalMinutes = 60;
    int validFailedVolumesTolerated = 10;
    long validDiskCheckMinGap = 2;
    long validDiskCheckTimeout = 1;
    long validBlockDeleteCommandWorkerInterval = 1;
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(CONTAINER_DELETE_THREADS_MAX_KEY, validDeleteThreads);
    conf.setLong(PERIODIC_DISK_CHECK_INTERVAL_MINUTES_KEY,
        validDiskCheckIntervalMinutes);
    conf.setInt(FAILED_DATA_VOLUMES_TOLERATED_KEY,
        validFailedVolumesTolerated);
    conf.setInt(FAILED_METADATA_VOLUMES_TOLERATED_KEY,
        validFailedVolumesTolerated);
    conf.setInt(FAILED_DB_VOLUMES_TOLERATED_KEY,
        validFailedVolumesTolerated);
    conf.setTimeDuration(DISK_CHECK_MIN_GAP_KEY,
        validDiskCheckMinGap, TimeUnit.MINUTES);
    conf.setTimeDuration(DISK_CHECK_TIMEOUT_KEY,
        validDiskCheckTimeout, TimeUnit.MINUTES);
    conf.setTimeDuration(BLOCK_DELETE_COMMAND_WORKER_INTERVAL,
        validBlockDeleteCommandWorkerInterval, TimeUnit.SECONDS);

    // WHEN
    DatanodeConfiguration subject = conf.getObject(DatanodeConfiguration.class);

    // THEN
    assertEquals(validDeleteThreads, subject.getContainerDeleteThreads());
    assertEquals(validDiskCheckIntervalMinutes,
        subject.getPeriodicDiskCheckIntervalMinutes());
    assertEquals(validFailedVolumesTolerated,
        subject.getFailedDataVolumesTolerated());
    assertEquals(validFailedVolumesTolerated,
        subject.getFailedMetadataVolumesTolerated());
    assertEquals(validFailedVolumesTolerated,
        subject.getFailedDbVolumesTolerated());
    assertEquals(validDiskCheckMinGap,
        subject.getDiskCheckMinGap().toMinutes());
    assertEquals(validDiskCheckTimeout,
        subject.getDiskCheckTimeout().toMinutes());
    assertEquals(validBlockDeleteCommandWorkerInterval,
        subject.getBlockDeleteCommandWorkerInterval().getSeconds());
  }

  @Test
  public void overridesInvalidValues() {
    // GIVEN
    int invalidDeleteThreads = 0;
    long invalidDiskCheckIntervalMinutes = -1;
    int invalidFailedVolumesTolerated = -2;
    long invalidDiskCheckMinGap = -1;
    long invalidDiskCheckTimeout = -1;
    long invalidBlockDeleteCommandWorkerInterval = -1;
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(CONTAINER_DELETE_THREADS_MAX_KEY, invalidDeleteThreads);
    conf.setLong(PERIODIC_DISK_CHECK_INTERVAL_MINUTES_KEY,
        invalidDiskCheckIntervalMinutes);
    conf.setInt(FAILED_DATA_VOLUMES_TOLERATED_KEY,
        invalidFailedVolumesTolerated);
    conf.setInt(FAILED_METADATA_VOLUMES_TOLERATED_KEY,
        invalidFailedVolumesTolerated);
    conf.setInt(FAILED_DB_VOLUMES_TOLERATED_KEY,
        invalidFailedVolumesTolerated);
    conf.setTimeDuration(DISK_CHECK_MIN_GAP_KEY,
        invalidDiskCheckMinGap, TimeUnit.MINUTES);
    conf.setTimeDuration(DISK_CHECK_TIMEOUT_KEY,
        invalidDiskCheckTimeout, TimeUnit.MINUTES);
    conf.setTimeDuration(BLOCK_DELETE_COMMAND_WORKER_INTERVAL,
        invalidBlockDeleteCommandWorkerInterval, TimeUnit.SECONDS);

    // WHEN
    DatanodeConfiguration subject = conf.getObject(DatanodeConfiguration.class);

    // THEN
    assertEquals(CONTAINER_DELETE_THREADS_DEFAULT,
        subject.getContainerDeleteThreads());
    assertEquals(PERIODIC_DISK_CHECK_INTERVAL_MINUTES_DEFAULT,
        subject.getPeriodicDiskCheckIntervalMinutes());
    assertEquals(FAILED_VOLUMES_TOLERATED_DEFAULT,
        subject.getFailedDataVolumesTolerated());
    assertEquals(FAILED_VOLUMES_TOLERATED_DEFAULT,
        subject.getFailedMetadataVolumesTolerated());
    assertEquals(FAILED_VOLUMES_TOLERATED_DEFAULT,
        subject.getFailedDbVolumesTolerated());
    assertEquals(DISK_CHECK_MIN_GAP_DEFAULT,
        subject.getDiskCheckMinGap());
    assertEquals(DISK_CHECK_TIMEOUT_DEFAULT,
        subject.getDiskCheckTimeout());
    assertEquals(BLOCK_DELETE_COMMAND_WORKER_INTERVAL_DEFAULT,
        subject.getBlockDeleteCommandWorkerInterval());
  }

  @Test
  public void isCreatedWitDefaultValues() {
    // GIVEN
    OzoneConfiguration conf = new OzoneConfiguration();
    // unset over-ridding configuration from ozone-site.xml defined for the test module
    conf.unset(DatanodeConfiguration.HDDS_DATANODE_VOLUME_MIN_FREE_SPACE); // set in ozone-site.xml

    // Capture logs to verify no warnings are generated
    LogCapturer logCapturer = LogCapturer.captureLogs(DatanodeConfiguration.class);

    // WHEN
    DatanodeConfiguration subject = conf.getObject(DatanodeConfiguration.class);

    // THEN
    assertEquals(CONTAINER_DELETE_THREADS_DEFAULT,
        subject.getContainerDeleteThreads());
    assertEquals(PERIODIC_DISK_CHECK_INTERVAL_MINUTES_DEFAULT,
        subject.getPeriodicDiskCheckIntervalMinutes());
    assertEquals(FAILED_VOLUMES_TOLERATED_DEFAULT,
        subject.getFailedDataVolumesTolerated());
    assertEquals(FAILED_VOLUMES_TOLERATED_DEFAULT,
        subject.getFailedMetadataVolumesTolerated());
    assertEquals(FAILED_VOLUMES_TOLERATED_DEFAULT,
        subject.getFailedDbVolumesTolerated());
    assertEquals(DISK_CHECK_MIN_GAP_DEFAULT,
        subject.getDiskCheckMinGap());
    assertEquals(DISK_CHECK_TIMEOUT_DEFAULT,
        subject.getDiskCheckTimeout());
    assertEquals(BLOCK_DELETE_COMMAND_WORKER_INTERVAL_DEFAULT,
        subject.getBlockDeleteCommandWorkerInterval());
    assertEquals(DatanodeConfiguration.getDefaultFreeSpace(), subject.getMinFreeSpace());
    assertEquals(HDDS_DATANODE_VOLUME_MIN_FREE_SPACE_PERCENT_DEFAULT, subject.getMinFreeSpaceRatio());
    final long oneGB = 1024 * 1024 * 1024;
    // capacity is less, consider default min_free_space
    assertEquals(DatanodeConfiguration.getDefaultFreeSpace(), subject.getMinFreeSpace(oneGB));
    // capacity is large, consider min_free_space_percent, max(min_free_space, min_free_space_percent * capacity)ß
    assertEquals(HDDS_DATANODE_VOLUME_MIN_FREE_SPACE_PERCENT_DEFAULT * oneGB * oneGB,
        subject.getMinFreeSpace(oneGB * oneGB));

    // Verify that no warnings were logged when using default values
    String logOutput = logCapturer.getOutput();
    assertThat(logOutput).doesNotContain("is invalid, should be between 0 and 1");
  }

  @Test
  void rejectsInvalidMinFreeSpaceRatio() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setFloat(DatanodeConfiguration.HDDS_DATANODE_VOLUME_MIN_FREE_SPACE_PERCENT, 1.5f);

    DatanodeConfiguration subject = conf.getObject(DatanodeConfiguration.class);

    assertEquals(HDDS_DATANODE_VOLUME_MIN_FREE_SPACE_PERCENT_DEFAULT, subject.getMinFreeSpaceRatio());
  }

  @Test
  void useMaxIfBothMinFreeSpacePropertiesSet() {
    OzoneConfiguration conf = new OzoneConfiguration();
    int minFreeSpace = 10000;
    conf.setLong(DatanodeConfiguration.HDDS_DATANODE_VOLUME_MIN_FREE_SPACE, minFreeSpace);
    conf.setFloat(DatanodeConfiguration.HDDS_DATANODE_VOLUME_MIN_FREE_SPACE_PERCENT, .5f);

    DatanodeConfiguration subject = conf.getObject(DatanodeConfiguration.class);

    assertEquals(minFreeSpace, subject.getMinFreeSpace());
    assertEquals(.5f, subject.getMinFreeSpaceRatio());

    for (long capacity : CAPACITIES) {
      // disk percent is higher than minFreeSpace configured 10000 bytes
      assertEquals((long)(capacity * 0.5f), subject.getMinFreeSpace(capacity));
    }
  }

  @ParameterizedTest
  @ValueSource(longs = {1_000, 10_000, 100_000})
  void usesFixedMinFreeSpace(long bytes) {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setLong(DatanodeConfiguration.HDDS_DATANODE_VOLUME_MIN_FREE_SPACE, bytes);
    // keeping %cent low so that min free space is picked up
    conf.setFloat(DatanodeConfiguration.HDDS_DATANODE_VOLUME_MIN_FREE_SPACE_PERCENT, 0.00001f);

    DatanodeConfiguration subject = conf.getObject(DatanodeConfiguration.class);

    assertEquals(bytes, subject.getMinFreeSpace());

    for (long capacity : CAPACITIES) {
      assertEquals(bytes, subject.getMinFreeSpace(capacity));
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 10, 100})
  void calculatesMinFreeSpaceRatio(int percent) {
    OzoneConfiguration conf = new OzoneConfiguration();
    // keeping min free space low so that %cent is picked up after calculation
    conf.set(DatanodeConfiguration.HDDS_DATANODE_VOLUME_MIN_FREE_SPACE, "1000"); // set in ozone-site.xml
    conf.setFloat(DatanodeConfiguration.HDDS_DATANODE_VOLUME_MIN_FREE_SPACE_PERCENT, percent / 100.0f);

    DatanodeConfiguration subject = conf.getObject(DatanodeConfiguration.class);

    assertEquals(percent / 100.0f, subject.getMinFreeSpaceRatio());
    for (long capacity : CAPACITIES) {
      assertEquals(capacity * percent / 100, subject.getMinFreeSpace(capacity));
    }
  }

  @Test
  public void testConf() throws Exception {
    final OzoneConfiguration conf = new OzoneConfiguration();
    final String dir = "dummy/dir";
    conf.set(OzoneConfigKeys.HDDS_CONTAINER_RATIS_DATANODE_STORAGE_DIR, dir);

    final DatanodeRatisServerConfig ratisConf = conf.getObject(
        DatanodeRatisServerConfig.class);
    assertEquals(0, ratisConf.getLogAppenderWaitTimeMin(),
        "getLogAppenderWaitTimeMin");

    assertWaitTimeMin(TimeDuration.ZERO, conf);
    ratisConf.setLogAppenderWaitTimeMin(1);
    conf.setFromObject(ratisConf);
    assertWaitTimeMin(TimeDuration.ONE_MILLISECOND, conf);
  }

  static void assertWaitTimeMin(TimeDuration expected,
      OzoneConfiguration conf) throws Exception {
    final DatanodeDetails dn = MockPipeline.createPipeline(1).getFirstNode();
    final RaftProperties p = ContainerTestUtils.newXceiverServerRatis(dn, conf)
        .newRaftProperties();
    final TimeDuration t = RaftServerConfigKeys.Log.Appender.waitTimeMin(p);
    assertEquals(expected, t,
        RaftServerConfigKeys.Log.Appender.WAIT_TIME_MIN_KEY);
  }
}
