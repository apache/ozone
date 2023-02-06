/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.container.common.statemachine;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration.CONTAINER_DELETE_THREADS_DEFAULT;
import static org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration.CONTAINER_DELETE_THREADS_MAX_KEY;
import static org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration.DISK_CHECK_MIN_GAP_KEY;
import static org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration.DISK_CHECK_MIN_GAP_DEFAULT;
import static org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration.DISK_CHECK_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration.DISK_CHECK_TIMEOUT_KEY;
import static org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration.FAILED_DB_VOLUMES_TOLERATED_KEY;
import static org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration.PERIODIC_DISK_CHECK_INTERVAL_MINUTES_KEY;
import static org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration.PERIODIC_DISK_CHECK_INTERVAL_MINUTES_DEFAULT;
import static org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration.FAILED_DATA_VOLUMES_TOLERATED_KEY;
import static org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration.FAILED_METADATA_VOLUMES_TOLERATED_KEY;
import static org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration.FAILED_VOLUMES_TOLERATED_DEFAULT;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test for {@link DatanodeConfiguration}.
 */
public class TestDatanodeConfiguration {

  @Test
  public void acceptsValidValues() {
    // GIVEN
    int validDeleteThreads = 42;
    long validDiskCheckIntervalMinutes = 60;
    int validFailedVolumesTolerated = 10;
    long validDiskCheckMinGap = 2;
    long validDiskCheckTimeout = 1;
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
  }

  @Test
  public void overridesInvalidValues() {
    // GIVEN
    int invalidDeleteThreads = 0;
    long invalidDiskCheckIntervalMinutes = -1;
    int invalidFailedVolumesTolerated = -2;
    long invalidDiskCheckMinGap = -1;
    long invalidDiskCheckTimeout = -1;
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
        subject.getDiskCheckMinGap().toMillis());
    assertEquals(DISK_CHECK_TIMEOUT_DEFAULT,
        subject.getDiskCheckTimeout().toMillis());
  }

  @Test
  public void isCreatedWitDefaultValues() {
    // GIVEN
    OzoneConfiguration conf = new OzoneConfiguration();

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
        subject.getDiskCheckMinGap().toMillis());
    assertEquals(DISK_CHECK_TIMEOUT_DEFAULT,
        subject.getDiskCheckTimeout().toMillis());
  }

}
