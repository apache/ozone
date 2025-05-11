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

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DB_PROFILE;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.FEATURE_NOT_ENABLED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.DBProfile;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Integration test to verify Ozone snapshot RPCs throw exception when called.
 */
public class TestOmSnapshotDisabled {

  private static MiniOzoneHAClusterImpl cluster = null;
  private static OzoneClient client;
  private static ObjectStore store;

  @BeforeAll
  public static void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT,
        BucketLayout.LEGACY.name());
    conf.setEnum(HDDS_DB_PROFILE, DBProfile.TEST);
    // Disable filesystem snapshot feature for this test
    conf.setBoolean(OMConfigKeys.OZONE_FILESYSTEM_SNAPSHOT_ENABLED_KEY, false);

    cluster = MiniOzoneCluster.newHABuilder(conf)
        .setOMServiceId("om-service-test1")
        .setNumOfOzoneManagers(3)
        .build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();

    store = client.getObjectStore();
  }

  @AfterAll
  public static void tearDown() throws Exception {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testExceptionThrown() throws Exception {
    String volumeName = "vol-" + RandomStringUtils.secure().nextNumeric(5);
    String bucketName = "buck-" + RandomStringUtils.secure().nextNumeric(5);
    String snapshotName = "snap-" + RandomStringUtils.secure().nextNumeric(5);

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);

    // create snapshot should throw
    OMException omException = assertThrows(OMException.class,
        () -> store.createSnapshot(volumeName, bucketName, snapshotName));
    assertEquals(FEATURE_NOT_ENABLED, omException.getResult());
    // delete snapshot should throw
    omException = assertThrows(OMException.class,
        () -> store.deleteSnapshot(volumeName, bucketName, snapshotName));
    assertEquals(FEATURE_NOT_ENABLED, omException.getResult());
  }
}
