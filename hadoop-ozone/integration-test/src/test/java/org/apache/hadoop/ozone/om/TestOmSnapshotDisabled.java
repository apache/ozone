/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.om;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.DBProfile;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.ozone.test.LambdaTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.UUID;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DB_PROFILE;

/**
 * Integration test to verify Ozone snapshot RPCs throw exception when called.
 */
public class TestOmSnapshotDisabled {

  private static MiniOzoneCluster cluster = null;
  private static OzoneClient client;
  private static ObjectStore store;

  @BeforeAll
  @Timeout(60)
  public static void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    String clusterId = UUID.randomUUID().toString();
    String scmId = UUID.randomUUID().toString();
    conf.set(OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT,
        BucketLayout.LEGACY.name());
    conf.setEnum(HDDS_DB_PROFILE, DBProfile.TEST);
    // Disable filesystem snapshot feature for this test
    conf.setBoolean(OMConfigKeys.OZONE_FILESYSTEM_SNAPSHOT_ENABLED_KEY, false);

    cluster = MiniOzoneCluster.newOMHABuilder(conf)
        .setClusterId(clusterId)
        .setScmId(scmId)
        .setOMServiceId("om-service-test1")
        .setNumOfOzoneManagers(3)
        .build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();

    OzoneManager leaderOzoneManager =
        ((MiniOzoneHAClusterImpl) cluster).getOMLeader();
    OzoneConfiguration leaderConfig = leaderOzoneManager.getConfiguration();
    cluster.setConf(leaderConfig);
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
    String volumeName = "vol-" + RandomStringUtils.randomNumeric(5);
    String bucketName = "buck-" + RandomStringUtils.randomNumeric(5);
    String snapshotName = "snap-" + RandomStringUtils.randomNumeric(5);

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);

    // create snapshot should throw
    LambdaTestUtils.intercept(OMException.class, "FEATURE_NOT_ENABLED",
        () -> store.createSnapshot(volumeName, bucketName, snapshotName));
    // delete snapshot should throw
    LambdaTestUtils.intercept(OMException.class, "FEATURE_NOT_ENABLED",
        () -> store.deleteSnapshot(volumeName, bucketName, snapshotName));
  }
}
