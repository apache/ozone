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
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.dn;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.debug.DatanodeLayout;
import org.junit.Before;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.client.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.client.ReplicationType.RATIS;

/**
 * Test Datanode Layout Upgrade Tool.
 */
public class TestDatanodeLayoutUpgradeTool {
  private MiniOzoneCluster cluster = null;

  @Before
  public void setup() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3).setTotalPipelineNumLimit(2).build();
    cluster.waitForClusterToBeReady();
  }

  @After
  public void destroy() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private void writeData() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    Instant testStartTime = Instant.now();

    ObjectStore store = cluster.getClient().getObjectStore();

    String value = "sample value";
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    for (int i = 0; i < 10; i++) {
      String keyName = UUID.randomUUID().toString();

      OzoneOutputStream out = bucket.createKey(keyName,
          value.getBytes(UTF_8).length, RATIS,
          THREE, new HashMap<>());
      out.write(value.getBytes(UTF_8));
      out.close();
      OzoneKey key = bucket.getKey(keyName);
      Assert.assertEquals(keyName, key.getName());
      OzoneInputStream is = bucket.readKey(keyName);
      byte[] fileContent = new byte[value.getBytes(UTF_8).length];
      is.read(fileContent);
      Assert.assertEquals(value, new String(fileContent, UTF_8));
      Assert.assertFalse(key.getCreationTime().isBefore(testStartTime));
      Assert.assertFalse(key.getModificationTime().isBefore(testStartTime));
    }

    // wait for the container report to propagate to SCM
    Thread.sleep(5000);
  }

  @Test
  public void testDatanodeLayoutVerify() throws Exception {
    writeData();
    cluster.stop();

    List<HddsDatanodeService> dns = cluster.getHddsDatanodes();
    OzoneConfiguration c1 = dns.get(0).getConf();
    Collection<String> paths = MutableVolumeSet.getDatanodeStorageDirs(c1);

    for (String p : paths) {
      // Verify that tool is able to verify the storage path
      List<HddsVolume> volumes = DatanodeLayout.runUpgrade(c1, p, true);
      Assert.assertEquals(0, volumes.size());

      HddsVolume.Builder volumeBuilder = new HddsVolume.Builder(p)
          .conf(c1);
      HddsVolume vol = volumeBuilder.build();

      // Rename the path and verify that the tool fails
      File clusterDir = new File(vol.getHddsRootDir(), vol.getClusterID());
      File renamePath = new File(vol.getHddsRootDir(),
          UUID.randomUUID().toString());
      Assert.assertTrue(clusterDir.renameTo(renamePath));

      List<HddsVolume> failedVols = DatanodeLayout.runUpgrade(c1, p, true);
      Assert.assertEquals(1, failedVols.size());
    }
  }
}
