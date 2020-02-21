/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.scm.pipeline;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.SCMPipelineMetrics;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.UUID;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_PIPELINE_AUTO_CREATE_FACTOR_ONE;
import static org.apache.hadoop.test.MetricsAsserts.getLongCounter;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;

/**
 * Test cases to verify the SCM pipeline bytesWritten metrics.
 */
public class TestSCMPipelineBytesWrittenMetrics {

  private MiniOzoneCluster cluster;
  private OzoneConfiguration conf;

  @Before
  public void setup() throws Exception {
    conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.HDDS_SCM_SAFEMODE_PIPELINE_AVAILABILITY_CHECK,
        Boolean.TRUE.toString());
    conf.setBoolean(OZONE_SCM_PIPELINE_AUTO_CREATE_FACTOR_ONE, false);
    conf.setInt(OZONE_DATANODE_PIPELINE_LIMIT, 1);

    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .build();
    cluster.waitForClusterToBeReady();
  }

  private void writeNumBytes(int numBytes) throws Exception {
    ObjectStore store = OzoneClientFactory.getRpcClient(conf).getObjectStore();

    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    String value = RandomStringUtils.randomAlphabetic(numBytes);
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    String keyName = UUID.randomUUID().toString();

    OzoneOutputStream out = bucket
        .createKey(keyName, value.getBytes().length, ReplicationType.RATIS,
            ReplicationFactor.THREE, new HashMap<>());
    out.write(value.getBytes());
    out.close();

    OmKeyArgs.Builder builder = new OmKeyArgs.Builder();
    builder.setVolumeName(volumeName).setBucketName(bucketName)
        .setKeyName(keyName).setRefreshPipeline(true);

    OzoneKeyDetails keyDetails = bucket.getKey(keyName);
    Assert.assertEquals(keyName, keyDetails.getName());
    Assert.assertEquals(value.getBytes().length, keyDetails
        .getOzoneKeyLocations().get(0).getLength());
  }

  @Test
  public void testNumBytesWritten() throws Exception {
    MetricsRecordBuilder metrics =
        getMetrics(SCMPipelineMetrics.class.getSimpleName());
    for (Pipeline pipeline : cluster.getStorageContainerManager()
        .getPipelineManager().getPipelines()) {
      Assert.assertEquals(0L, getLongCounter(
          SCMPipelineMetrics.getBytesWrittenMetricName(pipeline), metrics));
    }
    int bytesWritten = 1000;
    writeNumBytes(bytesWritten);
    Thread.sleep(100 * 1000L);
    metrics =
        getMetrics(SCMPipelineMetrics.class.getSimpleName());
    for (Pipeline pipeline : cluster.getStorageContainerManager()
        .getPipelineManager().getPipelines()) {
      Assert.assertEquals(bytesWritten, getLongCounter(
          SCMPipelineMetrics.getBytesWrittenMetricName(pipeline), metrics));
    }
  }

  @After
  public void teardown() {
    cluster.shutdown();
  }
}
