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

package org.apache.hadoop.ozone.recon;

import java.util.Objects;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for the Storage Distribution REST endpoint using
 * EC (Erasure Coding) replication. The cluster is started with 5 datanodes
 * to satisfy the EC (3,2) placement requirements.
 *
 * <p>Common infrastructure and verification helpers are provided by
 * {@link AbstractTestStorageDistributionEndpoint}.
 */
public class TestStorageDistributionEndpointEC extends AbstractTestStorageDistributionEndpoint {

  private static final int NUM_DATANODES = 5;

  @BeforeAll
  public static void setup() throws Exception {
    initializeCluster(NUM_DATANODES);
  }

  @Override
  protected int getNumDatanodes() {
    return NUM_DATANODES;
  }

  @Test
  public void testStorageDistributionEndpoint() throws Exception {
    ReplicationConfig replicationConfig = new ECReplicationConfig(3, 2);

    OzoneBucket bucket = createVolumeAndBucket(replicationConfig);
    createOpenKeysAndMultipartKeys(bucket.getVolumeName(), bucket.getName(), replicationConfig);

    String rootPath = String.format("%s://%s.%s/",
        OzoneConsts.OZONE_URI_SCHEME, bucket.getName(), bucket.getVolumeName());
    getConf().set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);
    setFs(FileSystem.get(getConf()));

    Path dir1 = new Path("/dir1");
    getFs().mkdirs(dir1);
    for (int i = 1; i <= 10; i++) {
      try (FSDataOutputStream stream = getFs().create(new Path(dir1, "testKey" + i))) {
        stream.write(new byte[10]);
      }
    }
    Path dir2 = new Path("/dir2");
    getFs().mkdirs(dir2);
    for (int i = 1; i <= 10; i++) {
      try (FSDataOutputStream stream = getFs().create(new Path(dir2, "testKey" + i))) {
        stream.write(new byte[10]);
      }
    }

    GenericTestUtils.waitFor(this::verifyStorageDistributionAfterKeyCreation, 1000, 60000);
    closeAllContainers();
    getFs().delete(dir1, true);
    GenericTestUtils.waitFor(this::verifyPendingDeletionAfterKeyDeletionOm, 1000, 30000);
    GenericTestUtils.waitFor(this::verifyPendingDeletionAfterKeyDeletionScm, 2000, 30000);
    GenericTestUtils.waitFor(() -> Objects.requireNonNull(
            getScm().getClientProtocolServer().getDeletedBlockSummary()).getTotalBlockCount() == 0,
        1000, 30000);
    GenericTestUtils.waitFor(this::verifyPendingDeletionAfterKeyDeletionDn, 2000, 60000);
    GenericTestUtils.waitFor(this::verifyPendingDeletionClearsAtDn, 2000, 60000);
  }
}
