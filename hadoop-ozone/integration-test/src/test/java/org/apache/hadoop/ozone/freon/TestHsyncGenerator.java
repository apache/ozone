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

package org.apache.hadoop.ozone.freon;

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OFS_URI_SCHEME;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.UUID;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.ozone.test.NonHATests;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

/**
 * Tests Freon HsyncGenerator with MiniOzoneCluster and validate data.
 */
public abstract class TestHsyncGenerator implements NonHATests.TestCase {

  @Test
  public void test() throws IOException {
    HsyncGenerator randomKeyGenerator =
        new HsyncGenerator(cluster().getConf());
    CommandLine cmd = new CommandLine(randomKeyGenerator);

    String volumeName = "vol-" + UUID.randomUUID();
    String bucketName = "bucket1";
    try (OzoneClient client = cluster().newClient()) {
      ObjectStore store = client.getObjectStore();
      store.createVolume(volumeName);
      OzoneVolume volume = store.getVolume(volumeName);
      volume.createBucket(bucketName);

      String rootPath = String.format("%s://%s/%s/%s/", OZONE_OFS_URI_SCHEME,
          cluster().getConf().get(OZONE_OM_ADDRESS_KEY), volumeName, bucketName);

      int exitCode = cmd.execute(
          "--path", rootPath,
          "--bytes-per-write", "8",
          "--writes-per-transaction", "64",
          "-t", "5",
          "-n", "100");
      assertEquals(0, exitCode);
    }
  }
}
