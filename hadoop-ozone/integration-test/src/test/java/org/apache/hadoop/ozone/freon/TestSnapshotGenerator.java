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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.Iterator;
import java.util.UUID;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneSnapshot;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.ozone.test.NonHATests;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

/**
 * Tests SnapshotGenerator CLI with MiniOzoneCluster.
 * This class uses the same cluster initialization pattern as TestHsyncGenerator.
 */
public abstract class TestSnapshotGenerator implements NonHATests.TestCase {

  @Test
  public void test() throws IOException {
    SnapshotGenerator snapshotGenerator = new SnapshotGenerator(cluster().getConf());
    CommandLine cmd = new CommandLine(snapshotGenerator);

    String volumeName = "vol-" + UUID.randomUUID();
    String bucketName = "bucket1";
    try (OzoneClient client = cluster().newClient()) {
      ObjectStore store = client.getObjectStore();
      store.createVolume(volumeName);
      OzoneVolume volume = store.getVolume(volumeName);
      volume.createBucket(bucketName);

      int exitCode = cmd.execute(
          "--volume", volumeName,
          "--bucket", bucketName,
          "--snapshots", "2",
          "--files", "3",
          "--dirs", "2");
      assertEquals(0, exitCode);

      // verify that two snapshots were created using listSnapshots

      int snapshotCount = 0;
      for (Iterator<OzoneSnapshot> snapshotIter = store.listSnapshot(volumeName, bucketName, null, null);
            snapshotIter.hasNext(); snapshotIter.next()) {
        snapshotCount++;
      }
      assertEquals(2, snapshotCount);
    }
  }
}
