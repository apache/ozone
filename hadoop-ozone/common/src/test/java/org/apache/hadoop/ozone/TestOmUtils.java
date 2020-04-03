/**
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

package org.apache.hadoop.ozone;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.io.IOUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * Unit tests for {@link OmUtils}.
 */
public class TestOmUtils {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Rule
  public Timeout timeout = new Timeout(60_000);

  @Test
  public void testWriteCheckpointToOutputStream() throws Exception {

    FileInputStream fis = null;
    FileOutputStream fos = null;

    try {
      String testDirName = folder.newFolder().getAbsolutePath();
      File file = new File(testDirName + "/temp1.txt");
      FileWriter writer = new FileWriter(file);
      writer.write("Test data 1");
      writer.close();

      file = new File(testDirName + "/temp2.txt");
      writer = new FileWriter(file);
      writer.write("Test data 2");
      writer.close();

      File outputFile =
          new File(Paths.get(testDirName, "output_file.tgz").toString());
      TestDBCheckpoint dbCheckpoint = new TestDBCheckpoint(
          Paths.get(testDirName));
      OmUtils.writeOmDBCheckpointToStream(dbCheckpoint,
          new FileOutputStream(outputFile));
      assertNotNull(outputFile);
    } finally {
      IOUtils.closeStream(fis);
      IOUtils.closeStream(fos);
    }
  }

  @Test
  public void createOMDirCreatesDirectoryIfNecessary() throws IOException {
    File parent = folder.newFolder();
    File omDir = new File(new File(parent, "sub"), "dir");
    assertFalse(omDir.exists());

    OmUtils.createOMDir(omDir.getAbsolutePath());

    assertTrue(omDir.exists());
  }

  @Test
  public void createOMDirDoesNotThrowIfAlreadyExists() throws IOException {
    File omDir = folder.newFolder();
    assertTrue(omDir.exists());

    OmUtils.createOMDir(omDir.getAbsolutePath());

    assertTrue(omDir.exists());
  }

  @Test(expected = IllegalArgumentException.class)
  public void createOMDirThrowsIfCannotCreate() throws IOException {
    File parent = folder.newFolder();
    File omDir = new File(new File(parent, "sub"), "dir");
    assumeTrue(parent.setWritable(false, false));

    OmUtils.createOMDir(omDir.getAbsolutePath());

    // expecting exception
  }

  @Test
  public void testGetOmHAAddressesById() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_OM_SERVICE_IDS_KEY, "ozone1");
    conf.set("ozone.om.nodes.ozone1", "node1,node2,node3");
    conf.set("ozone.om.address.ozone1.node1", "1.1.1.1");
    conf.set("ozone.om.address.ozone1.node2", "1.1.1.2");
    conf.set("ozone.om.address.ozone1.node3", "1.1.1.3");
    Map<String, List<InetSocketAddress>> addresses =
        OmUtils.getOmHAAddressesById(conf);
    assertFalse(addresses.isEmpty());
    List<InetSocketAddress> rpcAddrs = addresses.get("ozone1");
    assertFalse(rpcAddrs.isEmpty());
    assertTrue(rpcAddrs.stream().anyMatch(
        a -> a.getAddress().getHostAddress().equals("1.1.1.1")));
    assertTrue(rpcAddrs.stream().anyMatch(
        a -> a.getAddress().getHostAddress().equals("1.1.1.2")));
    assertTrue(rpcAddrs.stream().anyMatch(
        a -> a.getAddress().getHostAddress().equals("1.1.1.3")));
  }
}

class TestDBCheckpoint implements DBCheckpoint {

  private Path checkpointFile;

  TestDBCheckpoint(Path checkpointFile) {
    this.checkpointFile = checkpointFile;
  }

  @Override
  public Path getCheckpointLocation() {
    return checkpointFile;
  }

  @Override
  public long getCheckpointTimestamp() {
    return 0;
  }

  @Override
  public long getLatestSequenceNumber() {
    return 0;
  }

  @Override
  public long checkpointCreationTimeTaken() {
    return 0;
  }

  @Override
  public void cleanupCheckpoint() throws IOException {
    FileUtils.deleteDirectory(checkpointFile.toFile());
  }

  @Override
  public void setRatisSnapshotIndex(long omRatisSnapshotIndex) {
  }

  @Override
  public long getRatisSnapshotIndex() {
    return 0;
  }

  @Override
  public void setRatisSnapshotTerm(long omRatisSnapshotTermIndex) {
  }

  @Override
  public long getRatisSnapshotTerm() {
    return 0;
  }
}
