/*
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
package org.apache.hadoop.ozone.debug;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import picocli.CommandLine;

import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OFS_URI_SCHEME;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test cases for LeaseRecoverer.
 */
public class TestLeaseRecoverer {
  private static MiniOzoneCluster cluster = null;
  private static OzoneConfiguration conf;
  private static OzoneBucket fsoOzoneBucket;
  private static OzoneClient client;

  @Rule
  public Timeout timeout = new Timeout(120000);

  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   *
   * @throws IOException
   */
  @BeforeClass
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    String clusterId = UUID.randomUUID().toString();
    String scmId = UUID.randomUUID().toString();
    String omId = UUID.randomUUID().toString();
    // Set the number of keys to be processed during batch operate.
    cluster = MiniOzoneCluster.newBuilder(conf).setClusterId(clusterId)
        .setScmId(scmId).setOmId(omId).build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();

    // create a volume and a FSO bucket
    fsoOzoneBucket = TestDataUtil
        .createVolumeAndBucket(client, BucketLayout.FILE_SYSTEM_OPTIMIZED);
  }

  @AfterClass
  public static void teardownClass() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testCLI() throws IOException {
    final String rootPath = String.format("%s://%s/",
        OZONE_OFS_URI_SCHEME, conf.get(OZONE_OM_ADDRESS_KEY));
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);
    FileSystem fs = FileSystem.get(conf);
    final String dir = rootPath + fsoOzoneBucket.getVolumeName()
        + OZONE_URI_DELIMITER + fsoOzoneBucket.getName();
    final Path file = new Path(dir, "file");
    final int dataSize = 1024;
    final byte[] data = new byte[dataSize];
    ThreadLocalRandom.current().nextBytes(data);

    // create a file, write, hsync
    FSDataOutputStream os = fs.create(file, true);
    os.write(data);
    os.hsync();
    // call lease recovery cli
    String[] args = new String[] {
        "--path", file.toUri().toString()};
    StringWriter stdout = new StringWriter();
    PrintWriter pstdout = new PrintWriter(stdout);
    StringWriter stderr = new StringWriter();
    PrintWriter pstderr = new PrintWriter(stderr);

    CommandLine cmd = new CommandLine(new LeaseRecoverer())
        .setOut(pstdout)
        .setErr(pstderr);
    cmd.execute(args);

    assertEquals("", stderr.toString());

    // make sure file is visible and closed
    FileStatus fileStatus = fs.getFileStatus(file);
    assertEquals(dataSize, fileStatus.getLen());
    // make sure the writer can not write again.
    // TODO: write does not fail here. Looks like a bug. HDDS-8439 to fix it.
    os.write(data);
    fileStatus = fs.getFileStatus(file);
    assertEquals(dataSize, fileStatus.getLen());
    // make sure hsync fails
    assertThrows(OMException.class, os::hsync);
    // make sure length remains the same
    fileStatus = fs.getFileStatus(file);
    assertEquals(dataSize, fileStatus.getLen());
    // make sure close fails
    assertThrows(OMException.class, os::close);
    // make sure length remains the same
    fileStatus = fs.getFileStatus(file);
    assertEquals(dataSize, fileStatus.getLen());
  }
}
