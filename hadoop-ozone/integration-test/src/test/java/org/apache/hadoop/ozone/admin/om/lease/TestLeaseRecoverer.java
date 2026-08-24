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

package org.apache.hadoop.ozone.admin.om.lease;

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OFS_URI_SCHEME;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LeaseRecoverable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.ozone.test.NonHATests;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import picocli.CommandLine;

/**
 * Test cases for LeaseRecoverer.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class TestLeaseRecoverer implements NonHATests.TestCase {
  private OzoneBucket fsoOzoneBucket;
  private OzoneClient client;

  @BeforeAll
  void init() throws Exception {
    client = cluster().newClient();

    // create a volume and a FSO bucket
    fsoOzoneBucket = TestDataUtil
        .createVolumeAndBucket(client, BucketLayout.FILE_SYSTEM_OPTIMIZED);
  }

  @AfterAll
  void teardownClass() {
    IOUtils.closeQuietly(client);
  }

  @Test
  public void testCLI() throws IOException {
    final String rootPath = String.format("%s://%s/",
        OZONE_OFS_URI_SCHEME, cluster().getConf().get(OZONE_OM_ADDRESS_KEY));
    final String dir = rootPath + fsoOzoneBucket.getVolumeName()
        + OZONE_URI_DELIMITER + fsoOzoneBucket.getName();
    final Path file = new Path(dir, "file");
    try (FileSystem fs = FileSystem.get(URI.create(rootPath), cluster().getConf())) {
      testWithFS(fs, file);
    }
  }

  private void testWithFS(FileSystem fs, Path file) throws IOException {
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
    // write data
    os.write(data);
    // flush should fail since flush will call writeChunk and putBlock
    assertThrows(IOException.class, os::flush);

    fileStatus = fs.getFileStatus(file);
    assertEquals(dataSize, fileStatus.getLen());
    // make sure hsync fails
    assertThrows(IOException.class, os::hsync);
    // make sure length remains the same
    fileStatus = fs.getFileStatus(file);
    assertEquals(dataSize, fileStatus.getLen());
    // close succeeds since it's already closed in failure handling of flush
    assertTrue(((LeaseRecoverable)fs).isFileClosed(file));
    os.close();
    // make sure length remains the same
    fileStatus = fs.getFileStatus(file);
    assertEquals(dataSize, fileStatus.getLen());

    // recover the same file second time should succeed
    cmd.execute(args);
    assertEquals("", stderr.toString());
  }
}
