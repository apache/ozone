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

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.LinkedList;
import java.util.UUID;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.ozone.test.NonHATests;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for HadoopNestedDirGenerator.
 */
public abstract class TestHadoopNestedDirGenerator implements NonHATests.TestCase {
  private ObjectStore store = null;
  private static final Logger LOG =
          LoggerFactory.getLogger(TestHadoopNestedDirGenerator.class);
  private OzoneClient client;

  @AfterEach
  void shutdown() {
    IOUtils.closeQuietly(client);
  }

  @BeforeEach
  void init() throws Exception {
    client = cluster().newClient();
    store = client.getObjectStore();
  }

  @Test
  public void testNestedDirTreeGeneration() throws Exception {
    verifyDirTree("vol-" + UUID.randomUUID(), "bucket1", 1, 1);
    verifyDirTree("vol-" + UUID.randomUUID(), "bucket1", 1, 5);
    verifyDirTree("vol-" + UUID.randomUUID(), "bucket1", 2, 0);
    verifyDirTree("vol-" + UUID.randomUUID(), "bucket1", 3, 2);
    verifyDirTree("vol-" + UUID.randomUUID(), "bucket1", 5, 4);
  }

  private void verifyDirTree(String volumeName, String bucketName,
                             int actualDepth, int span)
            throws IOException {
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    String rootPath = "o3fs://" + bucketName + "." + volumeName;
    String om = cluster().getConf().get(OZONE_OM_ADDRESS_KEY);
    new Freon().getCmd().execute(
        "-D", OZONE_OM_ADDRESS_KEY + "=" + om,
        "ddsg",
        "-d", String.valueOf(actualDepth),
        "-s", String.valueOf(span),
        "-n", "1",
        "-r", rootPath
    );
    // verify the directory structure
    try (FileSystem fileSystem = FileSystem.get(URI.create(rootPath), cluster().getConf())) {
      Path rootDir = new Path(rootPath.concat("/"));
      // verify root path details
      FileStatus[] fileStatuses = fileSystem.listStatus(rootDir);
      Path p = null;
      // verify the num of peer directories and span directories
      p = depthBFS(fileSystem, fileStatuses, span, actualDepth);
      int actualSpan = spanCheck(fileSystem, p);
      assertEquals(span, actualSpan, "Mismatch span in a path");
    }
  }

    /**
     * Using BFS(Breadth First Search) to find the depth of nested
     * directories. First we push the directory at level 1 to
     * queue and follow BFS, as we encounter the child directories
     * we put them in an array and increment the depth variable by 1.
     */

  private Path depthBFS(FileSystem fs, FileStatus[] fileStatuses,
                        int span, int actualDepth) throws IOException {
    int depth = 0;
    Path p = null;
    if (span > 0) {
      depth = 0;
    } else if (span == 0) {
      depth = 1;
    } else {
      LOG.info("Span value can never be negative");
    }
    LinkedList<FileStatus> queue = new LinkedList<FileStatus>();
    FileStatus f1 = fileStatuses[0];
    queue.add(f1);
    while (!queue.isEmpty()) {
      FileStatus f = queue.poll();
      FileStatus[] temp = fs.listStatus(f.getPath());
      if (temp.length > 0) {
        ++depth;
        Collections.addAll(queue, temp);
      }
      if (span == 0) {
        p = f.getPath();
      } else {
        p = f.getPath().getParent();
      }
    }
    assertEquals(depth, actualDepth, "Mismatch depth in a path");
    return p;
  }

    /**
     * We get the path of last parent directory or leaf parent directory
     * from depthBFS function above and perform 'ls' on that path
     * and count the span directories.
     */

  private int spanCheck(FileSystem fs, Path p) throws IOException {
    int sp = 0;
    FileStatus[] fileStatuses = fs.listStatus(p);
    for (FileStatus fileStatus : fileStatuses) {
      if (fileStatus.isDirectory()) {
        ++sp;
      }
    }
    return sp;
  }
}
