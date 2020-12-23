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
package org.apache.hadoop.ozone.freon;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.raftlog.RaftLog;
import java.util.LinkedList;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

/**
 * Test for HadoopNestedDirGenerator.
 */

public class TestHadoopNestedDirGenerator {

  private String path;
  private OzoneConfiguration conf = null;
  private MiniOzoneCluster cluster = null;
  private ObjectStore store = null;
  private static final Logger LOG =
          LoggerFactory.getLogger(TestHadoopNestedDirGenerator.class);
  @Before
    public void setup() {
    path = GenericTestUtils
                .getTempPath(TestOzoneClientKeyGenerator.class.getSimpleName());
    GenericTestUtils.setLogLevel(RaftLog.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(RaftServer.LOG, Level.DEBUG);
    File baseDir = new File(path);
    baseDir.mkdirs();
  }

    /**
     * Shutdown MiniDFSCluster.
     */

  private void shutdown() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
      FileUtils.deleteDirectory(new File(path));
    }
  }

    /**
     * Create a MiniDFSCluster for testing.
     *
     * @throws IOException
     */

  private void startCluster() throws Exception {
    conf = new OzoneConfiguration();
    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(5).build();
    cluster.waitForClusterToBeReady();
    cluster.waitTobeOutOfSafeMode();
    store = OzoneClientFactory.getRpcClient(conf).getObjectStore();
  }

  @Test
    public void testNestedDirTreeGeneration() throws Exception {
    try {
      startCluster();
      FileOutputStream out = FileUtils.openOutputStream(new File(path,
              "conf"));
      cluster.getConf().writeXml(out);
      out.getFD().sync();
      out.close();
      verifyDirTree("vol1",
              "bucket1", 1, 1);
      verifyDirTree("vol2",
              "bucket1", 1, 5);
      verifyDirTree("vol3",
              "bucket1", 2, 0);
      verifyDirTree("vol4",
              "bucket1", 3, 2);
      verifyDirTree("vol5",
              "bucket1", 5, 4);
    } finally {
      shutdown();
    }
  }

  private void verifyDirTree(String volumeName, String bucketName,
                             int actualDepth, int span)
            throws IOException {
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    String rootPath = "o3fs://" + bucketName + "." + volumeName;
    String confPath = new File(path, "conf").getAbsolutePath();
    new Freon().execute(new String[]{"-conf", confPath, "ddsg", "-d",
        actualDepth + "", "-s", span + "", "-n", "1", "-r", rootPath});
    // verify the directory structure
    FileSystem fileSystem = FileSystem.get(URI.create(rootPath),
            conf);
    Path rootDir = new Path(rootPath.concat("/"));
    // verify root path details
    FileStatus[] fileStatuses = fileSystem.listStatus(rootDir);
    Path p = null;
    for (FileStatus fileStatus : fileStatuses) {
      // verify the num of peer directories and span directories
      p = depthBFS(fileSystem, fileStatuses, span, actualDepth);
      int actualSpan = spanCheck(fileSystem, span, p);
      Assert.assertEquals("Mismatch span in a path",
              span, actualSpan);
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
    if(span > 0){
      depth = 0;
    } else if(span == 0){
      depth = 1;
    } else{
      LOG.info("Span value can never be negative");
    }
    LinkedList<FileStatus> queue = new LinkedList<FileStatus>();
    FileStatus f1 = fileStatuses[0];
    queue.add(f1);
    while(queue.size() != 0){
      FileStatus f = queue.poll();
      FileStatus[] temp = fs.listStatus(f.getPath());
      if(temp.length > 0){
        ++depth;
        for(int i = 0; i < temp.length; i++){
          queue.add(temp[i]);
        }
      }
      if(span == 0){
        p = f.getPath();
      } else{
        p = f.getPath().getParent();
      }
    }
    Assert.assertEquals("Mismatch depth in a path",
            depth, actualDepth);
    return p;
  }

    /**
     * We get the path of last parent directory or leaf parent directory
     * from depthBFS function above and perform 'ls' on that path
     * and count the span directories.
     */

  private int spanCheck(FileSystem fs, int span, Path p) throws IOException{
    int sp = 0;
    int depth = 0;
    if(span >= 0){
      depth = 0;
    } else{
      LOG.info("Span value can never be negative");
    }
    FileStatus[] fileStatuses = fs.listStatus(p);
    for (FileStatus fileStatus : fileStatuses){
      if(fileStatus.isDirectory()){
        ++sp;
      }
    }
    return sp;
  }
}
