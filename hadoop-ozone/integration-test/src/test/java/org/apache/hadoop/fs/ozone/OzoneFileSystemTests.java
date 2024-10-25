/*
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
package org.apache.hadoop.fs.ozone;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_LISTING_PAGE_SIZE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION_TYPE;

import static org.assertj.core.api.Assertions.assertThat;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Common test cases for Ozone file systems.
 */
final class OzoneFileSystemTests {

  private OzoneFileSystemTests() {
    // no instances
  }

  /**
   * Tests listStatusIterator operation on directory with different
   * numbers of child directories.
   */
  public static void listStatusIteratorOnPageSize(OzoneConfiguration conf,
      String rootPath) throws IOException {
    final int pageSize = 32;
    int[] dirCounts = {
        1,
        pageSize - 1,
        pageSize,
        pageSize + 1,
        pageSize + pageSize / 2,
        pageSize + pageSize
    };
    OzoneConfiguration config = new OzoneConfiguration(conf);
    config.setInt(OZONE_FS_LISTING_PAGE_SIZE, pageSize);
    URI uri = FileSystem.getDefaultUri(config);
    config.setBoolean(
        String.format("fs.%s.impl.disable.cache", uri.getScheme()), true);
    try (FileSystem subject = FileSystem.get(uri, config)) {
      Path dir = new Path(Objects.requireNonNull(rootPath),
          "listStatusIterator");
      try {
        Set<String> paths = new TreeSet<>();
        for (int dirCount : dirCounts) {
          listStatusIterator(subject, dir, paths, dirCount);
        }
      } finally {
        subject.delete(dir, true);
      }
    }
  }

  private static void listStatusIterator(FileSystem subject,
      Path dir, Set<String> paths, int total) throws IOException {
    for (int i = paths.size(); i < total; i++) {
      Path p = new Path(dir, String.valueOf(i));
      subject.mkdirs(p);
      paths.add(p.getName());
    }

    RemoteIterator<FileStatus> iterator = subject.listStatusIterator(dir);
    int iCount = 0;
    if (iterator != null) {
      while (iterator.hasNext()) {
        FileStatus fileStatus = iterator.next();
        iCount++;
        String filename = fileStatus.getPath().getName();
        assertThat(paths).contains(filename);
      }
    }

    assertEquals(total, iCount);
  }

  static void createKeyWithECReplicationConfiguration(OzoneConfiguration inputConf, Path keyPath)
      throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration(inputConf);
    conf.set(OZONE_REPLICATION, "rs-3-2-1024k");
    conf.set(OZONE_REPLICATION_TYPE, "EC");
    URI uri = FileSystem.getDefaultUri(conf);
    conf.setBoolean(
        String.format("fs.%s.impl.disable.cache", uri.getScheme()), true);
    try (FileSystem fileSystem = FileSystem.get(uri, conf)) {
      ContractTestUtils.touch(fileSystem, keyPath);
    }
  }

  static void testSetReplication(FileSystem fs, OzoneBucket bucket, Path baseUri)
      throws IOException {
    String testKeyName = "testKey" + RandomStringUtils.randomNumeric(4);
    Path keyPath = new Path(baseUri, testKeyName);
    try (OzoneOutputStream outputStream = bucket.createKey(testKeyName, 1,
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE),
        new HashMap<>())) {
      outputStream.write(RandomUtils.nextBytes(1));
    }
    OzoneKeyDetails key = bucket.getKey(testKeyName);
    assertEquals(HddsProtos.ReplicationType.RATIS,
        key.getReplicationConfig().getReplicationType());
    assertEquals(ReplicationFactor.ONE.toString(),
        key.getReplicationConfig().getReplication());
    fs.setReplication(keyPath, (short) 3);
    key = bucket.getKey(testKeyName);
    assertEquals(ReplicationFactor.THREE.toString(),
        key.getReplicationConfig().getReplication());
    IOException exception = assertThrows(IOException.class,
        () -> fs.setReplication(keyPath, (short) 5),
        "Does not throw IOException");
    assertThat(exception.getMessage()).contains("not supported");
    exception = assertThrows(IOException.class,
        () -> fs.setReplication(keyPath, (short) 2),
        "Does not throw IOException");
    assertThat(exception.getMessage()).contains("not supported");
  }

}
