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
package org.apache.hadoop.ozone.container.stream;

import org.apache.ozone.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;


/**
 * Testing stream server.
 */
public class TestStreamingServer {

  private static final String SUBDIR = "test1";

  private static final byte[] CONTENT = "Stream it if you can"
      .getBytes(StandardCharsets.UTF_8);

  @Test
  public void simpleStream() throws Exception {
    Path sourceDir = GenericTestUtils.getRandomizedTestDir().toPath();
    Path destDir = GenericTestUtils.getRandomizedTestDir().toPath();
    Files.createDirectories(sourceDir.resolve(SUBDIR));
    Files.createDirectories(destDir.resolve(SUBDIR));

    //GIVEN: generate file
    Files.write(sourceDir.resolve(SUBDIR).resolve("file1"), CONTENT);

    //WHEN: stream subdir
    streamDir(sourceDir, destDir, SUBDIR);

    //THEN: compare the files
    final byte[] targetContent = Files
        .readAllBytes(destDir.resolve(SUBDIR).resolve("file1"));
    Assert.assertArrayEquals(CONTENT, targetContent);

  }


  @Test(expected = RuntimeException.class)
  public void failedStream() throws Exception {
    Path sourceDir = GenericTestUtils.getRandomizedTestDir().toPath();
    Path destDir = GenericTestUtils.getRandomizedTestDir().toPath();
    Files.createDirectories(sourceDir.resolve(SUBDIR));
    Files.createDirectories(destDir.resolve(SUBDIR));

    //GIVEN: generate file
    Files.write(sourceDir.resolve(SUBDIR).resolve("file1"), CONTENT);

    //WHEN: stream subdir
    streamDir(sourceDir, destDir, "NO_SUCH_ID");

    //THEN: compare the files
    //exception is expected

  }

  @Test(expected = RuntimeException.class)
  public void timeout() throws Exception {
    Path sourceDir = GenericTestUtils.getRandomizedTestDir().toPath();
    Path destDir = GenericTestUtils.getRandomizedTestDir().toPath();
    Files.createDirectories(sourceDir.resolve(SUBDIR));
    Files.createDirectories(destDir.resolve(SUBDIR));

    //GIVEN: generate file
    Files.write(sourceDir.resolve(SUBDIR).resolve("file1"), CONTENT);

    //WHEN: stream subdir
    try (StreamingServer server =
             new StreamingServer(new DirectoryServerSource(sourceDir) {
               @Override
               public Map<String, Path> getFilesToStream(String id)
                   throws InterruptedException {
                 Thread.sleep(3000L);
                 return super.getFilesToStream(id);
               }
             }, 0)) {
      server.start();
      try (StreamingClient client =
               new StreamingClient("localhost", server.getPort(),
                   new DirectoryServerDestination(
                       destDir))) {
        client.stream(SUBDIR, 1L, TimeUnit.SECONDS);
      }
    }

    //THEN: compare the files
    //exception is expected

  }

  private void streamDir(Path sourceDir, Path destDir, String subdir)
      throws InterruptedException {
    try (StreamingServer server = new StreamingServer(
        new DirectoryServerSource(sourceDir), 0)) {
      server.start();
      try (StreamingClient client =
               new StreamingClient("localhost", server.getPort(),
                   new DirectoryServerDestination(
                       destDir))) {
        client.stream(subdir);
      }
    }
  }
}