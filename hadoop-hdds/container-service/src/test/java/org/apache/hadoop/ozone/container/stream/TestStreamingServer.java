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

package org.apache.hadoop.ozone.container.stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Testing stream server.
 */
public class TestStreamingServer {

  private static final String SUBDIR = "test1";

  private static final byte[] CONTENT = "Stream it if you can"
      .getBytes(StandardCharsets.UTF_8);

  @TempDir
  private Path sourceDir;
  @TempDir
  private Path destDir;

  @Test
  public void simpleStream() throws Exception {
    Files.createDirectories(sourceDir.resolve(SUBDIR));
    Files.createDirectories(destDir.resolve(SUBDIR));

    //GIVEN: generate file
    Files.write(sourceDir.resolve(SUBDIR).resolve("file1"), CONTENT);

    //WHEN: stream subdir
    streamDir(SUBDIR);

    //THEN: compare the files
    final byte[] targetContent = Files
        .readAllBytes(destDir.resolve(SUBDIR).resolve("file1"));
    assertArrayEquals(CONTENT, targetContent);

  }

  @Test
  public void ssl() throws Exception {

    SelfSignedCertificate ssc = new SelfSignedCertificate();

    SslContext sslCtxServer =
        SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey())
            .build();

    final SslContext sslCtxClient = SslContextBuilder.forClient()
        .trustManager(InsecureTrustManagerFactory.INSTANCE)
        .build();

    Files.createDirectories(sourceDir.resolve(SUBDIR));
    Files.createDirectories(destDir.resolve(SUBDIR));

    //GIVEN: generate file
    Files.write(sourceDir.resolve(SUBDIR).resolve("file1"), CONTENT);

    //WHEN: stream subdir
    try (StreamingServer server =
             new StreamingServer(
                 new DirectoryServerSource(sourceDir), 0,
                 sslCtxServer)) {

      server.start();

      try (StreamingClient client =
               new StreamingClient(
                   "localhost",
                   server.getPort(),
                   new DirectoryServerDestination(destDir),
                   sslCtxClient)) {
        client.stream(SUBDIR);
      }
    }

    //THEN: compare the files
    final byte[] targetContent = Files
        .readAllBytes(destDir.resolve(SUBDIR).resolve("file1"));
    assertArrayEquals(CONTENT, targetContent);

  }

  @Test
  public void failedStream() throws Exception {
    Files.createDirectories(sourceDir.resolve(SUBDIR));
    Files.createDirectories(destDir.resolve(SUBDIR));

    //GIVEN: generate file
    Files.write(sourceDir.resolve(SUBDIR).resolve("file1"), CONTENT);

    //WHEN: stream subdir
    assertThrows(RuntimeException.class,
        () -> streamDir("NO_SUCH_ID"));

    //THEN: compare the files
    //exception is expected

  }

  @Test
  public void timeout() throws Exception {
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
        assertThrows(RuntimeException.class, () ->
            client.stream(SUBDIR, 1L, TimeUnit.SECONDS));
      }
    }

    //THEN: compare the files
    //exception is expected

  }

  @Test
  public void testChannelLeakOnTimeoutWithoutClose() throws Exception {
    Files.createDirectories(sourceDir.resolve(SUBDIR));
    Files.write(sourceDir.resolve(SUBDIR).resolve("file1"), CONTENT);

    try (StreamingServer server = new StreamingServer(
        new DirectoryServerSource(sourceDir) {
          @Override
          public Map<String, Path> getFilesToStream(String id)
              throws InterruptedException {
            // Delay to cause timeout
            Thread.sleep(3000L);
            return super.getFilesToStream(id);
          }
        }, 0)) {
      server.start();

      // Create client WITHOUT try-with-resources to simulate resource leak
      StreamingClient client = new StreamingClient("localhost", server.getPort(),
          new DirectoryServerDestination(destDir));

      try {
        client.stream(SUBDIR, 1L, TimeUnit.SECONDS);
        // Should not reach here
        throw new AssertionError("Expected exception, but none was thrown");
      } catch (StreamingException e) {
        String message = e.getMessage();
        if (!message.contains("timed out") && !message.contains("timeout")) {
          throw new AssertionError(
              "Expected timeout exception, but got: " + message + ". " +
              "This indicates the bug: await() returned false but we didn't check it. " +
              "Channel may be leaking.");
        }
      }
      client.close();
    }
  }

  private void streamDir(String subdir) {
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
