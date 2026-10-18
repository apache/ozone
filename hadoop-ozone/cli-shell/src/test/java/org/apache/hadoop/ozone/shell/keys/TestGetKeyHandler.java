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

package org.apache.hadoop.ozone.shell.keys;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

/**
 * Unit tests for GetKeyHandler's cleanup behaviour on failure.
 */
public class TestGetKeyHandler {

  private GetKeyHandler cmd;
  private OzoneBucket bucket;
  private OzoneAddress address;
  private Path out;

  @BeforeEach
  public void setup(@TempDir Path tempDir) throws Exception {
    // Override getConf() so execute() can be called directly without going
    // through Handler.call(), which normally sets the OzoneConfiguration.
    cmd = new GetKeyHandler() {
      @Override
      public OzoneConfiguration getConf() {
        return new OzoneConfiguration();
      }
    };
    bucket = mock(OzoneBucket.class);
    address = new OzoneAddress("o3://ozone1/vol/bucket/key");
    out = tempDir.resolve("out.dat");
    parseArgs(out.toString());
  }

  /**
   * When the first read throws an IOException (zero bytes written),
   * the empty output file should be deleted.
   */
  @Test
  public void testEmptyFileDeletedOnFirstReadFailure() throws Exception {
    when(bucket.readKey(anyString()))
        .thenReturn(new OzoneInputStream(failOnFirstReadWithIOException()));

    assertThrows(IOException.class, () -> cmd.execute(buildClient(), address));

    assertThat(out).doesNotExist();
  }

  /**
   * When the first read throws a RuntimeException (e.g. NO_REPLICA_FOUND from
   * XceiverClientManager), the empty output file should be deleted.
   */
  @Test
  public void testEmptyFileDeletedOnRuntimeException() throws Exception {
    when(bucket.readKey(anyString()))
        .thenReturn(new OzoneInputStream(failOnFirstReadWithRuntimeException()));

    assertThrows(IllegalArgumentException.class, () -> cmd.execute(buildClient(), address));

    assertThat(out).doesNotExist();
  }

  /**
   * When some bytes are written before failure, the partial file should be
   * kept so the user can inspect or recover what was downloaded.
   */
  @Test
  public void testPartialFileKeptOnMidTransferFailure() throws Exception {
    byte[] partial = "hello".getBytes(StandardCharsets.UTF_8);

    when(bucket.readKey(anyString()))
        .thenReturn(new OzoneInputStream(failAfterBytes(partial)));

    OzoneClient client = buildClient();
    assertThrows(IOException.class, () -> cmd.execute(client, address));

    assertThat(out).hasBinaryContent(partial);
  }

  /**
   * Successful download: file exists and contains the full content.
   */
  @Test
  public void testSuccessfulDownload() throws Exception {
    byte[] content = "full content".getBytes(StandardCharsets.UTF_8);

    when(bucket.readKey(anyString()))
        .thenReturn(new OzoneInputStream(new ByteArrayInputStream(content)));

    cmd.execute(buildClient(), address);

    assertThat(out).hasBinaryContent(content);
  }

  /**
   * Successful zero-byte key: the empty file must be kept (cleanup only runs
   * on the failure path).
   */
  @Test
  public void testSuccessfulZeroByteKeyKeepsEmptyFile() throws Exception {
    when(bucket.readKey(anyString()))
        .thenReturn(new OzoneInputStream(new ByteArrayInputStream(new byte[0])));

    cmd.execute(buildClient(), address);

    assertThat(out).isEmptyFile();
  }

  /** Simulate a stream that throws IOException on the first read (zero bytes written). */
  private static InputStream failOnFirstReadWithIOException() {
    return new InputStream() {
      @Override
      public int read() throws IOException {
        throw new IOException("Simulated Datanode failure");
      }

      @Override
      public int read(byte[] b, int off, int len) throws IOException {
        throw new IOException("Simulated Datanode failure");
      }
    };
  }

  /** Simulate a stream that throws RuntimeException on the first read (e.g. NO_REPLICA_FOUND). */
  private static InputStream failOnFirstReadWithRuntimeException() {
    return new InputStream() {
      @Override
      public int read() {
        throw new IllegalArgumentException("NO_REPLICA_FOUND");
      }

      @Override
      public int read(byte[] b, int off, int len) {
        throw new IllegalArgumentException("NO_REPLICA_FOUND");
      }
    };
  }

  /** Simulate a stream that yields {@code prefix} then throws. */
  private static InputStream failAfterBytes(byte[] prefix) {
    return new InputStream() {
      private int pos = 0;

      @Override
      public int read() throws IOException {
        if (pos < prefix.length) {
          return prefix[pos++] & 0xFF;
        }
        throw new IOException("Simulated mid-transfer failure");
      }

      @Override
      public int read(byte[] b, int off, int len) throws IOException {
        if (pos >= prefix.length) {
          throw new IOException("Simulated mid-transfer failure");
        }
        int n = Math.min(len, prefix.length - pos);
        System.arraycopy(prefix, pos, b, off, n);
        pos += n;
        return n;
      }
    };
  }

  private void parseArgs(String... extra) {
    // KeyHandler uses @Mixin KeyUri for index 0, GetKeyHandler uses
    // @Parameters index 1 for the local file path.
    String[] base = {"o3://ozone1/vol/bucket/key"};
    String[] all = new String[base.length + extra.length];
    System.arraycopy(base, 0, all, 0, base.length);
    System.arraycopy(extra, 0, all, base.length, extra.length);
    new CommandLine(cmd).parseArgs(all);
  }

  private OzoneClient buildClient() throws Exception {
    OzoneClient client = mock(OzoneClient.class);
    ObjectStore os = mock(ObjectStore.class);
    OzoneVolume vol = mock(OzoneVolume.class);
    when(client.getObjectStore()).thenReturn(os);
    when(os.getVolume(anyString())).thenReturn(vol);
    when(vol.getBucket(anyString())).thenReturn(bucket);
    return client;
  }
}
