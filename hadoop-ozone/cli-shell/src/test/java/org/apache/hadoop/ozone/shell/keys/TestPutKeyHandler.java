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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.tracing.TestSpanCollector;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

/**
 * Tests the span attributes {@link PutKeyHandler} records.
 */
class TestPutKeyHandler {

  @Test
  void testPutRecordsDataSizeAndReplication(@TempDir Path tempDir) throws Exception {
    Path file = tempDir.resolve("in.dat");
    Files.write(file, "sample value".getBytes(StandardCharsets.UTF_8));
    PutKeyHandler cmd = new PutKeyHandler() {
      @Override
      public OzoneConfiguration getConf() {
        return new OzoneConfiguration();
      }
    };
    new CommandLine(cmd).parseArgs("o3://ozone1/vol/bucket/key", file.toString(), "--type", "RATIS",
        "--replication", "ONE");
    OzoneBucket bucket = mock(OzoneBucket.class);
    when(bucket.getMetadata()).thenReturn(ImmutableMap.of());
    when(bucket.createKey(anyString(), anyLong(), any(ReplicationConfig.class), anyMap()))
        .thenReturn(new OzoneOutputStream(new ByteArrayOutputStream(), null));
    OzoneClient client = mock(OzoneClient.class);
    ObjectStore store = mock(ObjectStore.class);
    OzoneVolume volume = mock(OzoneVolume.class);
    when(client.getObjectStore()).thenReturn(store);
    when(store.getVolume(anyString())).thenReturn(volume);
    when(volume.getBucket(anyString())).thenReturn(bucket);

    try (TestSpanCollector collector = TestSpanCollector.install("shell")) {
      TracingUtil.executeInNewSpan("root", () -> cmd.execute(client, new OzoneAddress("o3://ozone1/vol/bucket/key")));

      collector.assertAttribute("root", OzoneConsts.DATA_SIZE, Files.size(file));
      collector.assertAttribute("root", OzoneConsts.REPLICATION_CONFIG, "RATIS/ONE");
    }
  }
}
