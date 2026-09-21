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

package org.apache.hadoop.ozone.shell.snapshot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import org.apache.hadoop.ozone.snapshot.SnapshotBucketCount;
import org.apache.hadoop.ozone.snapshot.SnapshotCountResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link CountSnapshotHandler}.
 */
public class TestCountSnapshotHandler {

  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final PrintStream originalOut = System.out;
  private static final String DEFAULT_ENCODING = StandardCharsets.UTF_8.name();

  @BeforeEach
  public void setup() throws UnsupportedEncodingException {
    outContent.reset();
    System.setOut(new PrintStream(outContent, false, DEFAULT_ENCODING));
  }

  @AfterEach
  public void teardown() {
    System.setOut(originalOut);
  }

  @Test
  public void testCountBucketWiseSnapshotDistribution() throws IOException {
    CountSnapshotHandler handler = new CountSnapshotHandler();
    OzoneClient client = mock(OzoneClient.class);
    ObjectStore store = mock(ObjectStore.class);
    when(client.getObjectStore()).thenReturn(store);

    SnapshotCountResponse countResponse = new SnapshotCountResponse(3, 1, 4, Arrays.asList(
        new SnapshotBucketCount("vol1", "bucket1", 1, 1, 2),
        new SnapshotBucketCount("vol2", "bucket3", 2, 0, 2)));
    when(store.snapshotCount(null)).thenReturn(countResponse);

    handler.execute(client, new OzoneAddress());

    JsonNode output = new ObjectMapper().readTree(outContent.toString(DEFAULT_ENCODING));
    assertEquals(3, output.get("total").get("active").asInt());
    assertEquals(1, output.get("total").get("deleted").asInt());
    assertEquals(4, output.get("total").get("total").asInt());

    assertEquals(2, output.get("buckets").size());
    JsonNode bucket1Count = output.get("buckets").get("vol1/bucket1");
    assertEquals(1, bucket1Count.get("active").asInt());
    assertEquals(1, bucket1Count.get("deleted").asInt());
    assertEquals(2, bucket1Count.get("total").asInt());

    JsonNode bucket3Count = output.get("buckets").get("vol2/bucket3");
    assertEquals(2, bucket3Count.get("active").asInt());
    assertEquals(0, bucket3Count.get("deleted").asInt());
    assertEquals(2, bucket3Count.get("total").asInt());
  }

  @Test
  public void testCountBucketFilterIncludesMatchingEmptyBucket() throws Exception {
    CountSnapshotHandler handler = new CountSnapshotHandler();
    setBucketFilter(handler, "vol1/bucket2");

    OzoneClient client = mock(OzoneClient.class);
    ObjectStore store = mock(ObjectStore.class);
    when(client.getObjectStore()).thenReturn(store);
    SnapshotCountResponse emptyBucketResponse = new SnapshotCountResponse(0, 0, 0,
        Arrays.asList(new SnapshotBucketCount("vol1", "bucket2", 0, 0, 0)));
    when(store.snapshotCount("vol1/bucket2")).thenReturn(emptyBucketResponse);

    handler.execute(client, new OzoneAddress());
    verify(store).snapshotCount("vol1/bucket2");

    JsonNode output = new ObjectMapper().readTree(outContent.toString(DEFAULT_ENCODING));
    assertEquals(0, output.get("total").get("active").asInt());
    assertEquals(0, output.get("total").get("deleted").asInt());
    assertEquals(0, output.get("total").get("total").asInt());
    assertEquals(1, output.get("buckets").size());
    assertEquals(0, output.get("buckets").get("vol1/bucket2").get("total").asInt());
  }

  private static void setBucketFilter(CountSnapshotHandler handler, String value) throws Exception {
    Field field = CountSnapshotHandler.class.getDeclaredField("bucketFilter");
    field.setAccessible(true);
    field.set(handler, value);
  }
}
