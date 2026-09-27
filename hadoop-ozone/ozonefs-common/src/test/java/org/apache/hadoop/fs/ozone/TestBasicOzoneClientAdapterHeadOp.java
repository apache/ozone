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

package org.apache.hadoop.fs.ozone;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.Collections;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/**
 * Unit tests for headOp propagation through
 * {@link BasicOzoneClientAdapterImpl#getFileStatus} (HDDS-15877). Uses a partial
 * mock so no OM connection is required.
 */
public class TestBasicOzoneClientAdapterHeadOp {

  private static final URI URI_O3FS = URI.create("o3fs://bucket.vol/");
  private static final Path WORKING_DIR = new Path("/");

  private BasicOzoneClientAdapterImpl adapter;
  private OzoneBucket bucket;

  @BeforeEach
  public void setUp() throws Exception {
    adapter = mock(BasicOzoneClientAdapterImpl.class, CALLS_REAL_METHODS);
    bucket = mock(OzoneBucket.class);
    // Inject the mock bucket so getFileStatus can run without a live OM.
    Field field =
        BasicOzoneClientAdapterImpl.class.getDeclaredField("bucket");
    field.setAccessible(true);
    field.set(adapter, bucket);
  }

  @AfterEach
  public void tearDown() {
    // Ensure no Mockito stubbing state leaks into other test classes running in
    // the same JVM.
    Mockito.validateMockitoUsage();
  }

  private static OzoneFileStatus fileStatus(boolean isDir) {
    OmKeyInfo keyInfo = new OmKeyInfo.Builder()
        .setVolumeName("vol")
        .setBucketName("bucket")
        .setKeyName("key")
        .setReplicationConfig(RatisReplicationConfig.getInstance(
            HddsProtos.ReplicationFactor.THREE))
        .setOmKeyLocationInfos(Collections.emptyList())
        .setDataSize(0)
        .setCreationTime(0)
        .setModificationTime(0)
        .setAcls(Collections.emptyList())
        .build();
    return new OzoneFileStatus(keyInfo, 512, isDir);
  }

  @Test
  public void headOpOverloadThreadsHeadOp() throws IOException {
    when(bucket.getFileStatus(anyString(), anyBoolean()))
        .thenReturn(fileStatus(false));

    assertFalse(adapter.getFileStatus("key", URI_O3FS, WORKING_DIR, "user", true)
        .isDir());

    ArgumentCaptor<Boolean> headOp = ArgumentCaptor.forClass(Boolean.class);
    verify(bucket).getFileStatus(anyString(), headOp.capture());
    assertTrue(headOp.getValue());
  }

  @Test
  public void fourArgOverloadDoesNotUseHeadOp() throws IOException {
    when(bucket.getFileStatus(anyString(), anyBoolean()))
        .thenReturn(fileStatus(true));

    assertTrue(adapter.getFileStatus("key", URI_O3FS, WORKING_DIR, "user")
        .isDir());
    verify(bucket).getFileStatus(anyString(), eq(false));
  }

  @Test
  public void fileNotFoundMappedToFileNotFoundException() throws IOException {
    when(bucket.getFileStatus(anyString(), anyBoolean()))
        .thenThrow(new OMException("missing",
            OMException.ResultCodes.FILE_NOT_FOUND));
    assertThrows(FileNotFoundException.class,
        () -> adapter.getFileStatus("key", URI_O3FS, WORKING_DIR, "user", true));
  }

  @Test
  public void otherOMExceptionPropagates() throws IOException {
    when(bucket.getFileStatus(anyString(), anyBoolean()))
        .thenThrow(new OMException("boom",
            OMException.ResultCodes.INTERNAL_ERROR));
    assertThrows(OMException.class,
        () -> adapter.getFileStatus("key", URI_O3FS, WORKING_DIR, "user", true));
  }
}
