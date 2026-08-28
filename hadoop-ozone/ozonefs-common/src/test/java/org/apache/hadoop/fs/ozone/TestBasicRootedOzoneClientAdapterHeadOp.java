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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.time.Instant;
import java.util.Collections;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.OFSPath;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Unit tests for headOp propagation through
 * {@link BasicRootedOzoneClientAdapterImpl#getFileStatus} (HDDS-15678). Uses a
 * partial mock so no OM connection is required.
 */
public class TestBasicRootedOzoneClientAdapterHeadOp {

  private static final URI URI_OFS = URI.create("ofs://om/");
  private static final Path WORKING_DIR = new Path("/");

  private BasicRootedOzoneClientAdapterImpl adapter;
  private OzoneBucket bucket;

  @BeforeEach
  public void setUp() throws Exception {
    adapter = mock(BasicRootedOzoneClientAdapterImpl.class, CALLS_REAL_METHODS);
    bucket = mock(OzoneBucket.class);
    doReturn(bucket).when(adapter).getBucket(any(OFSPath.class), eq(false));

    // Inject a mock object store so the volume/snapshot dispatch branches can
    // run without a live OM connection.
    OzoneVolume volume = mock(OzoneVolume.class);
    when(volume.getName()).thenReturn("vol");
    when(volume.getOwner()).thenReturn("user");
    when(volume.getCreationTime()).thenReturn(Instant.EPOCH);
    ObjectStore objectStore = mock(ObjectStore.class);
    when(objectStore.getVolume(anyString())).thenReturn(volume);
    Field field =
        BasicRootedOzoneClientAdapterImpl.class.getDeclaredField("objectStore");
    field.setAccessible(true);
    field.set(adapter, objectStore);
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
  public void keyPathThreadsHeadOp() throws IOException {
    when(bucket.getFileStatus(anyString(), anyBoolean()))
        .thenReturn(fileStatus(false));

    assertFalse(adapter.getFileStatus("/vol/bucket/key", URI_OFS, WORKING_DIR,
        "user", true).isDir());

    ArgumentCaptor<Boolean> headOp = ArgumentCaptor.forClass(Boolean.class);
    verify(bucket).getFileStatus(anyString(), headOp.capture());
    assertTrue(headOp.getValue());
  }

  @Test
  public void fourArgOverloadDoesNotUseHeadOp() throws IOException {
    when(bucket.getFileStatus(anyString(), anyBoolean()))
        .thenReturn(fileStatus(true));

    assertTrue(adapter.getFileStatus("/vol/bucket/key", URI_OFS, WORKING_DIR,
        "user").isDir());
    verify(bucket).getFileStatus(anyString(), eq(false));
  }

  @Test
  public void rootPathReturnsDirectory() throws IOException {
    assertTrue(adapter.getFileStatus("/", URI_OFS, WORKING_DIR, "user", true)
        .isDir());
  }

  @Test
  public void fileNotFoundMappedToFileNotFoundException() throws IOException {
    when(bucket.getFileStatus(anyString(), anyBoolean()))
        .thenThrow(new OMException("missing",
            OMException.ResultCodes.FILE_NOT_FOUND));
    assertThrows(FileNotFoundException.class,
        () -> adapter.getFileStatus("/vol/bucket/key", URI_OFS, WORKING_DIR,
            "user", true));
  }

  @Test
  public void otherOMExceptionPropagates() throws IOException {
    when(bucket.getFileStatus(anyString(), anyBoolean()))
        .thenThrow(new OMException("boom",
            OMException.ResultCodes.INTERNAL_ERROR));
    assertThrows(OMException.class,
        () -> adapter.getFileStatus("/vol/bucket/key", URI_OFS, WORKING_DIR,
            "user", true));
  }

  @Test
  public void bucketNotFoundMappedToFileNotFoundException() throws IOException {
    when(bucket.getFileStatus(anyString(), anyBoolean()))
        .thenThrow(new OMException("no bucket",
            OMException.ResultCodes.BUCKET_NOT_FOUND));
    assertThrows(FileNotFoundException.class,
        () -> adapter.getFileStatus("/vol/bucket/key", URI_OFS, WORKING_DIR,
            "user", true));
  }

  @Test
  public void volumePathReturnsDirectory() throws IOException {
    assertTrue(adapter.getFileStatus("/vol", URI_OFS, WORKING_DIR, "user", true)
        .isDir());
  }

  @Test
  public void snapshotIndicatorPathReturnsDirectory() throws IOException {
    when(bucket.getVolumeName()).thenReturn("vol");
    when(bucket.getName()).thenReturn("bucket");
    when(bucket.getCreationTime()).thenReturn(Instant.EPOCH);
    // keyName == ".snapshot" is the snapshot indicator path.
    assertTrue(adapter.getFileStatus("/vol/bucket/.snapshot", URI_OFS,
        WORKING_DIR, "user", true).isDir());
  }
}
