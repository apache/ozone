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

package org.apache.hadoop.ozone.container.replication;

import static org.apache.hadoop.ozone.container.common.impl.ContainerImplTestUtils.newContainerSet;
import static org.apache.hadoop.ozone.container.replication.CopyContainerCompression.NO_COMPRESSION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.VolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.common.volume.VolumeChoosingPolicyFactory;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test for {@link SendContainerRequestHandler}.
 */
public class TestSendContainerRequestHandler {

  @TempDir
  private File tempDir;

  private OzoneConfiguration conf;

  private ContainerSet containerSet;
  private MutableVolumeSet volumeSet;
  private ContainerImporter importer;
  private StreamObserver<ContainerProtos.SendContainerResponse> responseObserver;
  private SendContainerRequestHandler sendContainerRequestHandler;
  private long containerMaxSize;

  @BeforeEach
  void setup() throws IOException {
    conf = new OzoneConfiguration();
    conf.set(ScmConfigKeys.HDDS_DATANODE_DIR_KEY, tempDir.getAbsolutePath());
    VolumeChoosingPolicy volumeChoosingPolicy = VolumeChoosingPolicyFactory.getPolicy(conf);
    containerSet = newContainerSet(0);
    volumeSet = new MutableVolumeSet("test", conf, null,
        StorageVolume.VolumeType.DATA_VOLUME, null);
    importer = new ContainerImporter(conf, containerSet,
        mock(ContainerController.class), volumeSet, volumeChoosingPolicy);
    importer = spy(importer);
    responseObserver = mock(StreamObserver.class);
    sendContainerRequestHandler = new SendContainerRequestHandler(importer, responseObserver, null);
    containerMaxSize = (long) conf.getStorageSize(
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT, StorageUnit.BYTES);
  }

  /**
   * Provides stream of different container sizes for tests.
   */
  public static Stream<Arguments> sizeProvider() {
    return Stream.of(
        Arguments.of("Null replicate size (fallback to default)", null),
        Arguments.of("Zero Size", 0L),
        Arguments.of("Normal 2GB", 2L * 1024L * 1024L * 1024L),
        Arguments.of("Overallocated 20GB", 20L * 1024L * 1024L * 1024L)
    );
  }

  @Test
  void testReceiveDataForExistingContainer() throws Exception {
    long containerId = 1;
    // create containerImporter
    KeyValueContainerData containerData = new KeyValueContainerData(containerId,
        ContainerLayoutVersion.FILE_PER_BLOCK, 100, "test", "test");
    // add container to container set
    KeyValueContainer container = new KeyValueContainer(containerData, conf);
    containerSet.addContainer(container);

    doAnswer(invocation -> {
      Object arg = invocation.getArgument(0);
      assertInstanceOf(StorageContainerException.class, arg);
      assertEquals(ContainerProtos.Result.CONTAINER_EXISTS,
          ((StorageContainerException) arg).getResult());
      return null;
    }).when(responseObserver).onError(any());

    sendContainerRequestHandler.onNext(createRequest(containerId,
        ByteString.copyFromUtf8("test"), 0, null));
  }

  @ParameterizedTest(name = "for {0}")
  @MethodSource("sizeProvider")
  public void testSpaceReservedAndReleasedWhenRequestCompleted(String testName, Long size) throws Exception {
    long containerId = 1;
    HddsVolume volume = (HddsVolume) volumeSet.getVolumesList().get(0);
    long initialCommittedBytes = volume.getCommittedBytes();
    long expectedReservedSpace = size != null ?
        importer.getRequiredReplicationSpace(size) :
        importer.getDefaultReplicationSpace();

    // Create and execute the first request to reserve space
    sendContainerRequestHandler.onNext(
        createRequest(containerId, ByteString.EMPTY, 0, size));

    // Verify commit space is reserved
    assertEquals(volume.getCommittedBytes(), initialCommittedBytes + expectedReservedSpace);

    // complete the request
    sendContainerRequestHandler.onCompleted();

    // Verify commit space is released
    assertEquals(volume.getCommittedBytes(), initialCommittedBytes);
  }

  @ParameterizedTest(name = "for {0}")
  @MethodSource("sizeProvider")
  public void testSpaceReservedAndReleasedWhenOnNextFails(String testName, Long size) throws Exception {
    long containerId = 1;
    HddsVolume volume = (HddsVolume) volumeSet.getVolumesList().get(0);
    long initialCommittedBytes = volume.getCommittedBytes();
    long expectedReservedSpace = size != null ?
        importer.getRequiredReplicationSpace(size) :
        importer.getDefaultReplicationSpace();

    ByteString data = ByteString.copyFromUtf8("test");
    // Execute first request to reserve space
    sendContainerRequestHandler.onNext(
        createRequest(containerId, data, 0, size));

    // Verify commit space is reserved
    assertEquals(volume.getCommittedBytes(), initialCommittedBytes + expectedReservedSpace);

    // mock the importer is not allowed to import this container
    when(importer.isAllowedContainerImport(containerId)).thenReturn(false);
    
    sendContainerRequestHandler.onNext(createRequest(containerId, data, 0,
        size));

    // Verify commit space is released
    assertEquals(volume.getCommittedBytes(), initialCommittedBytes);
  }

  @ParameterizedTest(name = "for {0}")
  @MethodSource("sizeProvider")
  public void testSpaceReservedAndReleasedWhenOnCompletedFails(String testName, Long size) throws Exception {
    long containerId = 1;
    HddsVolume volume = (HddsVolume) volumeSet.getVolumesList().get(0);
    long initialCommittedBytes = volume.getCommittedBytes();
    long expectedReservedSpace = size != null ?
        importer.getRequiredReplicationSpace(size) :
        importer.getDefaultReplicationSpace();

    // Execute request
    sendContainerRequestHandler.onNext(createRequest(containerId,
        ByteString.copyFromUtf8("test"), 0, size));

    // Verify commit space is reserved
    assertEquals(volume.getCommittedBytes(), initialCommittedBytes + expectedReservedSpace);

    doThrow(new IOException("Failed")).when(importer).importContainer(anyLong(), any(), any(), any());

    sendContainerRequestHandler.onCompleted();

    // Verify commit space is released
    assertEquals(volume.getCommittedBytes(), initialCommittedBytes);
  }

  /**
   * Test that verifies the actual space calculation difference between
   * overallocated containers and default containers.
   */
  @Test
  public void testOverAllocatedReservesMoreSpace() {
    long containerId1 = 1;
    long containerId2 = 2;
    long overallocatedSize = containerMaxSize * 2; // 10GB
    HddsVolume volume = (HddsVolume) volumeSet.getVolumesList().get(0);
    long initialCommittedBytes = volume.getCommittedBytes();
    // Test overallocated container (10GB)
    SendContainerRequestHandler handler1 = new SendContainerRequestHandler(importer, responseObserver, null);
    handler1.onNext(createRequest(containerId1, ByteString.EMPTY, 0, overallocatedSize));

    long overallocatedReservation = volume.getCommittedBytes() - initialCommittedBytes;
    handler1.onCompleted(); // Release space

    // Test default container (null size)
    SendContainerRequestHandler handler2 = new SendContainerRequestHandler(importer, responseObserver, null);
    handler2.onNext(createRequest(containerId2, ByteString.EMPTY, 0, null));

    long defaultReservation = volume.getCommittedBytes() - initialCommittedBytes;
    handler2.onCompleted(); // Release space

    // Verify overallocated container reserves more space
    assertTrue(overallocatedReservation > defaultReservation);

    // Verify specific calculations
    assertEquals(2 * overallocatedSize, overallocatedReservation);
    assertEquals(2 * containerMaxSize, defaultReservation);
  }

  private ContainerProtos.SendContainerRequest createRequest(
      long containerId, ByteString data, int offset, Long size) {
    ContainerProtos.SendContainerRequest.Builder builder =
        ContainerProtos.SendContainerRequest.newBuilder()
            .setContainerID(containerId)
            .setData(data)
            .setOffset(offset)
            .setCompression(NO_COMPRESSION.toProto());

    if (size != null) {
      builder.setSize(size);
    }
    return builder.build();
  }
}
