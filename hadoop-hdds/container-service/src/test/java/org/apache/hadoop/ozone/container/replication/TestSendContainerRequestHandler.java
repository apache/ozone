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
package org.apache.hadoop.ozone.container.replication;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.hadoop.ozone.container.replication.CopyContainerCompression.NO_COMPRESSION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

/**
 * Test for {@link SendContainerRequestHandler}.
 */
class TestSendContainerRequestHandler {

  private OzoneConfiguration conf;

  @BeforeEach
  void setup() {
    conf = new OzoneConfiguration();
  }

  @Test
  void testReceiveDataForExistingContainer() throws Exception {
    long containerId = 1;
    // create containerImporter
    ContainerSet containerSet = new ContainerSet(0);
    MutableVolumeSet volumeSet = new MutableVolumeSet("test", conf, null,
        StorageVolume.VolumeType.DATA_VOLUME, null);
    ContainerImporter containerImporter = new ContainerImporter(conf,
        new ContainerSet(0), mock(ContainerController.class), volumeSet);
    KeyValueContainerData containerData = new KeyValueContainerData(containerId,
        ContainerLayoutVersion.FILE_PER_BLOCK, 100, "test", "test");
    // add container to container set
    KeyValueContainer container = new KeyValueContainer(containerData, conf);
    containerSet.addContainer(container);

    StreamObserver observer = mock(StreamObserver.class);
    doAnswer(invocation -> {
      Object arg = invocation.getArgument(0);
      assertInstanceOf(StorageContainerException.class, arg);
      assertEquals(ContainerProtos.Result.CONTAINER_EXISTS,
          ((StorageContainerException) arg).getResult());
      return null;
    }).when(observer).onError(any());
    SendContainerRequestHandler sendContainerRequestHandler
        = new SendContainerRequestHandler(containerImporter, observer, null);
    ByteString data = ByteString.copyFromUtf8("test");
    ContainerProtos.SendContainerRequest request
        = ContainerProtos.SendContainerRequest.newBuilder()
        .setContainerID(containerId)
        .setData(data)
        .setOffset(0)
        .setCompression(NO_COMPRESSION.toProto())
        .build();
    sendContainerRequestHandler.onNext(request);
  }
}
