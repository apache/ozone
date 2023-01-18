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

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.SendContainerRequest;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.SendContainerResponse;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.apache.ratis.util.Preconditions.assertSame;

/**
 * Handles incoming container pushed by other datanode.
 */
class SendContainerRequestHandler
    implements StreamObserver<SendContainerRequest> {

  private static final Logger LOG =
      LoggerFactory.getLogger(SendContainerRequestHandler.class);

  private final ContainerImporter importer;
  private final StreamObserver<SendContainerResponse> responseObserver;

  private long containerId = -1;
  private long nextOffset;
  private OutputStream output;
  private HddsVolume volume;
  private Path path;

  SendContainerRequestHandler(
      ContainerImporter importer,
      StreamObserver<SendContainerResponse> responseObserver) {
    this.importer = importer;
    this.responseObserver = responseObserver;
  }

  @Override
  public void onNext(SendContainerRequest req) {
    try {
      LOG.info("Received part for container id:{} offset:{} len:{} eof:{}",
          req.getContainerID(), req.getOffset(), req.getLen(), req.getEof());

      assertSame(nextOffset, req.getOffset(), "offset");
      // TODO get rid of 'eof'?
      // TODO get rid of 'len'?

      if (containerId == -1) {
        containerId = req.getContainerID();
        volume = importer.chooseNextVolume();
        path = ContainerImporter.getUntarDirectory(volume)
            .resolve(ContainerUtils.getContainerTarGzName(containerId));
        output = Files.newOutputStream(path);
      }

      assertSame(containerId, req.getContainerID(), "containerID");

      req.getData().writeTo(output);

      nextOffset += req.getLen();
    } catch (Throwable t) {
      IOUtils.cleanupWithLogger(LOG, output);
      output = null;
      responseObserver.onError(t);
    }
  }

  @Override
  public void onError(Throwable t) {
    LOG.error("Error", t);
    IOUtils.cleanupWithLogger(LOG, output);
    responseObserver.onError(t);
  }

  @Override
  public void onCompleted() {
    if (output == null) {
      LOG.warn("Received container without any parts");
      return;
    }

    LOG.info("Received all parts for container {}", containerId);
    IOUtils.cleanupWithLogger(LOG, output);

    try {
      importer.importContainer(containerId, path, volume);
    } catch (Throwable t) {
      LOG.info("Failed to import container {}", containerId, t);
      responseObserver.onError(t);
    }

    LOG.info("Imported container {}", containerId);
    responseObserver.onCompleted();
  }
}
