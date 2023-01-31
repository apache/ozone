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

import java.io.IOException;
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
      final long length = req.getData().size();
      LOG.info("Received part for container id:{} offset:{} len:{}",
          req.getContainerID(), req.getOffset(), length);

      assertSame(nextOffset, req.getOffset(), "offset");

      if (containerId == -1) {
        containerId = req.getContainerID();
        volume = importer.chooseNextVolume();
        Path dir = ContainerImporter.getUntarDirectory(volume);
        Files.createDirectories(dir);
        path = dir.resolve(ContainerUtils.getContainerTarName(containerId));
        output = Files.newOutputStream(path);
      }

      assertSame(containerId, req.getContainerID(), "containerID");

      req.getData().writeTo(output);

      nextOffset += length;
    } catch (Throwable t) {
      onError(t);
    }
  }

  @Override
  public void onError(Throwable t) {
    LOG.error("Error", t);
    closeOutput();
    deleteTarball();
    responseObserver.onError(t);
  }

  @Override
  public void onCompleted() {
    if (output == null) {
      LOG.warn("Received container without any parts");
      return;
    }

    LOG.info("Received all parts for container {}", containerId);
    closeOutput();

    try {
      importer.importContainer(containerId, path, volume);
      LOG.info("Imported container {}", containerId);
      responseObserver.onNext(SendContainerResponse.newBuilder().build());
      responseObserver.onCompleted();
    } catch (Throwable t) {
      LOG.info("Failed to import container {}", containerId, t);
      deleteTarball();
      responseObserver.onError(t);
    }
  }

  private void closeOutput() {
    IOUtils.cleanupWithLogger(LOG, output);
    output = null;
  }

  private void deleteTarball() {
    try {
      Files.deleteIfExists(path);
    } catch (IOException e) {
      LOG.warn("Error removing {}", path);
    }
  }
}
