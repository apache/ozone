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

import static org.apache.ratis.util.Preconditions.assertSame;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.SendContainerRequest;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.SendContainerResponse;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.ratis.grpc.util.ZeroCopyMessageMarshaller;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private HddsVolume volume = null;
  private Path path;
  private CopyContainerCompression compression;
  private final ZeroCopyMessageMarshaller<SendContainerRequest> marshaller;
  private long spaceToReserve = 0;

  SendContainerRequestHandler(
      ContainerImporter importer,
      StreamObserver<SendContainerResponse> responseObserver,
      ZeroCopyMessageMarshaller<SendContainerRequest> marshaller) {
    this.importer = importer;
    this.responseObserver = responseObserver;
    this.marshaller = marshaller;
  }

  @Override
  public void onNext(SendContainerRequest req) {
    try {
      final long length = req.getData().size();
      LOG.debug("Received part for container id:{} offset:{} len:{}",
          req.getContainerID(), req.getOffset(), length);

      assertSame(nextOffset, req.getOffset(), "offset");

      // check and avoid download of container file if target already have
      // container data and import in progress
      if (!importer.isAllowedContainerImport(req.getContainerID())) {
        containerId = req.getContainerID();
        throw new StorageContainerException("Container exists or " +
            "import in progress with container Id " + req.getContainerID(),
            ContainerProtos.Result.CONTAINER_EXISTS);
      }

      if (containerId == -1) {
        containerId = req.getContainerID();
        
        // Use container size if available, otherwise fall back to default
        spaceToReserve = importer.getSpaceToReserve(
            req.hasSize() ? req.getSize() : null);

        volume = importer.chooseNextVolume(spaceToReserve);

        Path dir = ContainerImporter.getUntarDirectory(volume);
        Files.createDirectories(dir);
        path = dir.resolve(ContainerUtils.getContainerTarName(containerId));
        output = Files.newOutputStream(path);
        compression = CopyContainerCompression.fromProto(req.getCompression());

        LOG.info("Accepting container {}", req.getContainerID());
      }

      assertSame(containerId, req.getContainerID(), "containerID");

      req.getData().writeTo(output);

      nextOffset += length;
    } catch (Throwable t) {
      onError(t);
    } finally {
      if (marshaller != null) {
        marshaller.release(req);
      }
    }
  }

  @Override
  public void onError(Throwable t) {
    try {
      LOG.warn("Error receiving container {} at {}", containerId, nextOffset, t);
      closeOutput();
      deleteTarball();
      responseObserver.onError(t);
    } finally {
      if (volume != null && spaceToReserve > 0) {
        volume.incCommittedBytes(-spaceToReserve);
      }
    }
  }

  @Override
  public void onCompleted() {
    try {
      if (output == null) {
        LOG.warn("Received container without any parts");
        return;
      }

      LOG.info("Container {} is downloaded with size {}, starting to import.",
          containerId, nextOffset);
      closeOutput();

      try {
        importer.importContainer(containerId, path, volume, compression);
        LOG.info("Container {} is replicated successfully", containerId);
        responseObserver.onNext(SendContainerResponse.newBuilder().build());
        responseObserver.onCompleted();
      } catch (Throwable t) {
        LOG.warn("Failed to import container {}", containerId, t);
        deleteTarball();
        responseObserver.onError(t);
      }
    } finally {
      if (volume != null && spaceToReserve > 0) {
        volume.incCommittedBytes(-spaceToReserve);
      }
    }
  }

  private void closeOutput() {
    IOUtils.close(LOG, output);
    output = null;
  }

  private void deleteTarball() {
    try {
      if (null != path) {
        Files.deleteIfExists(path);
      }
    } catch (IOException e) {
      LOG.warn("Error removing {}", path);
    }
  }
}
