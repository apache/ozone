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

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.container.replication.AbstractReplicationTask.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.util.concurrent.CompletableFuture;

import static org.apache.hadoop.ozone.container.replication.CopyContainerCompression.NO_COMPRESSION;

/**
 * Pushes the container to the target datanode.
 */
public class PushReplicator implements ContainerReplicator {

  private static final Logger LOG =
      LoggerFactory.getLogger(PushReplicator.class);

  private final ContainerReplicationSource source;
  private final ContainerUploader uploader;

  public PushReplicator(ContainerReplicationSource source,
      ContainerUploader uploader) {
    this.source = source;
    this.uploader = uploader;
  }

  @Override
  public void replicate(ReplicationTask task) {
    long containerID = task.getContainerId();
    DatanodeDetails target = task.getTarget();
    CompletableFuture<Void> fut = new CompletableFuture<>();

    source.prepare(containerID);

    OutputStream output = null;
    try {
      output = uploader.startUpload(containerID, target, fut);
      source.copyData(containerID, output, NO_COMPRESSION.name());
      fut.get();
      task.setStatus(Status.DONE);
    } catch (Exception e) {
      LOG.warn("Container {} replication was unsuccessful.", containerID, e);
      task.setStatus(Status.FAILED);
    } finally {
      // output may have already been closed, ignore such errors
      IOUtils.cleanupWithLogger(LOG, output);
    }
  }
}
