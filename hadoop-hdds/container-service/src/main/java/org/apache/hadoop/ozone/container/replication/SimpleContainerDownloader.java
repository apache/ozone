/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.container.replication;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeDetails.Port.Name;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.ozone.OzoneConfigKeys;

import com.google.common.annotations.VisibleForTesting;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple ContainerDownloaderImplementation to download the missing container
 * from the first available datanode.
 * <p>
 * This is not the most effective implementation as it uses only one source
 * for he container download.
 */
public class SimpleContainerDownloader implements ContainerDownloader {

  private static final Logger LOG =
      LoggerFactory.getLogger(SimpleContainerDownloader.class);

  private final Path workingDirectory;
  private final SecurityConfig securityConfig;
  private final X509Certificate caCert;

  public SimpleContainerDownloader(
      ConfigurationSource conf,
      X509Certificate caCert
  ) {

    String workDirString =
        conf.get(OzoneConfigKeys.OZONE_CONTAINER_COPY_WORKDIR);

    if (workDirString == null) {
      workingDirectory = Paths.get(System.getProperty("java.io.tmpdir"))
          .resolve("container-copy");
    } else {
      workingDirectory = Paths.get(workDirString);
    }
    securityConfig = new SecurityConfig(conf);
    this.caCert = caCert;
  }

  @Override
  public CompletableFuture<Path> getContainerDataFromReplicas(
      long containerId,
      List<DatanodeDetails> sourceDatanodes
  ) {

    CompletableFuture<Path> result = null;

    final List<DatanodeDetails> shuffledDatanodes =
        shuffleDatanodes(sourceDatanodes);

    for (DatanodeDetails datanode : shuffledDatanodes) {
      try {
        if (result == null) {
          result = downloadContainer(containerId, datanode);
        } else {

          result = result.exceptionally(t -> {
            LOG.error("Error on replicating container: " + containerId, t);
            try {
              return downloadContainer(containerId, datanode).join();
            } catch (Exception e) {
              LOG.error("Error on replicating container: " + containerId,
                  e);
              return null;
            }
          });
        }
      } catch (Exception ex) {
        LOG.error(String.format(
            "Container %s download from datanode %s was unsuccessful. "
                + "Trying the next datanode", containerId, datanode), ex);
      }
    }
    return result;

  }

  //There is a chance for the download is successful but import is failed,
  //due to data corruption. We need a random selected datanode to have a
  //chance to succeed next time.
  @NotNull
  protected List<DatanodeDetails> shuffleDatanodes(
      List<DatanodeDetails> sourceDatanodes
  ) {

    final ArrayList<DatanodeDetails> shuffledDatanodes =
        new ArrayList<>(sourceDatanodes);

    Collections.shuffle(shuffledDatanodes);

    return shuffledDatanodes;
  }

  @VisibleForTesting
  protected CompletableFuture<Path> downloadContainer(
      long containerId,
      DatanodeDetails datanode
  ) throws IOException {
    CompletableFuture<Path> result;
    GrpcReplicationClient grpcReplicationClient =
        new GrpcReplicationClient(datanode.getIpAddress(),
            datanode.getPort(Name.REPLICATION).getValue(),
            workingDirectory, securityConfig, caCert);
    result = grpcReplicationClient.download(containerId)
        .thenApply(r -> {
          try {
            grpcReplicationClient.close();
          } catch (Exception e) {
            LOG.error("Couldn't close Grpc replication client", e);
          }
          return r;
        });

    return result;
  }

  @Override
  public void close() {
    // noop
  }
}
