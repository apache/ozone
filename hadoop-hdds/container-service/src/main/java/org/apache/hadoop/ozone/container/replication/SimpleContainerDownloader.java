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

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeDetails.Port.Name;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.utils.IOUtils;
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

  private final SecurityConfig securityConfig;
  private final CertificateClient certClient;

  public SimpleContainerDownloader(
      ConfigurationSource conf, CertificateClient certClient) {
    securityConfig = new SecurityConfig(conf);
    this.certClient = certClient;
  }

  @Override
  public Path getContainerDataFromReplicas(
      long containerId, List<DatanodeDetails> sourceDatanodes,
      Path downloadDir, CopyContainerCompression compression) {

    if (downloadDir == null) {
      downloadDir = Paths.get(System.getProperty("java.io.tmpdir"))
              .resolve(ContainerImporter.CONTAINER_COPY_DIR);
    }

    final List<DatanodeDetails> shuffledDatanodes =
        shuffleDatanodes(sourceDatanodes);

    for (int i = 0; i < shuffledDatanodes.size(); i++) {
      DatanodeDetails datanode = shuffledDatanodes.get(i);
      GrpcReplicationClient client = null;
      try {
        client = createReplicationClient(datanode, compression);
        CompletableFuture<Path> result =
            downloadContainer(client, containerId, downloadDir);
        return result.get();
      } catch (InterruptedException e) {
        logError(e, containerId, datanode, i, shuffledDatanodes.size());
        Thread.currentThread().interrupt();
      } catch (Exception e) {
        logError(e, containerId, datanode, i, shuffledDatanodes.size());
      } finally {
        IOUtils.close(LOG, client);
      }
    }
    LOG.error("Container {} could not be downloaded from any datanode",
        containerId);
    return null;
  }

  private static void logError(Exception e,
      long containerId, DatanodeDetails datanode, int datanodeIndex,
      int shuffledDatanodesSize) {
    StringBuilder sb =
        new StringBuilder("Error on replicating container: {} from {}. ");
    if (datanodeIndex < shuffledDatanodesSize - 1) {
      sb.append("Will try next datanode.");
    }
    LOG.error(sb.toString(), containerId,
        datanode, e);
  }

  //There is a chance for the download is successful but import is failed,
  //due to data corruption. We need a random selected datanode to have a
  //chance to succeed next time.
  @VisibleForTesting
  protected List<DatanodeDetails> shuffleDatanodes(
      List<DatanodeDetails> sourceDatanodes) {

    final ArrayList<DatanodeDetails> shuffledDatanodes =
        new ArrayList<>(sourceDatanodes);

    Collections.shuffle(shuffledDatanodes);

    return shuffledDatanodes;
  }

  @VisibleForTesting
  protected GrpcReplicationClient createReplicationClient(
      DatanodeDetails datanode, CopyContainerCompression compression
  ) throws IOException {
    return new GrpcReplicationClient(datanode.getIpAddress(),
        datanode.getPort(Name.REPLICATION).getValue(),
        securityConfig, certClient, compression);
  }

  @VisibleForTesting
  protected CompletableFuture<Path> downloadContainer(
      GrpcReplicationClient client, long containerId, Path downloadDir) {
    return client.download(containerId, downloadDir);
  }

  @Override
  public void close() {
    // noop
  }
}
